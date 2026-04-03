import json
import websocket
import os
from kafka import KafkaProducer
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get configuration from environment variables for easy modification in AWS console without rebuilding image
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'crypto-trades')

def create_topic_if_not_exists():
    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id='setup-admin'
    )
    
    topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=2, replication_factor=2)]
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info(f"🆕 Topic '{TOPIC_NAME}' created successfully!")
    except TopicAlreadyExistsError:
        logger.info(f"✅ Topic '{TOPIC_NAME}' already exists.")
    except Exception as e:
        logger.error(f"❌ Failed to create topic: {e}")
    finally:
        admin_client.close()

# Initialize Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=1,              # Ensure data reaches at least one Broker
        retries=5,           # Auto retry on network jitter
        request_timeout_ms=30000 # Increase timeout to accommodate cloud latency
    )
    logger.info(f"🚀 Producer initialized. Servers: {BOOTSTRAP_SERVERS}")
except Exception as e:
    logger.error(f"❌ Failed to init Kafka Producer: {e}")
    raise

def on_message(ws, message):
    try:
        data = json.loads(message)
        
        # Extract and clean data
        payload = {
            "symbol": data.get('s'),
            "price": float(data.get('p')),
            "quantity": float(data.get('q')),
            "event_time": data.get('E'),
            "trade_id": data.get('a')
        }
        
        # Send and log
        producer.send(TOPIC_NAME, value=payload)
        # For production, consider logging every 100 messages or only key info to save log costs
        logger.info(f"✅ Sent: {payload['symbol']} @ {payload['price']}")
        
    except Exception as e:
        logger.error(f"⚠️ Error processing message: {e}")

def on_error(ws, error):
    logger.error(f"❌ WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.info("🔌 Connection closed. Flushing producer...")
    producer.flush()

def on_open(ws):
    logger.info("🔌 Connection opened to Binance WebSocket")

if __name__ == "__main__":
    create_topic_if_not_exists()
    # Binance WebSocket URL
    socket = "wss://stream.binance.us:9443/ws/btcusdt@aggTrade/ethusdt@aggTrade"
    
    ws = websocket.WebSocketApp(
        socket,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Add reconnect logic since WebSocket may disconnect
    ws.run_forever(ping_interval=30, ping_timeout=10)