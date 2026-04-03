import json
import time
from kafka import KafkaProducer

# Ensure this address matches the one Spark reads from
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_test_data(symbol, price, qty, trade_id):
    payload = {
        "symbol": symbol,
        "price": float(price),
        "quantity": float(qty),
        "event_time": int(time.time() * 1000),
        "trade_id": trade_id
    }
    producer.send('crypto-trades', payload)
    print(f"Sent: {payload}")

# Send test data for different currency pairs
send_test_data("BTCUSDT", 65000.5, 0.01, 10001)
send_test_data("ETHUSDT", 3500.2, 0.5, 10002)

producer.flush()
print("Done!")