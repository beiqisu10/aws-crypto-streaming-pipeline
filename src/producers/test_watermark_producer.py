import json
import time
from kafka import KafkaProducer

# Initialize Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_custom_data(symbol, price, qty, trade_id, custom_time_ms):
    payload = {
        "symbol": symbol,
        "price": float(price),
        "quantity": float(qty),
        "event_time": custom_time_ms,
        "trade_id": trade_id
    }
    producer.send('crypto-trades', payload)
    print(f"Sent {symbol}: Time={time.strftime('%H:%M:%S', time.gmtime(custom_time_ms/1000))}, Price={price}")

# Get a baseline timestamp (assuming today 12:00:00)
# 1711713600000 is an example millisecond timestamp. Using current time minus 1 hour as start is safer
base_time = int((time.time() - 3600) * 1000) 

print("--- Phase 1 & 2: Sending baseline data ---")
send_custom_data("BTCUSDT", 60000.0, 0.1, 20001, base_time)
send_custom_data("BTCUSDT", 61000.0, 0.1, 20002, base_time + (15 * 60 * 1000))
producer.flush()

# Critical: Must wait more than 1 minute to ensure Batch 0 is processed and watermark is updated
print("Waiting 70 seconds to ensure Spark completes first batch and updates Watermark...")
time.sleep(70) 

print("\n--- Phase 3: Sending expired data (12:02) ---")
send_custom_data("BTCUSDT", 99999.0, 0.1, 20003, base_time + (2 * 60 * 1000))
producer.flush()

print("\nTest commands sent.")

'''
For the watermark test, update spark_stream_processor.py as below:

aggregated_df = parsed_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "1 minute"), # 1 minute aggregation window
        col("symbol")
    ) \
    .agg(
        avg("price").alias("avg_price"),
        sum("quantity").alias("total_volume")
    )

query = (aggregated_df.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "./checkpoints/crypto_stats")
    .outputMode("update") 
    .trigger(processingTime='5 seconds') # <--- Modify trigger time here! 5 seconds recommended for testing
    .start())
'''