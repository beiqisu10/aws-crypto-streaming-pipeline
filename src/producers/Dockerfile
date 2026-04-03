FROM python:3.10-slim

WORKDIR /app

# Install only the dependencies required to run the Producer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the script needed for production
COPY binance_producer.py ./producer.py

# Set default environment variables (can be overridden at runtime)
ENV KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
ENV KAFKA_TOPIC="binance-trades"

CMD ["python", "producer.py"]