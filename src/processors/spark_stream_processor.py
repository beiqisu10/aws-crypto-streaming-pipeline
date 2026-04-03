import sys
import os
import json
import boto3
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, sum, count, max as spark_max, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# 1. Initialize Glue Job and resolve parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'bootstrap_servers', 
    'redshift_host',
    'checkpoint_location'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Constants from Job Parameters
BOOTSTRAP_SERVERS = args['bootstrap_servers']
REDSHIFT_HOST = args['redshift_host']
CHECKPOINT_PATH = args['checkpoint_location']

# --- SECRETS MANAGER INTEGRATION ---

def get_secret():
    """
    Retrieves Redshift credentials from AWS Secrets Manager.
    This replaces hardcoded passwords for production security.
    """
    secret_name = "crypto-redshift-credentials" # Must match your Terraform resource name
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        raise e

    # Decodes the JSON string into a Python dictionary
    return json.loads(get_secret_value_response['SecretString'])

# --- DATABASE CONNECTIVITY ---

def get_redshift_conn():
    """
    Establishes a connection to Redshift Serverless using secrets from Secrets Manager.
    """
    import psycopg2
    creds = get_secret()
    
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=5439,
        database="cryptodb",
        user=creds['username'],
        password=creds['password'] # Secretly fetched password
    )

# --- STREAMING LOGIC ---

# 1. Define the Schema for incoming Binance trade data
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("event_time", LongType(), True),
    StructField("trade_id", LongType(), True)
])

# 2. Configure Spark for Kafka and Shuffle optimization
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "4")

# 3. Source: Read from MSK (Kafka)
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", "crypto-trades") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Transform: Parse JSON and create event timestamp
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", (col("event_time") / 1000).cast("timestamp"))

# 5. Process: 1-minute windowed aggregation with Watermarking
aggregated_df = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("symbol")
    ) \
    .agg(
        avg("price").alias("avg_price"),
        sum("quantity").alias("total_volume"),
        spark_max("quantity").alias("max_single_trade"),
        count("trade_id").alias("trade_count")
    )

# --- INITIALIZATION & UPSERT SINK ---

def initialize_database():
    """
    Creates the Target Schema and Table in Redshift if they do not exist.
    """
    import psycopg2
    try:
        conn = get_redshift_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS raw_crypto;
            CREATE TABLE IF NOT EXISTS raw_crypto.crypto_stats (
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                symbol VARCHAR(20),
                avg_price DECIMAL(18,2),
                total_volume DECIMAL(18,4),
                max_single_trade DECIMAL(18,4),
                trade_count BIGINT,
                processed_at TIMESTAMP DEFAULT GETDATE(),
                PRIMARY KEY (window_start, symbol)
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("Database schema initialized successfully.")
    except Exception as e:
        print(f"Schema initialization failed: {e}")

initialize_database()

def write_to_redshift(batch_df, batch_id):
    """
    Writes Micro-batches to Redshift using an Upsert (Delete + Insert) pattern.
    """
    if batch_df.count() == 0:
        return

    # Map Spark columns to Redshift Table Schema
    final_df = batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "symbol",
        col("avg_price").cast("decimal(18,2)"),
        col("total_volume").cast("decimal(18,4)"),
        col("max_single_trade").cast("decimal(18,4)"),
        col("trade_count").cast("bigint")
    )

    def upsert_partition(partition):
        import psycopg2 
        conn = None
        try:
            conn = get_redshift_conn()
            cur = conn.cursor()
            
            # Using Redshift transaction block for atomic Delete + Insert
            delete_sql = "DELETE FROM raw_crypto.crypto_stats WHERE window_start = %s AND symbol = %s;"
            insert_sql = """
                INSERT INTO raw_crypto.crypto_stats (
                    window_start, window_end, symbol, avg_price, 
                    total_volume, max_single_trade, trade_count, processed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, GETDATE());
            """
            
            for row in partition:
                cur.execute(delete_sql, (row.window_start, row.symbol))
                cur.execute(insert_sql, (
                    row.window_start, row.window_end, row.symbol, 
                    float(row.avg_price), float(row.total_volume),
                    float(row.max_single_trade), int(row.trade_count)
                ))
            
            conn.commit()
            cur.close()
        except Exception as e:
            if conn: conn.rollback()
            print(f"Write error: {e}")
            raise e
        finally:
            if conn: conn.close()

    final_df.foreachPartition(upsert_partition)

# --- SINK: Start Streaming Query ---

query = (aggregated_df.writeStream
    .foreachBatch(write_to_redshift)
    .option("checkpointLocation", CHECKPOINT_PATH) # S3 location for fault-tolerance
    .trigger(processingTime='1 minute') 
    .outputMode("update") 
    .start())

query.awaitTermination()
job.commit()