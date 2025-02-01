# Databricks notebook source
!pip install --upgrade pip
!pip install kafka-python
!pip install pyYaml
!pip install orjson
!pip install aiokafka 
!pip install --upgrade websockets
dbutils.library.restartPython()

"""
# !pip install cryptofeed[all] # add note about the cryptofeed install from image
git clone cryptofeed into workspace on dbx (https://github.com/bmoscon/cryptofeed/tree/master -> to clone repo )
replace the coinbase.py file with the new file in directory here as 'coinbase.py'
then need to rebuild the repo and install the .whl file to use the local repo. at the dbx cluster, build from wheel.
go to databricks > conmpute > libraries > install from wheel and select the created .whl file for cryptofeed 

!pip install setuptools wheel
!python setup.py bdist_wheel

relevant links;
https://github.com/bmoscon/cryptofeed/tree/master -> to clone repo 
https://github.com/kzk2000/deephaven-clickhouse -> contained example code for kafka & coinbase.py update
https://cloud.redpanda.com/clusters/ -> setup redpanda cluster for kafka usage
"""


# COMMAND ----------

#### OPTIONAL
# # Store secrets securely using databricks-cli (RECOMMENDED)
# %pip install databricks-cli

# databricks configure --token
# atabricks secrets create-scope --scope kafka

# dbutils.secrets.put(scope="kafka", key="username", string_value="your-username")
# dbutils.secrets.put(scope="kafka", key="password", string_value="your-password")
# dbutils.secrets.put(scope="kafka", key="bootstrap_servers", string_value="your-bootstrap-servers")

# Placeholder values (BETTER TO USE THE DBX CLI SECRET KEY VALUES)
USERNAME = "<ENTER_YOUR_USERNAME>"
PASSWORD = "<ENTER_YOUR_PASSWORD>"
KAFKA_SERVER = "<ENTER_YOUR_BOOTSTRAP_SERVERS>"


# COMMAND ----------

import os
import gzip
import json
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StringType, FloatType, StructType, StructField, LongType
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
CATALOG_NAME = "trader_etl"
SCHEMA_NAME = "market_etl"
TOPIC = "trade"
DATABASE = f"{CATALOG_NAME}.{SCHEMA_NAME}"
INGESTION_TABLE = f"{DATABASE}.{TOPIC}"
TOPIC_CHECKPOINT_PATH = "/tmp/checkpoints/trade_topic"

# Spark session setup
spark = SparkSession.builder.appName("KafkaStreamConsumer").getOrCreate()
"""
## Potentially useful snippets
# %fs rm -r dbfs:/tmp/checkpoints/trade_topic
# %sql
# drop table if exists trader_etl.market_etl.trade;
"""

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.{TOPIC} (
        exchange STRING,
        symbol STRING,
        side STRING,
        price FLOAT,
        ts LONG,
        receipt_ts LONG,
        size DOUBLE,
        trade_id STRING,
        epoch LONG
    )
    USING DELTA
    CLUSTER BY (symbol, ts)
    OPTIONS (
        delta.autoOptimize.autoCompact = 'auto',
        delta.autoOptimize.optimizeWrite = true
    )
    """
)


# Schema definition
schema = StructType([
    StructField("exchange", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("side", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("ts", LongType(), True),
    StructField("receipt_ts", LongType(), True),
    StructField("size", FloatType(), True),
    StructField("trade_id", StringType(), True),
    StructField("epoch", LongType(), True)
])

# Utility function for batch processing
def process_batch(df, epoch_id):
    """Processes each batch of streaming data."""
    logger.info(f"Processing batch with epoch ID: {epoch_id}")

    # Transform and cast fields to match the Delta table schema
    processed_df = df.select(
        F.col("exchange").alias("exchange"),
        F.col("symbol").alias("symbol"),
        F.col("side").alias("side"),
        F.col("price").cast("float").alias("price"),
        F.col("ts").cast("long").alias("ts"),
        F.col("receipt_ts").alias("receipt_ts"),
        F.col("size").cast("double").alias("size"),
        F.col("trade_id").alias("trade_id"),
        F.col("ts").cast("long").alias("epoch")
    )

    # Write to Delta table
    processed_df.write \
        .mode("append") \
        .format("delta") \
        .option("mergeSchema", "false") \
        .saveAsTable(INGESTION_TABLE)

# Read from Kafka as a structured stream
read_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{USERNAME}" password="{PASSWORD}";')
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)  # Reduce Kafka polling size
    .load()
)

# Decode Kafka message values and apply schema
decoded_stream = (
    read_stream.selectExpr("CAST(value AS STRING) as message")
    .select(F.from_json(F.col("message"), schema).alias("data"))
    .select("data.*")
)

# Write stream using foreachBatch & micro batching
write_stream = (
    decoded_stream.writeStream.outputMode("append")
    .trigger(processingTime="1 minute")
    .option("checkpointLocation", TOPIC_CHECKPOINT_PATH)
    .foreachBatch(process_batch)
    .start()
)

write_stream.awaitTermination()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  trader_etl.market_etl.trade order by ts desc ;

# COMMAND ----------

# %fs rm -r dbfs:/tmp/checkpoints/trade_topic

# COMMAND ----------

# %sql
# drop table if exists trader_etl.market_etl.trade;
