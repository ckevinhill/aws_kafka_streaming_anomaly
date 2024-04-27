import json, os, re

from delta.tables import *

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

spark = SparkSession \
    .builder \
    .appName("Kafka_Stream_Aggregator") \
    .getOrCreate()

# Load Stream from Kafka:
df_kafka_stream = ( spark
                   .readStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", "b-2.csciemsk.knyb5i.c1.kafka.us-east-1.amazonaws.com:9092")
                   .option("subscribe", "usage")
                   .option("startingOffsets", "earliest")
                   .load() 
                )

# Define JSON Schema:
schema = StructType([ 
    StructField("ts", TimestampType(), True),
    StructField("value" , FloatType(), True),
    ])

# Convert Kafka JSON to structured columns:
df_json_stream = ( df_kafka_stream
                  .select(
                      from_json(
                          col("Value").cast("String"),
                          schema).alias("json_value")
                      )
                  ).select(col("json_value.*"))


# s = df_json_stream.writeStream.format("console").start()

# Aggregate streaming values:
df_window_stream = ( df_json_stream
                    .withWatermark("ts", "1 hour")
                    .groupBy(
                        window("ts", "1 hour")
                    ).agg(avg("value").alias("avg_usage"))
                    ).select("window.start", "window.end", "avg_usage")

# Output to S3 in Delta format:

delta_location = "s3a://cscie192-final/deltalake/"
checkpoint_location = "s3a://cscie192-final/checkpoint/"

(spark
 .createDataFrame([], df_window_stream.schema)
 .write
 .option("mergeSchema", "true")
 .format("delta")
 .mode("append")
 .save(delta_location))

w = (df_window_stream
 .writeStream
 .format("delta")
 .option("checkpointLocation", checkpoint_location) 
 #.foreachBatch(upsertToDelta) 
 #.outputMode("update") 
 .start(delta_location))
