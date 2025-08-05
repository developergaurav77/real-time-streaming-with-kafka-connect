from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType
import requests
import json
import fastavro
import io

is_stream = 1
def show(df,n=5):
    return df.limit(n).toPandas()


spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("realtime_transactions")
        .config("spark.jars.packages", ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            "org.apache.spark:spark-avro_2.12:3.5.0"
        ]))
        .config("spark.jars", ",".join(["/home/jovyan/work/jars/mysql-connector-java-8.0.33.jar",
                                        "/home/jovyan/work/jars/postgresql-42.7.7.jar"]))  
        .getOrCreate()
)


# Configuration
registry = "http://schema-registry:8081"
topic_name = "dev.public.transaction_details"

# Get the reader schema
response = requests.get(f"{registry}/subjects/{topic_name}-value/versions/latest/schema")
reader_schema = json.loads(response.text)

# Convert reader schema to Spark schema
spark_schema = StructType.fromJson(json.loads(spark.sparkContext._jvm.org.apache.spark.sql.avro.SchemaConverters \
    .toSqlType(spark.sparkContext._jvm.org.apache.avro.Schema.Parser().parse(json.dumps(reader_schema))) \
    .dataType().json()))

# Create a broadcast variable for the reader schema
reader_schema_bc = spark.sparkContext.broadcast(reader_schema)

# Read from Kafka
if is_stream ==1:
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()
else:
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()
    kafka_df.show()



#  UDF for deserialization
def deserialize_confluent_avro(data):
    if data is None or len(data) < 5:
        return None
        
    try:
        # Extract schema ID (first byte is magic byte, next 4 bytes are schema ID)
        schema_id = int.from_bytes(data[1:5], byteorder='big')
        
        schema_resp = requests.get(f"{registry}/schemas/ids/{schema_id}")
        writer_schema = json.loads(schema_resp.json()["schema"])
        
        # Parse schemas
        writer_schema_parsed = fastavro.parse_schema(writer_schema)
        reader_schema_parsed = fastavro.parse_schema(reader_schema_bc.value)
        
        # Deserialize Avro payload
        with io.BytesIO(data[5:]) as bio:
            return fastavro.schemaless_reader(bio, writer_schema_parsed, reader_schema_parsed)
    except Exception as e:
        print(f"Deserialization error: {str(e)}")
        return None

deserialize_udf = udf(deserialize_confluent_avro, spark_schema)

# Apply deserialization
df = kafka_df.withColumn("data", deserialize_udf(col("value"))).select("data.*").dropna(how='all')

df = df.filter("__deleted == 'false'")

if is_stream ==0:
    df.show(n=100)
    
# deleted_rows = df.filter("__deleted == 'true'").select("txn_id")
# df = df.join(F.broadcast(deleted_rows),on="txn_id",how="leftanti")

agg_df = (
    df.withWatermark("last_modified_date", "5 minutes")
       .groupBy(
          F.window(col("last_modified_date"), "15 minutes")
      )
      .agg(
          F.sum("amount").alias("total_amount"),
          F.count("amount").alias("txn_count")
      )
)


import mysql.connector

database = "rp"
staging_table = "rp_stage_agg_15min_transactions"
final_table = "rp_agg_15min_transactions"

def upsert_batch(df, batch_id):
    df = (
        df
          .withColumn("window_start", col("window.start"))
          .withColumn("window_end", col("window.end"))
          .drop("window")
    )
    jdbc_url = f"jdbc:mysql://mysql:3306/{database}"
    props = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver",
        "rewriteBatchedStatements": "true"
    }
    df.write.jdbc(url=jdbc_url, table=staging_table, mode="append", properties=props)

    conn = mysql.connector.connect(
        host="mysql", user="root", password="root", database=f"{database}"
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
              INSERT INTO {final_table} (window_start, window_end, txn_count, total_amount)
              SELECT window_start, window_end, txn_count, total_amount FROM {staging_table}
              ON DUPLICATE KEY UPDATE
                window_end = VALUES(window_end),
                txn_count = VALUES(txn_count),
                total_amount = VALUES(total_amount)
            """)
        conn.commit()  # <- commit after finishing the INSERT

        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {staging_table}")
        conn.commit()  # <- commit after finishing the TRUNCATE

    finally:
        conn.close()

(agg_df.writeStream
    .foreachBatch(upsert_batch)
    .trigger(processingTime="1 minute")                     # run every minute
    .option("checkpointLocation", "checkpoint/gaurav/agg_15_min_n")
     .outputMode("update") 
    .start()
    .awaitTermination()
)