
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import pandas as pd
import numpy as np
from faker import Faker
import time

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("data_producer_transactions")
        .config("spark.jars", ",".join(["/home/jovyan/work/jars/mysql-connector-java-8.0.33.jar",
                                        "/home/jovyan/work/jars/postgresql-42.7.7.jar"]))  
        .getOrCreate()
)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Initialize faker
fake = Faker()

# Define number of rows
num_rows = 10


database="transactions"

def get_data():
    
    data = {
        'txn_id': np.random.randint(100000000, 999999999, num_rows),
        'sender_account_id': np.random.randint(100000, 999999, num_rows),
        'receiver_account_id': np.random.randint(100000, 999999, num_rows),
        'amount': np.round(np.random.uniform(10.0, 10000.0, num_rows), 2),
        'last_modified_date': [fake.date_time_between(start_date='-1h', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
        for _ in range(num_rows)],    
        'product_id': np.random.randint(100, 999, num_rows),
        'product_type_id': np.random.randint(1, 20, num_rows),
        'transactor_module_id': np.random.randint(1, 10, num_rows),
        'module_id': np.random.randint(1, 5, num_rows),
        'status': np.random.randint(1, 5, num_rows)
    }
    return data
    
batch = 0
while True:
    print(f"batch no : {batch}")
    if batch == 10:
        break
    data = get_data()
    df = pd.DataFrame(data)
    print("data")
    print(df)
    
    df_s= spark.createDataFrame(df)
    df_s = df_s.withColumn(
        "last_modified_date",
        F.to_timestamp(F.col("last_modified_date"), "yyyy-MM-dd HH:mm:ss")
    )
    
    df_s.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://postgres:5432/{database}") \
        .option("dbtable", "transaction_details") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    time.sleep(5)
    batch = batch + 1
    
    