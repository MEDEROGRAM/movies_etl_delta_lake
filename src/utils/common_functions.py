# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df


# COMMAND ----------

def overwrite_partition(db_name, table_name, column_partition, file_date):
    if spark.catalog.tableExists(f"{db_name}.{table_name}"):
        spark.sql(f"delete from {db_name}.{table_name} where {column_partition} = '{file_date}'")