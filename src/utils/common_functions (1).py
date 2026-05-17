# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df


# COMMAND ----------

def overwrite_partition(db_name, table_name, column_partition, file_date):
    if spark.catalog.tableExists(f"{db_name}.{table_name}"):
        spark.sql(f"delete from {db_name}.{table_name} where {column_partition} = '{file_date}'")

# COMMAND ----------

def merge_delta_lake(input_df, db_name, table_name, merge_condition, partition_column):

    from delta.tables import deltaTable

    if spark.catalog.tableExists(f"{db_name}.{table_name}"):

        deltaTable = DeltaTable.forName(spark, f"{db_name}.{table_name}")

        deltaTable.alias('tgt') \
        .merge(
            input_df.alias('src'),
            merge_condition
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()   

    else:

    input_df.write.mode("overwrite").partitionBy("partition_column").format("delta").saveAsTable(f"{db_name}.{table_name}")