# Databricks notebook source
from pyspark.sql.functions import current_timestamp, from_utc_timestamp
def add_ingestion_column(input_df):
    output_df = input_df.withColumn('ingestion_date', from_utc_timestamp(current_timestamp(), "GMT-3"))
    return output_df

# COMMAND ----------


