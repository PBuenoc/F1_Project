# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option('multiline', True) \
.json(f'{raw_folder_path}/pit_stops.json')

# COMMAND ----------

pit_stops_df = pit_stops_df.withColumnRenamed('raceId', 'race_id') \
                           .withColumnRenamed('driverId', 'driver_id')

# COMMAND ----------

pit_stops_df = add_ingestion_column(pit_stops_df)

# COMMAND ----------

pit_stops_df.write.mode("overwrite").parquet(f'{processed_folder_path}/pit_stops')