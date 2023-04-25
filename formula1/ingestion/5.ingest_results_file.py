# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json('/mnt/f1projectdl001/raw/results.json')

# COMMAND ----------

results_df = results_df.drop('statusId')

# COMMAND ----------

results_df = results_df.withColumnRenamed('resultId', 'result_id') \
                       .withColumnRenamed('raceId', 'race_id') \
                       .withColumnRenamed('driverId', 'driver_id') \
                       .withColumnRenamed('constructorId', 'constructor_id') \
                       .withColumnRenamed('positionText', 'position_text') \
                       .withColumnRenamed('positionOrder', 'position_order') \
                       .withColumnRenamed('fastestLap', 'fastest_lap') \
                       .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
                       .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, from_utc_timestamp

results_df = results_df.withColumn('ingestion_date', from_utc_timestamp(current_timestamp(), "GMT-3"))

# COMMAND ----------

results_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/f1projectdl001/processed/results')

# COMMAND ----------


