# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
lap_times_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                      StructField('driverId', IntegerType(), False),
                                      StructField('lap', IntegerType(), False),
                                      StructField('position', IntegerType(), True),
                                      StructField('time', StringType(), True),
                                      StructField('milliseconds', IntegerType(), True),
])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f'{raw_folder_path}/lap_times')

# COMMAND ----------

lap_times_df = lap_times_df.withColumnRenamed('raceId', 'race_id') \
                           .withColumnRenamed('driverId', 'driver_id')
lap_times_df = add_ingestion_column(lap_times_df)

# COMMAND ----------

lap_times_df.write.mode('overwrite').parquet(f'{processed_folder_path}/lap_times')