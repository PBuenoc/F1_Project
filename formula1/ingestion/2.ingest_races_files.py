# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest files

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.option('inferSchema', True) \
.csv(f"{raw_folder_path}/races.csv")


# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add ingestion data and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, concat, lit

# COMMAND ----------

races_with_timestamp_df = add_ingestion_column(races_with_timestamp_df)
races_with_timestamp_df = races_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Select only the required columns

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'),
                                                  col('round'), col('circuitId').alias('circuit_id'), col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the output to processed container in parquet format

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').format('delta').load(f'{processed_folder_path}/races')

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/races'))