# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.json(f'{raw_folder_path}/constructors.json', schema=constructors_schema)

# COMMAND ----------

constructors_df = constructors_df.drop(constructors_df.url)

# COMMAND ----------

constructors_df = constructors_df.withColumnRenamed('constructorId', 'constructor_id') \
                                .withColumnRenamed('constructorRef', 'constructor_ref')
constructors_df = add_ingestion_column(constructors_df)

# COMMAND ----------

constructors_df.write.mode('overwrite').parquet(f'{processed_folder_path}/constructors')
