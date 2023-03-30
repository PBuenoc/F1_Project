# Databricks notebook source
# MAGIC %run "/Users/simbastico123_gmail.com#ext#@simbastico123gmail.onmicrosoft.com/formula1/includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read and Rename

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races') \
.withColumnRenamed('race_timestamp', 'race_date') \
.withColumnRenamed('name', 'race_name')

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits') \
.withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

constructors_df = spark.read.parquet(f'{processed_folder_path}/constructors') \
.withColumnRenamed('name', 'team')

# COMMAND ----------

results_df = spark.read.parquet(f'{processed_folder_path}/results') \
.withColumnRenamed('time', 'race_time')

# COMMAND ----------

drivers_df = spark.read.parquet(f'{processed_folder_path}/drivers') \
.withColumnRenamed('number', 'driver_number') \
.withColumnRenamed('name', 'driver_name') \
.withColumnRenamed('nationality', 'driver_nationality')

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Join circuits to races

# COMMAND ----------

race_circuit_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, 'inner').select('race_id','race_year', 'race_name', 'race_date', 'circuit_location') 

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Join result to all other df's

# COMMAND ----------

race_results_df = results_df.join(race_circuit_df, results_df.race_id == race_circuit_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = race_results_df.select('race_year', 'race_name', 'race_date', 'circuit_location', 'driver_name', 'driver_number', 'driver_nationality', 'team', 'grid', 'fastest_lap', 'race_time', 'points', 'position') \
.withColumn('created_date', current_timestamp())

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

display(final_df.where("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

