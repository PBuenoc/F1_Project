## Project Overwiew
Utilize the Ergast Developer API, which provides a historical record of motor racing data for non-commercial purposes. Using this API of the Formula 1,  the project will perform all the steps of a real project, from the ingestion to creation of dashboards. In the end of the project, I will perform:
<p>•  Create, configure and use Databricks Clusters and Notebooks<p>
<p>•	Create, configure, access and use Azure Data Lake Storage Gen2<p>
<p>•	Use Azure Key-Vault to store secrets<p>
<p>•	Use Azure Active Directory to create a Service Principal<p>
<p>•	Realizing data ingestion with different files (CSV, JSON, Multiple Files)<p>
<p>•	Work with Databricks Workflows<p>
<p>•	Applying Filter and Join transformations and Aggregations<p>
<p>•	Use PySpark and Spark SQL<p>
<p>•	Create Databases, Tables and Views<p>
<p>•	Utilize Delta Lake<p>
<p>•	Create reports<p>

## The Data
<p>•	Circuits – CSV<p>
<p>•	Races – CSVp>
<p>•	Constructors – Single Line Nested JSON<p>
<p>•	Drivers – Single Line JSON<p>
<p>•	Pitstops – Multi Line JSON<p>
<p>•	Lap Times – Split CSV Files<p>
<p>•	Qualifying – Split Multi Line JSON files<p>
![image](https://user-images.githubusercontent.com/99371991/228934200-a1ceca1e-338b-4f61-8d46-4f2a3a553277.png)

## Project Requirement:
### •	Data Ingestion Requirements
<p>o	Ingest All 8 files into the data lake<p>
<p>o	Ingested data must have the schema applied<p>
<p>o	Ingested data must have audit columns<p>
<p>o	Ingested data must be stored in columnar format (Parquet)<p>
<p>o	Must be able to analyze the ingested data via SQL<p>
<p>o	Ingestion logic must be able to handle incremental load<p>

### •	Data Transformation Requirements
<p>o	Join the key information required for reporting to create a new table<p>
<p>o	Joint the key information required for Analysis to create a new table<p>
<p>o	Transformed tables must have audit columns<p>
<p>o	Must be able to analyze the transformed data via SQL<p>
<p>o	Transformed data must be stored in columnar format (Parquet)<p>
<p>o	Transformations logic must be able to handle incremental load<p>

### •	Reporting Requirements
<p>o	Driver Standings<p>
<p>o	Constructor Standings<p>

### •	Analysis Requirements
<p>o	Dominant Drivers<p>
<p>o	Dominant Teams<p>
<p>o	Visualize the outputs<p>
<p>o	Create Databricks Dashboards<p>

### •	Scheduling Requirements
<p>o	Scheduled to run every Sunday 10PM<p>
<p>o	Ability to monitor pipelines<p>
<p>o	Ability to re-run failed pipelines<p>
<p>o	Ability to set-up alerts on failures<p>

#### •	Other Non-Functional Requirements
<p>o	Ability to delete individual records<p>
<p>o	Ability to see history and time travel<p>
<po	Ability to roll back to a previous version><p>
