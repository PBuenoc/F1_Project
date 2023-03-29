##Project Overview:
Utilize the Ergast Developer API, which provides a historical record of motor racing data for non-commercial purposes. Using this API of the Formula 1, the project will perform all the steps of a real project, from the ingestion to creation of dashboards. In the end of the project, I will perform:

<p>  •	 Create, configure and use Databricks Clusters and Notebooks<p>
<p>  •	Create, configure, access and use Azure Data Lake Storage Gen2 <p>
<p>  •	Use Azure Key-Vault to store secrets <p>
<p>  •	Use Azure Active Directory to create a Service Principal<p>
<p>  •	Realizing data ingestion with different files (CSV, JSON, Multiple Files)<p>
<p>  •	Work with Databricks Workflows<p>
<p>  •	Applying Filter and Join transformations and Aggregations<p>
<p>  •	Use PySpark and Spark SQL<p>
<p>  •	Create Databases, Tables and Views<p>
<p>  •	Utilize Delta Lake<p>
<p>  •	Create reports<p>

##The Data:
  •	Circuits – CSV
  •	Races – CSV
  •	Constructors – Single Line Nested JSON
  •	Drivers – Single Line JSON
  •	Pitstops – Multi Line JSON
  •	Lap Times – Split CSV Files
  •	Qualifying – Split Multi Line JSON files
 
##Project Requirement:
#### • Data Ingestion Requirements
      o	Ingest All 8 files into the data lake
      o	Ingested data must have the schema applied
      o	Ingested data must have audit columns
      o	Ingested data must be stored in columnar format (Parquet)
      o	Must be able to analyze the ingested data via SQL
      o	Ingestion logic must be able to handle incremental load
      •	Data Transformation Requirements
      o	Join the key information required for reporting to create a new table
      o	Joint the key information required for Analysis to create a new table
      o	Transformed tables must have audit columns
      o	Must be able to analyze the transformed data via SQL
      o	Transformed data must be stored in columnar format (Parquet)
      o	Transformations logic must be able to handle incremental load
#### • Reporting Requirements
    o	Driver Standings
    o	Constructor Standings
#### • Analysis Requirements
    o	Dominant Drivers
    o	Dominant Teams
    o	Visualize the outputs
    o	Create Databricks Dashboards
#### • Scheduling Requirements
    o	Scheduled to run every Sunday 10PM
    o	Ability to monitor pipelines
    o	Ability to re-run failed pipelines
    o	Ability to set-up alerts on failures
####  •	Other Non-Functional Requirements
    o	Ability to delete individual records
    o	Ability to see history and time travel
    o	Ability to roll back to a previous version

##Prepare the environment:
  1. Create the Azure Databricks Service
    a.	Create the service in the premium tier and the workspace
  2. Create the Databricks Cluster
    a.	Configs:
      i.	All-purpose compute
      ii.	Policy – unrestricted
      iii.	Single node cluster
      iv.	Access mode – No isolation shared
      v.	Databricks runtime version - 12.2 LTS
      vi.	Node Type – Standard_DS3_V2 14 GB and 4 cores
      vii.	Terminate after 15 mins of inactivity
  3.	Create Azure Data Lake Storage Gen2
    a.	Set the same resource group of the rest project 
    b.	Configs:
      i.	Performance – Standard
      ii.	Replication – LRS
      iii.	Account Kind – Storage V2 (general purpose v2)
      iv.	Enable hierarchical namespace
    c.	Create Containers – All private
      i.	Raw 
      ii.	Processed
      iii.	Presentation 
  4.	Create a Service Principal
    a.	In AAD register a new app in the ‘App Registrations’
    b.	Service app name = databricks-service-app
    c.	Create a new client secret 
    d.	Store the value
  5.	Create Azure Key-vault and add secrets
    a.	Configs:
      i.	All default
    b.	Generate secrets, it’s just put the respective secret:
      i.	Databricks-app-client-id
      ii.	Databricks-app-tenant-id
      iii.	Databricks-app-secret
  6.	Provide required access to the service principal
  7.	Generate Secret Scope
    a.	Enter in the UI on the principal panel of databricks using:
      i.	In the final of the URL - #secrets/createScope
    b.	Pass the DNS name of the key-vault
      i.	In the properties of the key-vault service
    c.	And pass the Resource ID 
   8.	Mount Azure Data Lake using Service Principal
    a.	Get client_id, tenant_id and client¬_secret 
      i.	Use the dbutils.secrets.get(scope=’’, key=’’)
    b.	Set the spark config and mount
      i.	Create a function that makes:
        1.	Get the parameters:
          a.	Storage_account_name
          b.	Container_name
        2.	Get secrets from key-vault using Secret Scope
        3.	Get Spark configurations
          a.	Documentation link - https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts
        4.	Unmount the mount point if already exists
        5.	Mount the Storage Account Container


##Data Ingestion
 I started by ingesting the CSV files
  1.	Create a new folder for the ingestion notebooks
  2.	Create a new notebook for the ingestion of the Circuits file
    a.	Name – 1.ingest_circuits_file
    b.	Steps:
      i.	Define the Schema
        1.	Import the types that we going to use
        2.	Documentation
        3.	The types:
          a.	StructType - StructType
          b.	StructField - StructField
          c.	IntegerType
          d.	StringType
          e.	DoubleType
        4.	Define a variable to create the schema
      ii.	Read the CSV file using the spark DataFrame reader
        1.	Documentation
        2.	Read with the Header option as true
        3.	Use the schema and specify with the schema created 
        4.	Prints what we have inside of the data frame
      iii.	Select only the require columns
        1.	Documentation
      iv.	Rename the columns as required
        1.	Documentation
      v.	Add a new audit column 
        1.	Documentation
        2.	Use the current_timestamp ()
      vi.	Write the new data to the file system, as a Parquet file
        1.	Documentation
