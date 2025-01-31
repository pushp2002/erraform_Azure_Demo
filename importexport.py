# Get the list of databases in the catalog
databases = spark.sql("SHOW DATABASES").collect()

# Loop through each database
for db in databases:
    database_name = db.databaseName
    
    # Get the list of tables in the current database
    tables = spark.sql(f"SHOW TABLES IN {database_name}").collect()
    
    # Loop through each table to export metadata and data
    for table in tables:
        table_name = table.tableName
        
        # Export table metadata
        metadata = spark.sql(f"DESCRIBE FORMATTED {database_name}.{table_name}").toJSON().collect()
        metadata_str = "\n".join(metadata)
        dbutils.fs.put(f"abfss://your-container-name@your-storage-account.dfs.core.windows.net/your-export-location/{database_name}/{table_name}_metadata.json", metadata_str, overwrite=True)
        
        # Export table data
        data_df = spark.sql(f"SELECT * FROM {database_name}.{table_name}")
        data_df.write.mode("overwrite").parquet(f"abfss://your-container-name@your-storage-account.dfs.core.windows.net/your-export-location/{database_name}/{table_name}_data.parquet")
```text
### Importing All Databases and Their Tables

This code will read all exported files from the metadata backup folder and import them back into the Hive metastore.
```python
# Define the base path for the exported files
base_path = "abfss://your-container-name@your-storage-account.dfs.core.windows.net/your-export-location/"

# List all directories (databases) in the export location
databases_exported = dbutils.fs.ls(base_path)

for db in databases_exported:
    database_name = db.name.strip("/")  # Get the database name from the path
    
    # Create the database if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    
    # List all files in the current database's export folder
    tables_exported = dbutils.fs.ls(db.path)
    
    for table_file in tables_exported:
        if table_file.name.endswith("_metadata.json"):
            table_name = table_file.name.replace("_metadata.json", "")
            
            # Import table metadata (you may need to parse the JSON and create the table accordingly)
            metadata_path = table_file.path
            metadata_json = dbutils.fs.head(metadata_path)  # Read the metadata file
            # Here you would parse the metadata_json and create the table structure
            
            # Import table data
            data_path = f"{base_path}{database_name}/{table_name}_data.parquet"
            data_df = spark.read.parquet(data_path)
            data_df.write.mode("overwrite").saveAsTable(f"{database_name}.{table_name}")
