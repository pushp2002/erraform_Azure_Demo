import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HiveMetastoreBackup").enableHiveSupport().getOrCreate()

databases = [db.databaseName for db in spark.sql("SHOW DATABASES").collect()]
metastore_backup = {}

for db in databases:
    tables = [table.tableName for table in spark.sql(f"SHOW TABLES IN {db}").collect()]
    metastore_backup[db] = {}
    for table in tables:
        table_metadata = spark.sql(f"DESCRIBE FORMATTED {db}.{table}").collect()
        metastore_backup[db][table] = [row.asDict() for row in table_metadata]

dbutils.fs.put("/backup/metastore_backup.json", json.dumps(metastore_backup))
======================================================

Verify Backup:
Verify that the backup file has been created and is accessible:
Python

display(dbutils.fs.ls("/backup"))
================

Import Metastore Data:
Use the following code to import the metadata from the JSON file and recreate the tables:
Python

import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HiveMetastoreRestore").enableHiveSupport().getOrCreate()

metastore_backup = json.loads(dbutils.fs.head("/backup/metastore_backup.json"))

for db, tables in metastore_backup.items():
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    for table, metadata in tables.items():
        create_table_stmt = f"CREATE TABLE {db}.{table} ("
        for row in metadata:
            if row['col_name'] and row['data_type']:
                create_table_stmt += f"{row['col_name']} {row['data_type']}, "
        create_table_stmt = create_table_stmt.rstrip(", ") + ")"
        spark.sql(create_table_stmt)


        ============================================

        Verify Restore:
Verify that the metastore has been restored successfully by checking the metadata information:
Python

spark.sql("SHOW TABLES").show()
===================================
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HiveMetastoreRestore").enableHiveSupport().getOrCreate()

metastore_backup = json.loads(dbutils.fs.head("/backup/metastore_backup.json"))

for db, tables in metastore_backup.items():
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    for table, metadata in tables.items():
        create_table_stmt = f"CREATE TABLE {db}.{table} ("
        columns = []
        for row in metadata:
            if row['col_name'] and row['data_type']:
                columns.append(f"{row['col_name']} {row['data_type']}")
        create_table_stmt += ", ".join(columns) + ")"
        try:
            spark.sql(create_table_stmt)
        except Exception as e:
            print(f"Error creating table {db}.{table}: {e}")
            ===================================

            indivdual



            import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HiveMetastoreBackup").enableHiveSupport().getOrCreate()

databases = [db.databaseName for db in spark.sql("SHOW DATABASES").collect()]

for db in databases:
    tables = [table.tableName for table in spark.sql(f"SHOW TABLES IN {db}").collect()]
    for table in tables:
        table_metadata = spark.sql(f"DESCRIBE FORMATTED {db}.{table}").collect()
        table_metadata_dict = [row.asDict() for row in table_metadata]
        dbutils.fs.put(f"/backup/{db}_{table}_metadata.json", json.dumps(table_metadata_dict))


==========================


import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HiveMetastoreRestore").enableHiveSupport().getOrCreate()

backup_files = dbutils.fs.ls("/backup")

for file_info in backup_files:
    file_path = file_info.path
    if file_path.endswith("_metadata.json"):
        db_table = file_path.split("/")[-1].replace("_metadata.json", "")
        db, table = db_table.split("_", 1)
        table_metadata = json.loads(dbutils.fs.head(file_path))
        
        create_table_stmt = f"CREATE TABLE {db}.{table} ("
        columns = []
        for row in table_metadata:
            if row['col_name'] and row['data_type']:
                columns.append(f"{row['col_name']} {row['data_type']}")
        create_table_stmt += ", ".join(columns) + ")"
        
        try:
            spark.sql(create_table_stmt)
        except Exception as e:
            print(f"Error creating table {db}.{table}: {e}")


        

