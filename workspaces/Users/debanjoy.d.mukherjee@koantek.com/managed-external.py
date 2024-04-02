# Databricks notebook source
from pyspark.sql.functions import col, count

# Function to process database and table information
def process_db_table(db):
    table_list = [t.name for t in spark.catalog.listTables(dbName=db)]
    
    print(f"Database: {db}")
    
    # Initialize counters
    managed_count = 0
    external_count = 0
    
    for table in table_list:
        try:
            table_details = spark.sql(f"DESCRIBE EXTENDED {db}.{table}")
            table_type = table_details.filter(col("col_name") == "Type").select("data_type").collect()[0][0]
            
            # Count and print based on table type
            if table_type == 'MANAGED':
                managed_count += 1
                print(f"\tTable: {table}, Type: MANAGED")
            elif table_type == 'EXTERNAL':
                external_count += 1
                print(f"\tTable: {table}, Type: EXTERNAL")
        except Exception as e:
            print(f"\tError processing {table}: {str(e)}")
            continue
    
    print(f"\tManaged Table Count: {managed_count}")
    print(f"\tExternal Table Count: {external_count}")
    print()

# List databases
db_list = [d.name for d in spark.catalog.listDatabases()]
print('Total Databases:', len(db_list))

# Process databases and tables
for db in db_list:
    process_db_table(db)


