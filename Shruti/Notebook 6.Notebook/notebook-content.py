# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a25b9300-8b54-4bef-b3c1-bc3cfb69a3de",
# META       "default_lakehouse_name": "Pipeline_Practise",
# META       "default_lakehouse_workspace_id": "4eb34a24-0d28-44c2-b26c-d20ec4c41693",
# META       "known_lakehouses": [
# META         {
# META           "id": "a25b9300-8b54-4bef-b3c1-bc3cfb69a3de"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/products/products.csv")
# df now is a Spark DataFrame containing CSV data from "Files/products/products.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").saveAsTable("external_products", path="abfss://4eb34a24-0d28-44c2-b26c-d20ec4c41693@onelake.dfs.fabric.microsoft.com/a25b9300-8b54-4bef-b3c1-bc3cfb69a3de/Files/external_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE FORMATTED managed_products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE FORMATTED external_products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DROP TABLE managed_products;
# MAGIC DROP TABLE external_products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE products
# MAGIC USING DELTA
# MAGIC LOCATION 'Files/external_products';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC UPDATE products
# MAGIC SET ListPrice = ListPrice * 0.9
# MAGIC WHERE Category = 'Mountain Bikes';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE HISTORY products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_table_path = 'Files/external_products'

# Get the current data
current_data = spark.read.format("delta").load(delta_table_path)
display(current_data)

# Get the version 0 data
original_data = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
display(original_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
