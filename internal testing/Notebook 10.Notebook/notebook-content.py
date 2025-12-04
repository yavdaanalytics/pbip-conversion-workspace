# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1644fce6-4679-4b74-93e6-7d5d9dae80cd",
# META       "default_lakehouse_name": "Demo_Delta",
# META       "default_lakehouse_workspace_id": "4eb34a24-0d28-44c2-b26c-d20ec4c41693",
# META       "known_lakehouses": [
# META         {
# META           "id": "1644fce6-4679-4b74-93e6-7d5d9dae80cd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/Products/products.csv")
# df now is a Spark DataFrame containing CSV data from "Files/Products/products.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").saveAsTable("managed_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").saveAsTable("external_products", path="abfss://4eb34a24-0d28-44c2-b26c-d20ec4c41693@onelake.dfs.fabric.microsoft.com/1644fce6-4679-4b74-93e6-7d5d9dae80cd/Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
