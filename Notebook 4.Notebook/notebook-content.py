# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a4ddeed3-8043-49b3-a0e5-a3778764ad83",
# META       "default_lakehouse_name": "LakehosueTraining",
# META       "default_lakehouse_workspace_id": "4eb34a24-0d28-44c2-b26c-d20ec4c41693",
# META       "known_lakehouses": [
# META         {
# META           "id": "a4ddeed3-8043-49b3-a0e5-a3778764ad83"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

df = spark.sql("SELECT * FROM LakehosueTraining.sales LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
