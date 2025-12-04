# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b",
# META       "default_lakehouse_name": "lh_zoho_projects",
# META       "default_lakehouse_workspace_id": "4eb34a24-0d28-44c2-b26c-d20ec4c41693",
# META       "known_lakehouses": [
# META         {
# META           "id": "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


df = spark.read.json("Files/_zoho_projects_/All_Projects/")


display(df)


df.write.mode("overwrite").saveAsTable("All_Projects_Table")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
