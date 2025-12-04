# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b7af12f2-393d-4f7e-ba4f-6a272ed06f6e",
# META       "default_lakehouse_name": "Experimentation_for_MA",
# META       "default_lakehouse_workspace_id": "4eb34a24-0d28-44c2-b26c-d20ec4c41693"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.put(FILE_NAME, XML_CONTENT, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
