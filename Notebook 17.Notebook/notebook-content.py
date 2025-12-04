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

from pyspark.sql.functions import col, explode

# Example: flatten nested "tasklists" array
df_flat = df.withColumn("tasklist", explode(col("tasklists"))) \
            .select("tasklist.*")   # select inner fields

df_flat.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

def flatten_structs(df):
    flat_cols = []
    nested_cols = []
    for col_name, dtype in df.dtypes:
        if dtype.startswith("struct"):
            nested_cols.append(col_name)
        else:
            flat_cols.append(col_name)

    flat_df = df.select(
        *flat_cols,
        *[col(f"{c}.{nested}").alias(f"{c}_{nested}") 
          for c in nested_cols 
          for nested in df.select(f"{c}.*").columns]
    )
    return flat_df

df_flat = flatten_structs(df_exploded)
display(df_flat)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
