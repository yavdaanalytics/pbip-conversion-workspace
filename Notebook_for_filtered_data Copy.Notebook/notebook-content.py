# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ============================================================
# Microsoft Fabric - nb_write_filtered_tasklists
# Purpose : Write filtered Tasklists JSON to Lakehouse lh_29
# Input   : filtered_tasklists_json  (passed from pipeline)
# Output  : Parquet file in /lakehouse/default/Files/staging/tasklists_filtered/run_<timestamp>
# ============================================================

import json
import datetime
from pyspark.sql import SparkSession, functions as F
from notebookutils import mssparkutils

# -----------------------------------------------------
# 1️⃣ Read parameter (Fabric auto-injects as variable)
# -----------------------------------------------------
try:
    json_str = filtered_tasklists_json  # Fabric injects this automatically from pipeline
except NameError:
    # When running manually, define test value here
    json_str = """[
        {"id": "TST001", "name": "Demo Tasklist", "last_updated_time": "2025-10-29T08:00:00Z"}
    ]"""

if not json_str or json_str.strip() in ["", "[]", "null"]:
    print("⚠️ No filtered_tasklists_json provided or empty. Nothing to write.")
    mssparkutils.notebook.exit({"rowsWritten": 0, "outputPath": None})

# -----------------------------------------------------
# 2️⃣ Parse JSON
# -----------------------------------------------------
try:
    tasklists_data = json.loads(json_str)
except Exception as e:
    raise Exception(f"❌ Failed to parse JSON input: {e}")

if not tasklists_data:
    print("ℹ️ Filtered tasklist array is empty. Exiting.")
    mssparkutils.notebook.exit({"rowsWritten": 0, "outputPath": None})

# -----------------------------------------------------
# 3️⃣ Create Spark DataFrame
# -----------------------------------------------------
spark = SparkSession.builder.getOrCreate()

# Convert list of dicts to RDD → DataFrame
json_rdd = spark.sparkContext.parallelize([json.dumps(row) for row in tasklists_data])
df = spark.read.json(json_rdd)

# Convert timestamp column if exists
if "last_updated_time" in df.columns:
    df = df.withColumn("last_updated_time", F.to_timestamp("last_updated_time"))

row_count = df.count()
print(f"✅ Created Spark DataFrame with {row_count} rows and {len(df.columns)} columns")

# -----------------------------------------------------
# 4️⃣ Write to Lakehouse (Fabric-correct path)
# -----------------------------------------------------
# ✅ Ensure your notebook is attached to Lakehouse lh_29
# Fabric automatically mounts your attached Lakehouse as /lakehouse/default/
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = f"/lakehouse/default/Files/staging/tasklists_filtered/run_{timestamp}"

print(f"📂 Writing to: {output_path}")

# Write to Parquet
df.write.mode("overwrite").parquet(output_path)

row_count = df.count()
print(f"✅ Successfully wrote {row_count} rows to {output_path}")

# -----------------------------------------------------
# 5️⃣ Return info to pipeline
# -----------------------------------------------------
mssparkutils.notebook.exit({
    "rowsWritten": row_count,
    "outputPath": output_path
})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
