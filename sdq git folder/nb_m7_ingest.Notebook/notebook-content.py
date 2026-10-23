# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8dc76fcf-6ff3-413f-90a1-c1d732ffa899",
# META       "default_lakehouse_name": "SDQ_sample_ls",
# META       "default_lakehouse_workspace_id": "66e95359-350d-4abd-9b10-110ec6cfd52b",
# META       "known_lakehouses": [
# META         {
# META           "id": "8dc76fcf-6ff3-413f-90a1-c1d732ffa899"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Create materialized lake views 
# 1. Use this notebook to create materialized lake views. 
# 2. Select **Run all** to run the notebook. 
# 3. When the notebook run is completed, return to your lakehouse and refresh your materialized lake views graph. 


# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS m7")
spark.sql("""
CREATE TABLE IF NOT EXISTS m7.cdc_events (
  event_id STRING, customer_id STRING, op STRING,
  name STRING, city STRING, event_ts STRING, _batch_id STRING
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

EV = "event_id STRING, customer_id STRING, op STRING, name STRING, city STRING, event_ts STRING, _batch_id STRING"
def append_events(rows):
    (spark.createDataFrame(rows, EV).write.mode("append")
          .format("delta").saveAsTable("m7.cdc_events"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

append_events([
 ("E0", None, "I", "Ghost",  "Nowhere",   "2026-10-01 10:00:00", "B1"),  # null key
 ("E1", "C1", "I", "Asha",   "Chennai",   "2026-10-01 09:00:00", "B1"),
 ("E3", "C1", "U", "Asha",   "Bengaluru", "2026-10-05 10:00:00", "B1"),
 ("E2", "C2", "I", "Ben",    "Delhi",     "2026-10-01 09:05:00", "B1"),
 ("E4", "C2", "D", None,     None,        "2026-10-06 08:00:00", "B1"),  # delete
 ("E5", "C3", "I", "Chitra", "Mumbai",    "2026-10-02 11:00:00", "B1"),
 ("E5", "C3", "I", "Chitra", "Mumbai",    "2026-10-02 11:00:00", "B1"),  # duplicate delivery
 ("E6", "C3", "U", "Chitra", "Pune",      "2026-10-04 12:00:00", "B1"),
 ("E8", "C4", "U", "Dev",    "Kochi",     "2026-10-07 10:00:00", "B1"),  # update before its insert
 ("E11","C5", "I", "Esha",   "Jaipur",    "bad-ts",              "B1"),  # invalid timestamp
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
