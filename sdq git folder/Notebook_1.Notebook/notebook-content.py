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
# META         },
# META         {
# META           "id": "dec51d66-dfd0-4cf0-b73f-e25982a7d333"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "1a47ea5d-b01f-4bba-ac95-92c1f5c35c73",
# META       "known_warehouses": [
# META         {
# META           "id": "1a47ea5d-b01f-4bba-ac95-92c1f5c35c73",
# META           "type": "Lakewarehouse"
# META         },
# META         {
# META           "id": "e2882326-ebab-4b82-8c4c-cee290e6e5d4",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.sql("SHOW SCHEMAS").show(truncate=False)
spark.sql("SHOW TABLES IN test").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT 1 AS test").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT *
    FROM test.countries
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(spark.catalog.currentDatabase())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
