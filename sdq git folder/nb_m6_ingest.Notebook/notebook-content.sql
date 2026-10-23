-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "8dc76fcf-6ff3-413f-90a1-c1d732ffa899",
-- META       "default_lakehouse_name": "SDQ_sample_ls",
-- META       "default_lakehouse_workspace_id": "66e95359-350d-4abd-9b10-110ec6cfd52b",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "8dc76fcf-6ff3-413f-90a1-c1d732ffa899"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # Create materialized lake views 
-- 1. Use this notebook to create materialized lake views. 
-- 2. Select **Run all** to run the notebook. 
-- 3. When the notebook run is completed, return to your lakehouse and refresh your materialized lake views graph. 


-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from delta.tables import DeltaTable
-- MAGIC 
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS retail")
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE IF NOT EXISTS retail.current_orders (
-- MAGIC   order_id INT, customer_id STRING, order_ts STRING,
-- MAGIC   status STRING, order_total DECIMAL(18,2)
-- MAGIC ) USING DELTA
-- MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
-- MAGIC """)
-- MAGIC 
-- MAGIC SCHEMA = "order_id INT, customer_id STRING, order_ts STRING, status STRING, order_total STRING"
-- MAGIC 
-- MAGIC def upsert_orders(rows):
-- MAGIC     src = (spark.createDataFrame(rows, SCHEMA)
-- MAGIC            .withColumn("order_total", F.col("order_total").cast("decimal(18,2)")))
-- MAGIC     (DeltaTable.forName(spark, "retail.current_orders").alias("t")
-- MAGIC         .merge(src.alias("s"), "t.order_id = s.order_id")
-- MAGIC         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
-- MAGIC 
-- MAGIC def delete_orders(ids):
-- MAGIC     DeltaTable.forName(spark, "retail.current_orders").delete(
-- MAGIC         f"order_id IN ({','.join(str(i) for i in ids)})")
-- MAGIC 
-- MAGIC # Initial load
-- MAGIC upsert_orders([
-- MAGIC   (1,"C1","2026-10-01 09:00:00","COMPLETED","100.00"),
-- MAGIC   (2,"C2","2026-10-01 10:00:00","COMPLETED","200.00"),
-- MAGIC   (3,"C1","2026-10-02 11:00:00","COMPLETED","300.00"),
-- MAGIC   (4,"C3","2026-10-02 12:00:00","CANCELLED", "50.00"),
-- MAGIC   (5,"C2","bad-ts",             "COMPLETED", "75.00"),
-- MAGIC ])

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC upsert_orders([(3,"C1","2026-10-02 11:00:00","COMPLETED","350.00")])

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }
