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
-- MAGIC 
-- MAGIC spark.sql("create schema if not exists mlv_bronze")
-- MAGIC spark.sql("create schema if not exists mlv_silver")
-- MAGIC spark.sql("create schema if not exists mlv_gold")
-- MAGIC 
-- MAGIC 
-- MAGIC customers = [
-- MAGIC     (1, "Asha",   "asha@example.com",   "South"),
-- MAGIC     (2, "Ben",    "ben@example.com",    "North"),
-- MAGIC     (3, "Chitra", "chitra@example.com", "South"),
-- MAGIC     (4, "Dev",    "dev@example.com",    "West"),
-- MAGIC ]
-- MAGIC orders = [
-- MAGIC     (101, 1, "2026-10-01 09:15:00", "COMPLETED"),
-- MAGIC     (102, 2, "2026-10-01 14:40:00", "COMPLETED"),
-- MAGIC     (103, 1, "2026-10-02 10:05:00", "COMPLETED"),
-- MAGIC     (104, 3, "2026-10-02 16:20:00", "CANCELLED"),
-- MAGIC     (105, 4, "2026-10-03 11:00:00", "COMPLETED"),
-- MAGIC     (106, 2, "2026-10-03 12:00:00", "COMPLETED"),
-- MAGIC ]
-- MAGIC order_items = [
-- MAGIC     (101, "P1",  2, 500.0),
-- MAGIC     (101, "P2",  1, 250.0),
-- MAGIC     (102, "P1",  1, 500.0),
-- MAGIC     (103, "P3",  3, 100.0),
-- MAGIC     (104, "P2",  1, 250.0),
-- MAGIC     (105, "P1", -1, 500.0),   # deliberately invalid quantity
-- MAGIC     (105, "P3",  2, 100.0),
-- MAGIC     (106, "P2",  2, 250.0),
-- MAGIC ]
-- MAGIC 
-- MAGIC def save(rows, cols, name):
-- MAGIC     (spark.createDataFrame(rows, cols)
-- MAGIC           .write.mode("overwrite").format("delta")
-- MAGIC           .saveAsTable(name))
-- MAGIC 
-- MAGIC save(customers,   ["customer_id","name","email","region"],              "mlv_bronze.customers")
-- MAGIC save(orders,      ["order_id","customer_id","order_ts","status"],       "mlv_bronze.orders")
-- MAGIC save(order_items, ["order_id","product_id","quantity","unit_price"],    "mlv_bronze.order_items")


-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from  mlv_silver.order_lines

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC ALTER TABLE mlv_bronze.orders      SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
-- MAGIC ALTER TABLE mlv_bronze.order_items SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE MATERIALIZED LAKE VIEW   mlv_silver.order_lines(
    CONSTRAINT positive_qty CHECK(quantity > 0) ON MISMATCH DROP
)
COMMENT'Comleted Order Lines With Computed Line Amount '
AS
SELECT 
o.order_id,
  o.customer_id,
  CAST(o.order_ts AS TIMESTAMP)                AS order_ts,
  CAST(CAST(o.order_ts AS TIMESTAMP) AS DATE)  AS order_date,
  i.product_id,
  i.quantity,
  i.unit_price,
  i.quantity * i.unit_price                    AS line_amount
FROM mlv_bronze.orders o 
join mlv_bronze.order_items i ON o.order_id = i.order_id
where o.status='COMPLETED'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE MATERIALIZED LAKE VIEW mlv_gold.daily_sales AS
-- MAGIC SELECT
-- MAGIC   order_date,
-- MAGIC   COUNT(*)         AS line_count,
-- MAGIC   SUM(line_amount) AS revenue
-- MAGIC FROM mlv_silver.order_lines
-- MAGIC GROUP BY order_date;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
