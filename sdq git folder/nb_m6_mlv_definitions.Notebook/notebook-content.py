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

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.orders_clean (
# MAGIC   CONSTRAINT ts_valid CHECK (order_ts IS NOT NULL) ON MISMATCH DROP
# MAGIC )
# MAGIC (
# MAGIC   REFRESH_HINT pk UNIQUE (order_id)
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC AS
# MAGIC SELECT order_id, customer_id,
# MAGIC        CAST(order_ts AS TIMESTAMP)                AS order_ts,
# MAGIC        CAST(CAST(order_ts AS TIMESTAMP) AS DATE)  AS order_date,
# MAGIC        status, order_total
# MAGIC FROM retail.current_orders
# MAGIC WHERE status <> 'CANCELLED';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.daily_orders_inc
# MAGIC AS
# MAGIC SELECT order_date, COUNT(*) AS orders, SUM(order_total) AS revenue
# MAGIC FROM retail.orders_clean
# MAGIC GROUP BY order_date;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
