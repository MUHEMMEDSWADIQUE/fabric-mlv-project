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
-- MAGIC from pyspark.sql.window import Window
-- MAGIC 
-- MAGIC EMAIL_REGEX = r"^[^@ ]+@[^@ ]+[.][^@ ]+$"
-- MAGIC 
-- MAGIC def blank_to_null(col_name):
-- MAGIC     """trim; turn empty strings into NULL"""
-- MAGIC     c = F.trim(F.col(col_name))
-- MAGIC     return F.when(c == "", F.lit(None)).otherwise(c)
-- MAGIC 
-- MAGIC def latest_per_key(df, key_col, order_col):
-- MAGIC     """keep newest row per key; NULL timestamps sort last"""
-- MAGIC     w = Window.partitionBy(key_col).orderBy(F.col(order_col).desc_nulls_last())
-- MAGIC     return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC import fmlv
-- MAGIC 
-- MAGIC @fmlv.materialized_lake_view(
-- MAGIC     name="lab4.customers_py",
-- MAGIC     comment="Cleaned, deduplicated customers (PySpark)",
-- MAGIC     replace=True
-- MAGIC )
-- MAGIC @fmlv.check("key_present", "customer_id IS NOT NULL","DROP")
-- MAGIC def customers_py():
-- MAGIC     raw=spark.read.table("lab3.customers_raw")
-- MAGIC     typed = raw.select(
-- MAGIC         blank_to_null("customer_id").alias("customer_id"),
-- MAGIC         F.initcap(F.trim("name")).alias("name"),
-- MAGIC         F.lower(F.trim("email")).alias("email"),
-- MAGIC         F.initcap(F.trim("region")).alias("region"),
-- MAGIC         F.col("updated_at").cast("timestamp").alias("updated_ts")
-- MAGIC     )
-- MAGIC     deduped = latest_per_key(typed,"customer_id","updated_ts")
-- MAGIC     return deduped.select(
-- MAGIC         "customer_id", "name", "email", "region", "updated_ts",
-- MAGIC         F.coalesce(F.col("email").rlike(EMAIL_REGEX), F.lit(False)).alias("email_valid"),
-- MAGIC     )

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC import fmlv
-- MAGIC 
-- MAGIC @fmlv.materialized_lake_view(name="lab4.payments_py", replace=True)
-- MAGIC @fmlv.check("amount_valid", "amount IS NOT NULL", "DROP")
-- MAGIC def payments_py():
-- MAGIC     raw = spark.read.table("lab3.payments_raw")
-- MAGIC     typed = raw.select(
-- MAGIC         F.trim("payment_id").alias("payment_id"),
-- MAGIC         "order_id",
-- MAGIC         F.col("amount").cast("decimal(18,2)").alias("amount"),
-- MAGIC         F.lower(F.trim("method")).alias("method"),
-- MAGIC         F.col("paid_at").cast("timestamp").alias("paid_ts"),
-- MAGIC         F.upper(F.trim("status")).alias("status"),
-- MAGIC     )
-- MAGIC     return latest_per_key(typed, "payment_id", "paid_ts")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC import fmlv
-- MAGIC 
-- MAGIC @fmlv.materialized_lake_view(name="lab4.order_totals_py", replace=True)
-- MAGIC def order_totals_py():
-- MAGIC     o = spark.read.table("lab3.orders")
-- MAGIC     i = spark.read.table("lab3.order_items")
-- MAGIC     lines = (o.join(i, "order_id", "inner")
-- MAGIC               .filter((F.col("status") == "COMPLETED") & (F.col("quantity") > 0))
-- MAGIC               .select(
-- MAGIC                   "order_id", "customer_id",
-- MAGIC                   F.col("order_ts").cast("timestamp").cast("date").alias("order_date"),
-- MAGIC                   (F.col("quantity") * F.col("unit_price")).alias("line_amount")))
-- MAGIC     return (lines.groupBy("order_id", "customer_id", "order_date")
-- MAGIC                  .agg(F.count("*").alias("line_count"),
-- MAGIC                       F.sum("line_amount").alias("order_amount"))
-- MAGIC                  .filter("order_amount > 0"))

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC import fmlv
-- MAGIC 
-- MAGIC @fmlv.materialized_lake_view(
-- MAGIC     name="lab4.recon_py",
-- MAGIC     comment="Reconcile order totals with successful payments",
-- MAGIC     replace=True
-- MAGIC )
-- MAGIC def recon_py():
-- MAGIC     orders = spark.read.table("lab3.order_totals")                       # SQL-authored MLV
-- MAGIC     payments = (spark.read.table("lab4.payments_py")                      # PySpark-authored MLV
-- MAGIC                 .filter(F.col("status") == "SUCCESS"))                    # fix 3
-- MAGIC 
-- MAGIC     payments_by_order = (payments.groupBy("order_id")
-- MAGIC                          .agg(F.sum("amount").alias("aggregated_paid_amount")))   # fix 1
-- MAGIC 
-- MAGIC     joined = orders.join(payments_by_order, on="order_id", how="left")
-- MAGIC 
-- MAGIC     return (joined
-- MAGIC         .withColumn("paid_amount",
-- MAGIC                     F.coalesce(F.col("aggregated_paid_amount"), F.lit(0)))
-- MAGIC         .withColumn("pay_status",
-- MAGIC             F.when(F.col("aggregated_paid_amount").isNull(), F.lit("UNPAID"))  # fix 4
-- MAGIC              .when(F.abs(F.col("paid_amount") - F.col("order_amount")) < 0.005,
-- MAGIC                    F.lit("MATCHED"))
-- MAGIC              .otherwise(F.lit("MISMATCH")))
-- MAGIC         .select("order_id", "order_amount", "paid_amount", "pay_status"))

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW lab4.method_summary_sql AS
-- MAGIC SELECT method, COUNT(*) AS payment_count
-- MAGIC FROM lab4.payments_py
-- MAGIC GROUP BY method;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
