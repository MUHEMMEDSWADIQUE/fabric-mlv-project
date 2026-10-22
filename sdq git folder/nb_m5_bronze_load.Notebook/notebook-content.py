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

from pyspark.sql import functions as F
spark.sql("CREATE SCHEMA IF NOT EXISTS retail")

def load_batch(rows, cols, table, batch_id, ingested_at):
    """Append a raw batch. Everything stays a string; metadata columns added."""
    df = (spark.createDataFrame(rows, cols)
          .withColumn("_batch_id", F.lit(batch_id))
          .withColumn("_ingested_at", F.lit(ingested_at)))
    df.write.mode("append").format("delta").saveAsTable(f"retail.{table}")

B1, T1 = "B1", "2026-10-05 00:00:00"

load_batch([
 ("C1","  asha ","ASHA@Example.com","9876500001","south","2026-09-01","2026-09-01 08:00:00"),
 ("C1","Asha K","asha@example.com","9876500001","South","2026-09-01","2026-09-20 09:00:00"),
 ("C2","Ben","ben@example.com","9876500002","North","2026-09-03","2026-09-03 08:30:00"),
 ("C3","Chitra",None,"9876500003","South","2026-09-10","2026-09-10 10:00:00"),
 (None,"Ghost","ghost@example.com","9876500000","West","2026-09-12","2026-09-12 10:00:00"),
 ("C4","Dev","dev@example","9876500004","West","2026-09-15","not-a-date"),
 ("C5","Esha","esha@example.com","9876500005","East","2026-09-18","2026-09-18 11:00:00"),
], ["customer_id","full_name","email","phone","region","signup_date","updated_at"],
   "bronze_customers", B1, T1)

load_batch([
 ("P1","Phone Case","Accessories","120","500","2026-09-01 00:00:00"),
 ("P2","Charger","Accessories","90","240","2026-09-01 00:00:00"),
 ("P2","Charger","Accessories","90","250","2026-09-15 00:00:00"),
 ("P3","Cable","Accessories","30","100","2026-09-01 00:00:00"),
 ("P4","Earbuds","Audio","800","1500","2026-09-01 00:00:00"),
 ("P9","Broken Item","Accessories","10","-5","2026-09-01 00:00:00"),
], ["product_id","product_name","category","unit_cost","list_price","updated_at"],
   "bronze_products", B1, T1)

load_batch([
 ("101","C1","2026-10-01 09:15:00","COMPLETED"),
 ("102","C2","2026-10-01 14:40:00","COMPLETED"),
 ("102","C2","2026-10-01 14:40:00","COMPLETED"),   # duplicate delivery
 ("103","C1","2026-10-02 10:05:00","COMPLETED"),
 ("104","C3","2026-10-02 16:20:00","CANCELLED"),
 ("105","C4","2026-10-03 11:00:00","COMPLETED"),
 ("106","C2","2026-10-03 12:00:00","COMPLETED"),
 ("107","C9","2026-10-03 15:00:00","COMPLETED"),   # customer C9 unknown yet
 ("108","C5","bad-ts","COMPLETED"),                # invalid timestamp
], ["order_id","customer_id","order_ts","status"], "bronze_orders", B1, T1)

load_batch([
 ("101","1","P1","2","500"),("101","2","P2","1","250"),
 ("102","1","P1","1","500"),("103","1","P3","3","100"),
 ("104","1","P2","1","250"),("105","1","P1","-1","500"),  # invalid qty
 ("105","2","P3","2","100"),("106","1","P2","2","250"),
 ("107","1","P4","1","1500"),("108","1","P1","1","500"),  # order 108 will be rejected
], ["order_id","line_no","product_id","quantity","unit_price"],
   "bronze_order_items", B1, T1)

load_batch([
 ("PAY1","101","1250.00","upi","2026-10-01 09:20:00","SUCCESS"),
 ("PAY2","102","500.00","card","2026-10-01 14:45:00","SUCCESS"),
 ("PAY3","103","300.00","UPI","2026-10-02 10:10:00","SUCCESS"),
 ("PAY3","103","300.00","UPI","2026-10-02 10:10:00","SUCCESS"),  # duplicate
 ("PAY4","104","250.00","card","2026-10-02 16:25:00","FAILED"),
 ("PAY5","105","abc","card","2026-10-03 11:05:00","SUCCESS"),    # invalid amount
 ("PAY6","106","500.00","cod",None,"SUCCESS"),                    # null timestamp
 ("PAY7","107","1500.00","card","2026-10-03 15:05:00","SUCCESS"),
], ["payment_id","order_id","amount","method","paid_at","status"],
   "bronze_payments", B1, T1)

# Enable Change Data Feed on every bronze table (needed for incremental eligibility)
for t in ["customers","products","orders","order_items","payments"]:
    spark.sql(f"ALTER TABLE retail.bronze_{t} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -- %%sql
# -- SELECT 'customers' t, COUNT(*) n FROM retail.bronze_customers UNION ALL
# -- SELECT 'products', COUNT(*) FROM retail.bronze_products UNION ALL
# -- SELECT 'orders', COUNT(*) FROM retail.bronze_orders UNION ALL
# -- SELECT 'order_items', COUNT(*) FROM retail.bronze_order_items UNION ALL
# -- SELECT 'payments', COUNT(*) FROM retail.bronze_payments;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
