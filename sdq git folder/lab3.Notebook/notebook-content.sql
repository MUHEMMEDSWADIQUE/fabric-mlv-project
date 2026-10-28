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
-- MAGIC # --- Cell 1: PySpark ---
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS lab3")
-- MAGIC 
-- MAGIC def save(rows, cols, name):
-- MAGIC     (spark.createDataFrame(rows, cols)
-- MAGIC           .write.mode("overwrite").format("delta").saveAsTable(name))
-- MAGIC 
-- MAGIC # Orders and items: same shape as Lesson 1, but customer keys are strings
-- MAGIC save([
-- MAGIC   (101,"C1","2026-10-01 09:15:00","COMPLETED"),
-- MAGIC   (102,"C2","2026-10-01 14:40:00","COMPLETED"),
-- MAGIC   (103,"C1","2026-10-02 10:05:00","COMPLETED"),
-- MAGIC   (104,"C3","2026-10-02 16:20:00","CANCELLED"),
-- MAGIC   (105,"C4","2026-10-03 11:00:00","COMPLETED"),
-- MAGIC   (106,"C2","2026-10-03 12:00:00","COMPLETED"),
-- MAGIC ], ["order_id","customer_id","order_ts","status"], "lab3.orders")
-- MAGIC 
-- MAGIC save([
-- MAGIC   (101,"P1", 2,500.0),(101,"P2",1,250.0),(102,"P1",1,500.0),
-- MAGIC   (103,"P3", 3,100.0),(104,"P2",1,250.0),(105,"P1",-1,500.0),
-- MAGIC   (105,"P3", 2,100.0),(106,"P2",2,250.0),
-- MAGIC ], ["order_id","product_id","quantity","unit_price"], "lab3.order_items")
-- MAGIC 
-- MAGIC # Dirty customers: duplicate key, null key, messy case/whitespace, null email, bad timestamp
-- MAGIC save([
-- MAGIC   ("C1","  asha ","ASHA@Example.com","south","2026-10-01 08:00:00"),
-- MAGIC   ("C1","Asha K","asha@example.com","South","2026-10-05 09:00:00"),
-- MAGIC   ("C2","Ben","ben@example.com","NORTH","2026-10-01 08:30:00"),
-- MAGIC   ("C3","Chitra",None,"South","2026-10-02 10:00:00"),
-- MAGIC   (None,"Ghost","ghost@example.com","West","2026-10-03 10:00:00"),
-- MAGIC   ("C4","Dev","dev@example","West","not-a-date"),
-- MAGIC ], ["customer_id","name","email","region","updated_at"], "lab3.customers_raw")
-- MAGIC 
-- MAGIC # Dirty payments: exact duplicate, non-numeric amount, null timestamp, failed payment
-- MAGIC save([
-- MAGIC   ("PAY1",101,"1250.00","upi", "2026-10-01 09:20:00","SUCCESS"),
-- MAGIC   ("PAY2",102,"500.00", "card","2026-10-01 14:45:00","SUCCESS"),
-- MAGIC   ("PAY3",103,"300.00", "UPI", "2026-10-02 10:10:00","SUCCESS"),
-- MAGIC   ("PAY3",103,"300.00", "UPI", "2026-10-02 10:10:00","SUCCESS"),
-- MAGIC   ("PAY4",104,"250.00", "card","2026-10-02 16:25:00","FAILED"),
-- MAGIC   ("PAY5",105,"abc",    "card","2026-10-03 11:05:00","SUCCESS"),
-- MAGIC   ("PAY6",106,"500.00", "cod", None,                 "SUCCESS"),
-- MAGIC ], ["payment_id","order_id","amount","method","paid_at","status"], "lab3.payments_raw")
-- MAGIC 
-- MAGIC # Change Data Feed on all raw sources (so incremental refresh is possible)
-- MAGIC for t in ["orders","order_items","customers_raw","payments_raw"]:
-- MAGIC     spark.sql(f"ALTER TABLE lab3.{t} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from lab3.payments

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM lab3.payments_raw

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE MATERIALIZED LAKE VIEW lab3.customers(
    CONSTRAINT key_present CHECK (customer_id IS NOT NULL) ON MISMATCH DROP
)
COMMENT"Cleaned,deduplicated customers"
TBLPROPERTIES(delta.enableChangeDataFeed = true)
AS
WITH typed AS(
  SELECT
    NULLIF(TRIM(customer_id), '')            AS customer_id,
    INITCAP(TRIM(name))                      AS name,
    LOWER(TRIM(email))                       AS email,
    INITCAP(TRIM(region))                    AS region,
    CAST(updated_at AS TIMESTAMP)            AS updated_ts
  FROM lab3.customers_raw

),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY updated_ts DESC NULLS LAST
    ) AS rn
  FROM typed
)
SELECT
  customer_id, name, email, region, updated_ts,
  COALESCE(email RLIKE '^[^@ ]+@[^@ ]+[.][^@ ]+$', FALSE) AS email_valid
FROM ranked
WHERE rn = 1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- **Why not SELECT DISTINCT for exact duplicates? It works, but DISTINCT is also unsupported by incremental refresh. I chose ROW_NUMBER because it also picks which duplicate wins.**

-- CELL ********************


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

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW lab3.payments (
-- MAGIC   CONSTRAINT amount_valid CHECK (amount IS NOT NULL) ON MISMATCH DROP
-- MAGIC )
-- MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
-- MAGIC AS
-- MAGIC WITH typed AS (
-- MAGIC   SELECT
-- MAGIC     TRIM(payment_id)                  AS payment_id,
-- MAGIC     order_id,
-- MAGIC     CAST(amount AS DECIMAL(18,2))     AS amount,
-- MAGIC     LOWER(TRIM(method))               AS method,
-- MAGIC     CAST(paid_at AS TIMESTAMP)        AS paid_ts,
-- MAGIC     UPPER(TRIM(status))               AS status
-- MAGIC   FROM lab3.payments_raw
-- MAGIC ),
-- MAGIC ranked AS (
-- MAGIC   SELECT *, ROW_NUMBER() OVER (
-- MAGIC     PARTITION BY payment_id ORDER BY paid_ts DESC NULLS LAST) AS rn
-- MAGIC   FROM typed
-- MAGIC )
-- MAGIC SELECT payment_id, order_id, amount, method, paid_ts, status
-- MAGIC FROM ranked WHERE rn = 1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC -- Quarantine view: keep the rejects instead of losing them
-- MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW lab3.payments_rejects AS
-- MAGIC SELECT
-- MAGIC   payment_id,
-- MAGIC   order_id,
-- MAGIC   amount AS raw_amount,
-- MAGIC   'amount_not_numeric' AS reject_reason
-- MAGIC FROM lab3.payments_raw
-- MAGIC WHERE CAST(amount AS DECIMAL(18,2)) IS NULL;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW lab3.order_totals
-- MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
-- MAGIC AS
-- MAGIC WITH lines AS (
-- MAGIC   SELECT
-- MAGIC     o.order_id,
-- MAGIC     o.customer_id,
-- MAGIC     CAST(CAST(o.order_ts AS TIMESTAMP) AS DATE) AS order_date,
-- MAGIC     i.quantity * i.unit_price                   AS line_amount
-- MAGIC   FROM lab3.orders o
-- MAGIC   INNER JOIN lab3.order_items i ON o.order_id = i.order_id
-- MAGIC   WHERE o.status = 'COMPLETED' AND i.quantity > 0
-- MAGIC )
-- MAGIC SELECT
-- MAGIC   order_id, customer_id, order_date,
-- MAGIC   COUNT(*)         AS line_count,
-- MAGIC   SUM(line_amount) AS order_amount
-- MAGIC FROM lines
-- MAGIC GROUP BY order_id, customer_id, order_date
-- MAGIC HAVING SUM(line_amount) > 0;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW lab3.order_payment_recon AS
-- MAGIC SELECT
-- MAGIC   t.order_id,
-- MAGIC   t.order_amount,
-- MAGIC   COALESCE(p.paid_amount, 0) AS paid_amount,
-- MAGIC   CASE
-- MAGIC     WHEN p.paid_amount IS NULL            THEN 'UNPAID'
-- MAGIC     WHEN p.paid_amount = t.order_amount   THEN 'MATCHED'
-- MAGIC     ELSE 'MISMATCH'
-- MAGIC   END AS pay_status
-- MAGIC FROM lab3.order_totals t
-- MAGIC LEFT JOIN (
-- MAGIC   SELECT order_id, SUM(amount) AS paid_amount
-- MAGIC   FROM lab3.payments
-- MAGIC   WHERE status = 'SUCCESS'
-- MAGIC   GROUP BY order_id
-- MAGIC ) p ON t.order_id = p.order_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW lab3.customer_spend_rank AS
-- MAGIC WITH spend AS (
-- MAGIC   SELECT c.customer_id, c.name, c.region, SUM(t.order_amount) AS total_spend
-- MAGIC   FROM lab3.order_totals t
-- MAGIC   INNER JOIN lab3.customers c ON t.customer_id = c.customer_id
-- MAGIC   GROUP BY c.customer_id, c.name, c.region
-- MAGIC )
-- MAGIC SELECT *, RANK() OVER (ORDER BY total_spend DESC) AS spend_rank
-- MAGIC FROM spend;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
