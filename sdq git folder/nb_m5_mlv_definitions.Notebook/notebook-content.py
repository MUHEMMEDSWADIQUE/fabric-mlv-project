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
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.silver_customers (
# MAGIC   CONSTRAINT key_present CHECK (customer_id IS NOT NULL) ON MISMATCH DROP
# MAGIC )
# MAGIC COMMENT "Deduplicated, normalized customers"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     NULLIF(TRIM(customer_id), '')   AS customer_id,
# MAGIC     INITCAP(TRIM(full_name))        AS customer_name,
# MAGIC     LOWER(TRIM(email))              AS email,
# MAGIC     TRIM(phone)                     AS phone,
# MAGIC     INITCAP(TRIM(region))           AS region,
# MAGIC     CAST(signup_date AS DATE)       AS signup_date,
# MAGIC     CAST(updated_at AS TIMESTAMP)   AS updated_ts
# MAGIC   FROM retail.bronze_customers
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (
# MAGIC     PARTITION BY customer_id ORDER BY updated_ts DESC NULLS LAST) AS rn
# MAGIC   FROM typed
# MAGIC )
# MAGIC SELECT customer_id, customer_name, email, phone, region, signup_date, updated_ts,
# MAGIC        COALESCE(email RLIKE '^[^@ ]+@[^@ ]+[.][^@ ]+$', FALSE) AS email_valid
# MAGIC FROM ranked
# MAGIC WHERE rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.silver_products (
# MAGIC   CONSTRAINT key_present   CHECK (product_id IS NOT NULL) ON MISMATCH DROP,
# MAGIC   CONSTRAINT price_positive CHECK (list_price IS NOT NULL AND list_price > 0) ON MISMATCH DROP
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     NULLIF(TRIM(product_id), '')          AS product_id,
# MAGIC     TRIM(product_name)                    AS product_name,
# MAGIC     INITCAP(TRIM(category))               AS category,
# MAGIC     CAST(unit_cost  AS DECIMAL(18,2))     AS unit_cost,
# MAGIC     CAST(list_price AS DECIMAL(18,2))     AS list_price,
# MAGIC     CAST(updated_at AS TIMESTAMP)         AS updated_ts
# MAGIC   FROM retail.bronze_products
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (
# MAGIC     PARTITION BY product_id ORDER BY updated_ts DESC NULLS LAST) AS rn
# MAGIC   FROM typed
# MAGIC )
# MAGIC SELECT product_id, product_name, category, unit_cost, list_price, updated_ts
# MAGIC FROM ranked WHERE rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.silver_orders (
# MAGIC   CONSTRAINT order_key_present CHECK (order_id IS NOT NULL) ON MISMATCH DROP,
# MAGIC   CONSTRAINT order_ts_valid    CHECK (order_ts IS NOT NULL) ON MISMATCH DROP
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     CAST(order_id AS INT)            AS order_id,
# MAGIC     NULLIF(TRIM(customer_id), '')    AS customer_id,
# MAGIC     CAST(order_ts AS TIMESTAMP)      AS order_ts,
# MAGIC     UPPER(TRIM(status))              AS status,
# MAGIC     CAST(_ingested_at AS TIMESTAMP)  AS ingested_ts
# MAGIC   FROM retail.bronze_orders
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (
# MAGIC     PARTITION BY order_id ORDER BY ingested_ts DESC) AS rn
# MAGIC   FROM typed
# MAGIC )
# MAGIC SELECT order_id, customer_id, order_ts, CAST(order_ts AS DATE) AS order_date,
# MAGIC        status, ingested_ts
# MAGIC FROM ranked WHERE rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.silver_order_items (
# MAGIC   CONSTRAINT order_present CHECK (order_id IS NOT NULL) ON MISMATCH DROP,
# MAGIC   CONSTRAINT qty_positive  CHECK (quantity IS NOT NULL AND quantity > 0) ON MISMATCH DROP,
# MAGIC   CONSTRAINT price_valid   CHECK (unit_price IS NOT NULL AND unit_price >= 0) ON MISMATCH DROP
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     CAST(order_id AS INT)                AS order_id,
# MAGIC     CAST(line_no AS INT)                 AS line_no,
# MAGIC     TRIM(product_id)                     AS product_id,
# MAGIC     CAST(quantity AS INT)                AS quantity,
# MAGIC     CAST(unit_price AS DECIMAL(18,2))    AS unit_price,
# MAGIC     CAST(_ingested_at AS TIMESTAMP)      AS ingested_ts
# MAGIC   FROM retail.bronze_order_items
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (
# MAGIC     PARTITION BY order_id, line_no ORDER BY ingested_ts DESC) AS rn
# MAGIC   FROM typed
# MAGIC )
# MAGIC SELECT order_id, line_no, product_id, quantity, unit_price,
# MAGIC        quantity * unit_price AS line_amount
# MAGIC FROM ranked WHERE rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.silver_payments (
# MAGIC   CONSTRAINT amount_valid CHECK (amount IS NOT NULL) ON MISMATCH DROP
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     TRIM(payment_id)                  AS payment_id,
# MAGIC     CAST(order_id AS INT)             AS order_id,
# MAGIC     CAST(amount AS DECIMAL(18,2))     AS amount,
# MAGIC     LOWER(TRIM(method))               AS method,
# MAGIC     CAST(paid_at AS TIMESTAMP)        AS paid_ts,
# MAGIC     UPPER(TRIM(status))               AS status,
# MAGIC     CAST(_ingested_at AS TIMESTAMP)   AS ingested_ts
# MAGIC   FROM retail.bronze_payments
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (
# MAGIC     PARTITION BY payment_id
# MAGIC     ORDER BY ingested_ts DESC, paid_ts DESC NULLS LAST) AS rn
# MAGIC   FROM typed
# MAGIC )
# MAGIC SELECT payment_id, order_id, amount, method, paid_ts, status
# MAGIC FROM ranked WHERE rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.silver_sales_lines
# MAGIC COMMENT "Completed order lines enriched with region and product cost"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC AS
# MAGIC SELECT
# MAGIC   i.order_id, i.line_no, o.order_date, o.customer_id,
# MAGIC   COALESCE(c.region, 'Unknown')            AS region,
# MAGIC   i.product_id,
# MAGIC   COALESCE(p.product_name, 'Unknown')      AS product_name,
# MAGIC   COALESCE(p.category, 'Unknown')          AS category,
# MAGIC   i.quantity, i.unit_price, i.line_amount,
# MAGIC   i.quantity * p.unit_cost                 AS line_cost
# MAGIC FROM retail.silver_order_items i
# MAGIC INNER JOIN retail.silver_orders o     ON i.order_id    = o.order_id
# MAGIC LEFT  JOIN retail.silver_customers c  ON o.customer_id = c.customer_id
# MAGIC LEFT  JOIN retail.silver_products p   ON i.product_id  = p.product_id
# MAGIC WHERE o.status = 'COMPLETED';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.silver_dq_summary AS
# MAGIC SELECT 'customers_rows_removed' AS check_name,
# MAGIC   (SELECT COUNT(*) FROM retail.bronze_customers) AS checked_rows,
# MAGIC   (SELECT COUNT(*) FROM retail.bronze_customers) - (SELECT COUNT(*) FROM retail.silver_customers) AS issue_rows
# MAGIC UNION ALL SELECT 'products_rows_removed',
# MAGIC   (SELECT COUNT(*) FROM retail.bronze_products),
# MAGIC   (SELECT COUNT(*) FROM retail.bronze_products) - (SELECT COUNT(*) FROM retail.silver_products)
# MAGIC UNION ALL SELECT 'orders_rows_removed',
# MAGIC   (SELECT COUNT(*) FROM retail.bronze_orders),
# MAGIC   (SELECT COUNT(*) FROM retail.bronze_orders) - (SELECT COUNT(*) FROM retail.silver_orders)
# MAGIC UNION ALL SELECT 'order_items_rows_removed',
# MAGIC   (SELECT COUNT(*) FROM retail.bronze_order_items),
# MAGIC   (SELECT COUNT(*) FROM retail.bronze_order_items) - (SELECT COUNT(*) FROM retail.silver_order_items)
# MAGIC UNION ALL SELECT 'payments_rows_removed',
# MAGIC   (SELECT COUNT(*) FROM retail.bronze_payments),
# MAGIC   (SELECT COUNT(*) FROM retail.bronze_payments) - (SELECT COUNT(*) FROM retail.silver_payments)
# MAGIC UNION ALL SELECT 'orders_unknown_customer',
# MAGIC   (SELECT COUNT(*) FROM retail.silver_orders),
# MAGIC   (SELECT COUNT(*) FROM retail.silver_orders o
# MAGIC      LEFT JOIN retail.silver_customers c ON o.customer_id = c.customer_id
# MAGIC      WHERE c.customer_id IS NULL)
# MAGIC UNION ALL SELECT 'order_items_missing_order',
# MAGIC   (SELECT COUNT(*) FROM retail.silver_order_items),
# MAGIC   (SELECT COUNT(*) FROM retail.silver_order_items i
# MAGIC      LEFT JOIN retail.silver_orders o ON i.order_id = o.order_id
# MAGIC      WHERE o.order_id IS NULL)
# MAGIC UNION ALL SELECT 'payments_missing_order',
# MAGIC   (SELECT COUNT(*) FROM retail.silver_payments),
# MAGIC   (SELECT COUNT(*) FROM retail.silver_payments p
# MAGIC      LEFT JOIN retail.silver_orders o ON p.order_id = o.order_id
# MAGIC      WHERE o.order_id IS NULL);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.gold_daily_sales
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC AS
# MAGIC WITH per_order AS (
# MAGIC   SELECT order_date, order_id,
# MAGIC          SUM(quantity) AS units, SUM(line_amount) AS order_revenue
# MAGIC   FROM retail.silver_sales_lines
# MAGIC   GROUP BY order_date, order_id
# MAGIC )
# MAGIC SELECT order_date,
# MAGIC        COUNT(*)           AS orders,
# MAGIC        SUM(units)         AS units,
# MAGIC        SUM(order_revenue) AS revenue
# MAGIC FROM per_order
# MAGIC GROUP BY order_date;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.gold_monthly_sales AS
# MAGIC WITH m AS (
# MAGIC   SELECT trunc(order_date, 'MM') AS month_start, orders, units, revenue
# MAGIC   FROM retail.gold_daily_sales
# MAGIC )
# MAGIC SELECT month_start,
# MAGIC        SUM(orders) AS orders, SUM(units) AS units, SUM(revenue) AS revenue
# MAGIC FROM m GROUP BY month_start;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.gold_product_performance AS
# MAGIC SELECT product_id, product_name, category,
# MAGIC        SUM(quantity)                         AS units_sold,
# MAGIC        SUM(line_amount)                      AS revenue,
# MAGIC        SUM(line_cost)                        AS cost,
# MAGIC        SUM(line_amount) - SUM(line_cost)     AS gross_margin
# MAGIC FROM retail.silver_sales_lines
# MAGIC GROUP BY product_id, product_name, category;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.gold_regional_revenue AS
# MAGIC WITH per_order AS (
# MAGIC   SELECT region, order_id, SUM(line_amount) AS order_revenue
# MAGIC   FROM retail.silver_sales_lines
# MAGIC   GROUP BY region, order_id
# MAGIC )
# MAGIC SELECT region, COUNT(*) AS orders, SUM(order_revenue) AS revenue
# MAGIC FROM per_order GROUP BY region;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.gold_customer_lifetime_value AS
# MAGIC WITH per_order AS (
# MAGIC   SELECT customer_id, order_id, order_date, SUM(line_amount) AS order_revenue
# MAGIC   FROM retail.silver_sales_lines
# MAGIC   GROUP BY customer_id, order_id, order_date
# MAGIC ),
# MAGIC totals AS (
# MAGIC   SELECT customer_id,
# MAGIC          COUNT(*)           AS total_orders,
# MAGIC          SUM(order_revenue) AS lifetime_revenue,
# MAGIC          MIN(order_date)    AS first_order_date,
# MAGIC          MAX(order_date)    AS last_order_date
# MAGIC   FROM per_order GROUP BY customer_id
# MAGIC )
# MAGIC SELECT t.customer_id,
# MAGIC        COALESCE(c.customer_name, 'Unknown')            AS customer_name,
# MAGIC        t.total_orders, t.lifetime_revenue,
# MAGIC        ROUND(t.lifetime_revenue / t.total_orders, 2)   AS avg_order_value,
# MAGIC        t.first_order_date, t.last_order_date
# MAGIC FROM totals t
# MAGIC LEFT JOIN retail.silver_customers c ON t.customer_id = c.customer_id;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW retail.gold_payment_summary AS
# MAGIC SELECT method,
# MAGIC        COUNT(*) AS payment_count,
# MAGIC        SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) AS success_amount,
# MAGIC        SUM(CASE WHEN status = 'FAILED'  THEN amount ELSE 0 END) AS failed_amount
# MAGIC FROM retail.silver_payments
# MAGIC GROUP BY method;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Object	Reads from	Notes
# silver_customers/products/orders/order_items/payments	their bronze table	window dedup, so full refresh
# silver_sales_lines	silver order_items, orders, customers, products	LEFT joins, so full refresh when customers or products change
# silver_dq_summary	all five bronze and silver tables	scalar subqueries, so full refresh
# gold_daily_sales, gold_product_performance, gold_regional_revenue	silver_sales_lines	aggregates
# gold_monthly_sales	gold_daily_sales	gold-on-gold
# gold_customer_lifetime_value	silver_sales_lines, silver_customers	LEFT join
# gold_payment_summary	silver_payments	independent of the sales branch
