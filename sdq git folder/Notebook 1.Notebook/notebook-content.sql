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

-- INSERT INTO bronze.orders VALUES
--     ('O107', 'C01', 400.00, 'Completed');

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- CREATE TABLE IF NOT EXISTS bronze.orders (
--     order_id STRING,
--     customer_id STRING,
--     amount DECIMAL(12,2),
--     status STRING
-- )
-- USING DELTA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- CREATE TABLE IF NOT EXISTS bronze.customers (
--     customer_id STRING,
--     customer_name STRING,
--     country STRING
-- )
-- USING DELTA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- DELETE FROM bronze.customers;
-- DELETE FROM bronze.orders;

-- INSERT INTO bronze.customers VALUES
--     ('C01', 'Alice', 'India'),
--     ('C02', 'Bob', 'Japan'),
--     ('C03', 'Charlie', 'India');

-- INSERT INTO bronze.orders VALUES
--     ('O101', 'C01', 500.00, 'Completed'),
--     ('O102', 'C02', 300.00, 'Pending'),
--     ('O103', 'C01', 200.00, 'Completed'),
--     ('O104', 'C03', 100.00, 'Completed'),
--     ('O105', 'C99', 250.00, 'Completed'),
--     ('O106', 'C02', -50.00, 'Completed');

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM bronze.customers;
SELECT * FROM bronze.orders;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE MATERIALIZED LAKE VIEW
    silver.clean_orders
AS
SELECT
    order_id,
    customer_id,
    amount,
    status
FROM bronze.orders
WHERE amount > 0
  AND status = 'Completed';

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT *
FROM silver.clean_orders
ORDER BY order_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE MATERIALIZED LAKE VIEW
    silver.customer_orders
AS
SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.country,
    o.amount,
    o.status
FROM silver.clean_orders o
INNER JOIN bronze.customers c
    ON o.customer_id = c.customer_id;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE MATERIALIZED LAKE VIEW
    silver.valid_orders
(
    CONSTRAINT valid_amount
        CHECK (amount > 0)
        ON MISMATCH DROP
)
AS
SELECT
    order_id,
    customer_id,
    amount,
    status
FROM bronze.orders;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from silver.customer_orders

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
