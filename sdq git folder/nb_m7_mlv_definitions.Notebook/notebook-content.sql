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

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW m7.events_clean (
-- MAGIC   CONSTRAINT key_present CHECK (customer_id IS NOT NULL) ON MISMATCH DROP,
-- MAGIC   CONSTRAINT ts_valid    CHECK (event_time IS NOT NULL)  ON MISMATCH DROP,
-- MAGIC   CONSTRAINT op_valid    CHECK (op IN ('I','U','D'))     ON MISMATCH DROP
-- MAGIC )
-- MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
-- MAGIC AS
-- MAGIC WITH typed AS (
-- MAGIC   SELECT event_id,
-- MAGIC          NULLIF(TRIM(customer_id), '')  AS customer_id,
-- MAGIC          UPPER(TRIM(op))                AS op,
-- MAGIC          INITCAP(TRIM(name))            AS name,
-- MAGIC          INITCAP(TRIM(city))            AS city,
-- MAGIC          CAST(event_ts AS TIMESTAMP)    AS event_time
-- MAGIC   FROM m7.cdc_events
-- MAGIC ),
-- MAGIC dedup AS (
-- MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time) AS rn
-- MAGIC   FROM typed
-- MAGIC )
-- MAGIC SELECT event_id, customer_id, op, name, city, event_time
-- MAGIC FROM dedup WHERE rn = 1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from m7.events_clean

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from m7.customer_history

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW m7.customer_history
-- MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
-- MAGIC AS
-- MAGIC WITH ordered AS (
-- MAGIC   SELECT customer_id, op, name, city, event_time,
-- MAGIC          LEAD(event_time) OVER (
-- MAGIC            PARTITION BY customer_id ORDER BY event_time, event_id) AS next_time
-- MAGIC   FROM m7.events_clean
-- MAGIC )
-- MAGIC SELECT customer_id, name, city,
-- MAGIC        event_time AS valid_from,
-- MAGIC        next_time  AS valid_to,
-- MAGIC        (next_time IS NULL) AS is_current
-- MAGIC FROM ordered
-- MAGIC WHERE op <> 'D';

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW m7.customer_current AS
-- MAGIC SELECT customer_id, name, city, valid_from AS last_changed
-- MAGIC FROM m7.customer_history
-- MAGIC WHERE is_current;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
