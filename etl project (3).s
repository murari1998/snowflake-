

-- 1. Database and Schemas
CREATE OR REPLACE DATABASE projectdb;
CREATE OR REPLACE SCHEMA projectdb.snowpipe_schema;
CREATE OR REPLACE SCHEMA projectdb.table_schema_dim;
CREATE OR REPLACE SCHEMA projectdb.file_formate_schema_dim;
CREATE OR REPLACE SCHEMA projectdb.pro_stage_schema;

-- 2. File Format
CREATE OR REPLACE FILE FORMAT projectdb.file_formate_schema_dim.jsonformat
TYPE = JSON;

-- 3. Bronze Table
CREATE OR REPLACE TABLE projectdb.table_schema_dim.dim_customer (
    row_column VARIANT,
    file_name STRING,
    load_time TIMESTAMP
);

-- 4. Stage
CREATE OR REPLACE STAGE projectdb.pro_stage_schema.s3_snowpipe_stage
URL = 's3://myprojectscd2'
CREDENTIALS = (
  AWS_KEY_ID = '',
  AWS_SECRET_KEY = ''
);
-- 5. Snowpipe
CREATE OR REPLACE PIPE projectdb.snowpipe_schema.dim_pipe
AUTO_INGEST = TRUE
AS
COPY INTO projectdb.table_schema_dim.dim_customer
(row_column, file_name, load_time)
FROM (
    SELECT $1, METADATA$FILENAME, METADATA$FILE_LAST_MODIFIED
    FROM @projectdb.pro_stage_schema.s3_snowpipe_stage
)
FILE_FORMAT = projectdb.file_formate_schema_dim.jsonformat;

DESC PIPE projectdb.snowpipe_schema.dim_pipe;
SELECT * FROM projectdb.table_schema_dim.dim_customer;
select count(*) from projectdb.table_schema_dim.dim_customer;

-- 6. Stream (Captured from Bronze)
CREATE OR REPLACE STREAM projectdb.table_schema_dim.dim_customer_stream
ON TABLE projectdb.table_schema_dim.dim_customer
APPEND_ONLY = TRUE;

desc stream projectdb.table_schema_dim.dim_customer_stream;

-- 7. Silver Table (Target for SCD Type 2)
CREATE OR REPLACE TABLE projectdb.table_schema_dim.dim_customer_silver (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    phone_number STRING,
    city STRING,
    state STRING,
    country STRING,
    customer_segment STRING,
    customer_status STRING,
    source_system STRING,
    file_name STRING,
    record_hash STRING,
    eff_start_dt TIMESTAMP,
    eff_end_dt TIMESTAMP,
    is_current BOOLEAN
);

-- 8. SCD Type 2 Task
CREATE OR REPLACE TASK projectdb.table_schema_dim.process_customer_scd2
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('projectdb.table_schema_dim.dim_customer_stream')
AS
MERGE INTO projectdb.table_schema_dim.dim_customer_silver AS target
USING (
    SELECT sub.* FROM (
        -- PART A: Records to Insert (New or Changed)
        SELECT
            row_column:CUSTOMER_ID::STRING as cid,
            row_column:CUSTOMER_NAME::STRING as cname,
            row_column:EMAIL_ID::STRING as email,
            row_column:PHONE_NUMBER::STRING as phone,
            row_column:CITY::STRING as city,
            row_column:STATE::STRING as state,
            row_column:COUNTRY::STRING as country,
            row_column:CUSTOMER_SEGMENT::STRING as segment,
            row_column:CUSTOMER_STATUS::STRING as status,
            row_column:SOURCE_SYSTEM::STRING as source,
            file_name as fname,
            HASH(row_column:CUSTOMER_NAME, row_column:EMAIL_ID, row_column:CITY)::STRING as current_row_hash,
            load_time as load_ts,
            NULL AS merge_key
        FROM projectdb.table_schema_dim.dim_customer_stream s
        WHERE NOT EXISTS (
            SELECT 1 FROM projectdb.table_schema_dim.dim_customer_silver f
            WHERE f.customer_id = s.row_column:CUSTOMER_ID::STRING
            AND f.record_hash = HASH(s.row_column:CUSTOMER_NAME, s.row_column:EMAIL_ID, s.row_column:CITY)::STRING
            AND f.is_current = TRUE
        )

        UNION ALL

        -- PART B: Records to Mark as Expired
        SELECT
            row_column:CUSTOMER_ID::STRING as cid,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL as current_row_hash,
            load_time as load_ts,
            row_column:CUSTOMER_ID::STRING AS merge_key
        FROM projectdb.table_schema_dim.dim_customer_stream s
        WHERE EXISTS (
            SELECT 1 FROM projectdb.table_schema_dim.dim_customer_silver f
            WHERE f.customer_id = s.row_column:CUSTOMER_ID::STRING
            AND f.record_hash <> HASH(s.row_column:CUSTOMER_NAME, s.row_column:EMAIL_ID, s.row_column:CITY)::STRING
            AND f.is_current = TRUE
        )
    ) sub
) AS src
ON target.customer_id = src.merge_key
AND target.is_current = TRUE

WHEN MATCHED THEN
    UPDATE SET
        target.eff_end_dt = src.load_ts,
        target.is_current = FALSE

WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, email, phone_number, city, state, country,
            customer_segment, customer_status, source_system, file_name, record_hash,
            eff_start_dt, eff_end_dt, is_current)
    VALUES (src.cid, src.cname, src.email, src.phone, src.city, src.state, src.country,
            src.segment, src.status, src.source, src.fname, src.current_row_hash,
            src.load_ts, '9999-12-31'::TIMESTAMP_NTZ, TRUE);

-- Start the Task
ALTER TASK projectdb.table_schema_dim.process_customer_scd2 RESUME;

SELECT * FROM projectdb.table_schema_dim.dim_customer_silver;
select count(*) from projectdb.table_schema_dim.dim_customer_silver;

SELECT SYSTEM$PIPE_STATUS('projectdb.snowpipe_schema.dim_pipe');

SELECT COUNT(*) 
FROM projectdb.table_schema_dim.dim_customer_stream;

-- -----------

-- GOLD LAYER

CREATE OR REPLACE SCHEMA projectdb.gold_schema;

-- Gold Dimension Table
CREATE OR REPLACE TABLE projectdb.gold_schema.dim_customer_gold (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    phone_number STRING,
    city STRING,
    state STRING,
    country STRING,
    customer_segment STRING,
    customer_status STRING,
    source_system STRING,
    file_name STRING,
    eff_start_dt TIMESTAMP,
    eff_end_dt TIMESTAMP,
    is_current BOOLEAN,
    gold_load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Gold Fact Table

CREATE OR REPLACE TABLE projectdb.gold_schema.fact_customer_gold (
    customer_id STRING,
    rating_score INT,
    load_year INT,
    load_month INT,
    load_day INT,
    record_start_date TIMESTAMP_NTZ
);

-- Gold Dimension Task
CREATE OR REPLACE TASK projectdb.table_schema_dim.gold_dim_task
WAREHOUSE = COMPUTE_WH
AFTER projectdb.table_schema_dim.process_customer_scd2
AS
INSERT INTO projectdb.gold_schema.dim_customer_gold
SELECT
    customer_id, customer_name, email, phone_number,
    city, state, country, customer_segment,
    customer_status, source_system, file_name,
    eff_start_dt, eff_end_dt, is_current, CURRENT_TIMESTAMP()
FROM projectdb.table_schema_dim.dim_customer_silver;

-- gold fact task

CREATE OR REPLACE TASK projectdb.table_schema_dim.gold_fact_task
WAREHOUSE = COMPUTE_WH
AFTER projectdb.table_schema_dim.gold_dim_task
AS
INSERT INTO projectdb.gold_schema.fact_customer_gold
SELECT
    customer_id,
    CASE
        WHEN customer_segment = 'Premium' THEN 100
        WHEN customer_segment = 'Gold' THEN 80
        ELSE 50
    END,
    YEAR(eff_start_dt),
    MONTH(eff_start_dt),
    DAY(eff_start_dt),
    eff_start_dt
FROM projectdb.table_schema_dim.dim_customer_silver
WHERE is_current = TRUE;



-- ALTER TASK projectdb.table_schema_dim.process_customer_scd2 suspend;
-- Correct Task Resume Order
ALTER TASK projectdb.table_schema_dim.process_customer_scd2 resume;
ALTER TASK projectdb.table_schema_dim.gold_dim_task RESUME;
ALTER TASK projectdb.table_schema_dim.gold_fact_task RESUME;





SELECT * FROM projectdb.table_schema_dim.dim_customer_silver;
select count(*) from projectdb.table_schema_dim.dim_customer_silver;
SELECT * FROM projectdb.gold_schema.dim_customer_gold;
SELECT * FROM projectdb.gold_schema.fact_customer_gold;












