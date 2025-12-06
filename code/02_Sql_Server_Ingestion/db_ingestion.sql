/*
 SCRIPT : Load raw CSV data into staging tables for CLAIMS project
 AUTHOR : Sakkoum Hamza
 DATE   : 2025-12
 PURPOSE:
   - Create clean staging tables (policy_raw, claim_raw, customer_raw)
   - Bulk import CSV files mounted in /var/opt/mssql/datasets/
   - Insert into curated staging tables with ingest metadata
 
 WARNINGS:
   - THIS SCRIPT DROPS RAW STAGING TABLES (policy_raw, claim_raw, customer_raw)
   - Ensure CSV files exist in /var/opt/mssql/datasets before running

*/




USE claims;



-- 1. Policy table

DROP TABLE staging.policy_raw

CREATE TABLE staging.policy_raw (
    policy_no           VARCHAR(100), -- dirty, NOT a PK
    customer_id         VARCHAR(100),
    coverage_type       VARCHAR(100),
    policy_start_date   VARCHAR(50),
    policy_end_date     VARCHAR(50),
    make                VARCHAR(100),
    model               VARCHAR(100),
    model_year          VARCHAR(50),
    chassis_no          VARCHAR(100),
    use_of_vehicle      VARCHAR(100),
    product             VARCHAR(100),
    sum_insured         VARCHAR(50),
    premium             VARCHAR(50),
    deductable          VARCHAR(50)
);

BULK INSERT staging.policy_raw
FROM '/var/opt/mssql/datasets/policies.csv' 
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);


INSERT INTO staging.policy (
    policy_no,
    customer_id,
    coverage_type,
    policy_start_date,
    policy_end_date,
    make,
    model,
    model_year,
    chassis_no,
    use_of_vehicle,
    product,
    sum_insured,
    premium,
    deductable,
    ingest_batch_id,
    ingest_file_name,
    ingest_source
)
SELECT
    policy_no,
    customer_id,
    coverage_type,
    policy_start_date,
    policy_end_date,
    make,
    model,
    model_year,
    chassis_no,
    use_of_vehicle,
    product,
    sum_insured,
    premium,
    deductable,
    'BATCH_2025_01',
    'policy.csv',
    'LOCAL_FS'
FROM staging.policy_raw;

-- 2. claim table

DROP TABLE staging.claim_raw

CREATE TABLE staging.claim_raw (
    claim_no                        VARCHAR(255), -- dirty, NOT a PK
    policy_no                       VARCHAR(255),
    claim_date                      VARCHAR(255),
    months_as_customer              VARCHAR(255),
    injury                          VARCHAR(255),
    property                        VARCHAR(255),
    vehicle                         VARCHAR(255),
    total                           VARCHAR(255),
    collision_type                  VARCHAR(255),
    number_of_vehicles_involved     VARCHAR(255),
    driver_age                      VARCHAR(255),
    insured_relationship            VARCHAR(255),
    license_issue_date              VARCHAR(255),
    incident_date                   VARCHAR(255),
    incident_hour                   VARCHAR(255),
    incident_type                   VARCHAR(255),
    incident_severity               VARCHAR(255),
    number_of_witnesses             VARCHAR(255),
    suspicious_activity             VARCHAR(255)
);


BULK INSERT staging.claim_raw
FROM '/var/opt/mssql/datasets/claims.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);


INSERT INTO staging.claim (
    claim_no                    ,  
    policy_no                   ,
    claim_date                  ,
    months_as_customer          ,
    injury                      ,
    property                    ,
    vehicle                     ,
    total                       ,
    collision_type              ,
    number_of_vehicles_involved ,
    driver_age                  ,
    insured_relationship        ,
    license_issue_date          ,
    incident_date               ,
    incident_hour               ,
    incident_type               ,
    incident_severity           ,
    number_of_witnesses         ,
    suspicious_activity         ,
    ingest_batch_id,
    ingest_file_name,
    ingest_source
)
SELECT
    claim_no                    , 
    policy_no                   ,
    claim_date                  ,
    months_as_customer          ,
    injury                      ,
    property                    ,
    vehicle                     ,
    total                       ,
    collision_type              ,
    number_of_vehicles_involved ,
    driver_age                  ,
    insured_relationship        ,
    license_issue_date          ,
    incident_date               ,
    incident_hour               ,
    incident_type               ,
    incident_severity           ,
    number_of_witnesses         ,
    suspicious_activity ,
    'BATCH_2025_01',
    'claims.csv',
    'LOCAL_FS'

FROM staging.claim_raw ;

--- 3. Customer table

CREATE TABLE staging.customer_raw (
    customer_id VARCHAR(255), -- dirty, NOT a PK
    date_of_birth VARCHAR(255),
    borough VARCHAR(255),
    neighborhood VARCHAR(255),
    zip_code VARCHAR(255),
    name VARCHAR(255),
)


-- 3. POPULATE customer TABLE 
BULK INSERT staging.customer_raw
FROM '/var/opt/mssql/datasets/customers.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);


INSERT INTO staging.customer(
    customer_id ,
    date_of_birth ,
    borough ,
    neighborhood ,
    zip_code ,
    name ,
    ingest_batch_id ,
    ingest_file_name ,
    ingest_source   
)
SELECT
    customer_id ,
    date_of_birth ,
    borough ,
    neighborhood ,
    zip_code ,
    name ,
    'BATCH_2025_01',
    'customers.csv',
    'LOCAL_FS'
FROM staging.customer_raw