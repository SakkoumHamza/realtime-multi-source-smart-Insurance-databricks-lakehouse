/*
======================================================================
Script:  CLAIMS DATABASE SETUP – FULL INGEST + CDC + CHANGE TRACKING
AUTHOR : Sakkoum Hamza
DATE   : 2025-12
======================================================================

Purpose:
    This script creates and configures the complete SQL Server
    infrastructure required for ingestion, staging, and real-time
    data capture for the claims domain.

What This Script Does:
    1. Drops and recreates the `claims` database
    2. Creates `staging` schema
    3. Creates staging tables: policy, claim, customer
    4. Enables Change Tracking (CT) for incremental processing
    5. Enables Change Data Capture (CDC) for real-time streaming
    6. Prepares environment for schema evolution (DDL capture)

Warnings:
    - ⚠️ DROPS the database `claims` — all existing data will be lost.
    - Ensure SQL Server Agent is running before enabling CDC.
    - Requires elevated permissions (ALTER DATABASE, CONTROL, sysadmin).
    - CT retention is configured for 14 days (adjust as needed).

Prerequisites:
    - SQL Server Agent must be enabled.
    - Appropriate permissions for CT and CDC activation.

Next Steps After Execution:
    - Bulk load raw CSV data into staging tables.
    - Run ddl_support_objects.sql for schema-evolution support.
    - Verify CDC using: SELECT * FROM cdc.change_tables;

======================================================================
*/


DROP DATABASE claims

CREATE DATABASE claims

USE claims

Create SCHEMA staging

-- 1. Staging Table for Policy
-- All columns converted to VARCHAR for flexible data ingestion

CREATE TABLE staging.policy (
    policy_sk BIGINT IDENTITY(1,1) PRIMARY KEY,  -- Unified PK (1st column)
    policy_no           VARCHAR(100),
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
    deductable          VARCHAR(50),

    ingest_batch_id     VARCHAR(50),
    ingest_file_name    VARCHAR(255),
    ingest_source       VARCHAR(50),
    ingest_timestamp    DATETIME2 DEFAULT SYSDATETIME()
);

-- Staging Table for Claims (claims.csv)
CREATE TABLE staging.claim(
    claim_sk                        BIGINT IDENTITY(1,1) PRIMARY KEY,  -- Unified PK
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
    suspicious_activity             VARCHAR(255),


    ingest_batch_id     VARCHAR(50),
    ingest_file_name    VARCHAR(255),
    ingest_source       VARCHAR(50),
    ingest_timestamp    DATETIME2 DEFAULT SYSDATETIME()
);

-- Staging Table for Customers (customers.csv)
CREATE TABLE staging.customer (
    customer_sk  BIGINT IDENTITY(1,1) PRIMARY KEY,  -- Unified PK
    customer_id VARCHAR(255), -- dirty, NOT a PK
    date_of_birth VARCHAR(255),
    borough VARCHAR(255),
    neighborhood VARCHAR(255),
    zip_code VARCHAR(255),
    name VARCHAR(255),

    ingest_batch_id     VARCHAR(50),
    ingest_file_name    VARCHAR(255),
    ingest_source       VARCHAR(50),
    ingest_timestamp    DATETIME2 DEFAULT SYSDATETIME()
);

-- 1. ENABLE CHANGE TRACKING ON DB --
ALTER DATABASE claims SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON)

-- 2. ENABLE CHANGE TRACKING ON TABLES --
ALTER TABLE staging.policy ENABLE CHANGE_TRACKING
ALTER TABLE staging.claim ENABLE CHANGE_TRACKING
ALTER TABLE staging.customer ENABLE CHANGE_TRACKING

-- 3. ENABLE CDC on DB --
EXEC sys.sp_cdc_enable_db

-- If State is stopped , start the sql server agent 
EXEC xp_servicecontrol N'QUERYSTATE', N'SQLServerAgent';

-- 4. ENABLE CDC ON TABLES --
EXEC sys.sp_cdc_enable_table
@source_schema = N'staging',
@source_name   = N'policy',
@role_name     = NULL,
@supports_net_changes = 1


EXEC sys.sp_cdc_enable_table
@source_schema = N'staging',
@source_name   = N'claim',
@role_name     = NULL,
@supports_net_changes = 1


EXEC sys.sp_cdc_enable_table
@source_schema = N'staging',
@source_name   = N'customer',
@role_name     = NULL,
@supports_net_changes = 1

-- Verify the cdc is enabled
SELECT 
*
FROM 
    cdc.change_tables


-- 4. Set up DDL capture and schema evolution -

--> Run Script -> ddl_support_objects.sql and change the fields to your needs
