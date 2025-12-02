/*
=============================================================
Create Database, Schemas, Tables & Enable Change Data Capture
=============================================================
Script Purpose:
    This script creates a new database named 'claims_dev' and sets up the complete
    infrastructure for real-time data capture and streaming integration.
    
    The script performs the following operations:
    1. Creates database 'claims_dev' and schema 'demo'
    2. Creates three main tables: policy, claim, and customer
    3. Enables Change Tracking on database and all tables for incremental data capture
    4. Enables Change Data Capture (CDC) on database and tables for real-time streaming
    5. Prepares infrastructure for DDL capture and schema evolution support
    
    Tables Created:
    - demo.policy: Insurance policy information with vehicle and coverage details
    - demo.claim: Insurance claims with incident details and damage amounts  
    - demo.customer: Customer demographic and location information
    
    CDC Features Enabled:
    - Change Tracking: 14-day retention for incremental batch processing
    - Change Data Capture: Real-time change streaming for Databricks integration
    - Net changes support: Optimized change detection for data pipelines
*/



CREATE DATABASE claims_dev
USE claims_dev
CREATE SCHEMA demo

CREATE TABLE demo.policy (
    policy_no           VARCHAR(50)    NOT NULL PRIMARY KEY,
    cust_id             VARCHAR(50)    NOT NULL,        
    policytype          VARCHAR(50),
    pol_issue_date      DATE,
    pol_eff_date        DATE,
    pol_expiry_date     DATE,
    make                VARCHAR(50),
    model               VARCHAR(50),
    model_year          INT,
    chassis_no          VARCHAR(50),
    use_of_vehicle      VARCHAR(100),
    product             VARCHAR(100),
    sum_insured         FLOAT,
    premium             FLOAT,
    deductable          INT
)

CREATE TABLE demo.claim (
    claim_no                        VARCHAR(50)    NOT NULL PRIMARY KEY,
    policy_no                       VARCHAR(50)    NOT NULL,   
    claim_date                      VARCHAR(20),
    months_as_customer              INT,
    injury                          BIGINT,
    property                        BIGINT,
    vehicle                         BIGINT,
    total                           BIGINT,
    collision_type                  VARCHAR(50),
    number_of_vehicles_involved     INT,
    driver_age                      FLOAT,
    insured_relationship            VARCHAR(50),
    license_issue_date              VARCHAR(20),
    incident_date                   VARCHAR(20),
    incident_hour                   INT,
    incident_type                   VARCHAR(50),
    incident_severity               VARCHAR(50),
    number_of_witnesses             INT,
    suspicious_activity             BIT
)

CREATE TABLE demo.customer (
    customer_id INT NOT NULL PRIMARY KEY,
    date_of_birth VARCHAR(100) NULL,
    borough VARCHAR(100) NULL,
    neighborhood VARCHAR(150) NULL,
    zip_code VARCHAR(10) NULL,
    name VARCHAR(255) NULL
)



-- 1. ENABLE CHANGE TRACKING ON DB --
ALTER DATABASE claims_dev SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON)

-- 2. ENABLE CHANGE TRACKING ON TABLES --
ALTER TABLE demo.policy ENABLE CHANGE_TRACKING
ALTER TABLE demo.claim ENABLE CHANGE_TRACKING
ALTER TABLE demo.customer ENABLE CHANGE_TRACKING

-- 3. ENABLE CDC on DB --
EXEC msdb.dbo.rds_cdc_enable_db 'claims_dev'

-- 4. ENABLE CDC ON TABLES --
EXEC sys.sp_cdc_enable_table
@source_schema = N'demo',
@source_name   = N'policy',
@role_name     = NULL,
@supports_net_changes = 1


EXEC sys.sp_cdc_enable_table
@source_schema = N'demo',
@source_name   = N'claim',
@role_name     = NULL,
@supports_net_changes = 1


EXEC sys.sp_cdc_enable_table
@source_schema = N'demo',
@source_name   = N'customer',
@role_name     = NULL,
@supports_net_changes = 1

-- 4. Set up DDL capture and schema evolution -
--> Run Script -> ddl_support_objects.sql and change the fields to your needs
