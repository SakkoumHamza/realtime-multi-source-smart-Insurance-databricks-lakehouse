USE claims_dev;


-- 1. POPULATE POLICY TABLE FROM S3 CSV
BULK INSERT demo.policy
FROM 's3://claims-fsdm-s3/sql_server/policy.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,  -- Skip header row
    ERRORFILE = 's3://claims-fsdm-s3/errors/policy_errors.txt'
);

-- 2. POPULATE CUSTOMER TABLE FROM S3 CSV
BULK INSERT demo.customer
FROM 's3://claims-fsdm-s3/sql_server/customer.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    ERRORFILE = 's3://claims-fsdm-s3/errors/customer_errors.txt'
);

-- 3. POPULATE CLAIM TABLE FROM S3 CSV
BULK INSERT demo.claim
FROM 's3://claims-fsdm-s3/sql_server/claim.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    ERRORFILE = 's3://claims-fsdm-s3/errors/claim_errors.txt'
);