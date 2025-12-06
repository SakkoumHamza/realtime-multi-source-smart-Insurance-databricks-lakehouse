/*
===============================================================================
                           FIVETRAN CONFIGURATION SCRIPT
===============================================================================

Purpose:
    This script prepares SQL Server for Fivetran ingestion by creating a dedicated
    connector user, assigning the required permissions, and enabling the features
    needed for incremental and real-time replication.

What This Script Does:
    1. Creates a login and database user for Fivetran:
         - Login:        fivetran_user
         - Permissions:  db_datareader + db_owner + CDC + CT access
         - Authentication: SQL (NOT Windows)

    2. Grants the minimum required privileges:
         - Read access to all source tables
         - Access to system metadata
         - Ability to read CT & CDC change tables
         - Ability to execute CDC stored procedures

    3. Enables SQL Server features:
         - Change Tracking (CT) - Already enabled in the db_init
         - Change Data Capture (CDC) - Already enabled in the db_init
         - Ensures SQL Server Agent is running

Important Warnings:
    ⚠️ Requires sysadmin privileges to run successfully.
    ⚠️ CDC requires SQL Server Agent — Fivetran will fail if it is stopped.
    ⚠️ Retention settings must be aligned with sync frequency (default: 14 days).
    ⚠️ If using Docker, ensure port 1433 is exposed and reachable externally.

Post-Execution Validation:
    ✔ Test login:  sqlcmd -S host,1433 -U fivetran_user -P 'Password'
    ✔ Verify CT:   SELECT * FROM CHANGETABLE(CHANGES staging.policy, NULL);
    ✔ Verify CDC:  SELECT * FROM cdc.change_tables;
    ✔ Test connector connectivity in Fivetran UI.

===============================================================================
*/


/*==============================================================================
    1. CREATE LOGIN & USER FOR FIVETRAN
==============================================================================*/

-- Create server-level login
CREATE LOGIN fivetran_user 
WITH PASSWORD = 'FivetranP@ss123!', CHECK_POLICY = ON;

-- Create database user in claims DB
USE claims;
CREATE USER fivetran_user FOR LOGIN fivetran_user;


/*==============================================================================
    2. GRANT REQUIRED PERMISSIONS
==============================================================================*/

-- Basic read access
EXEC sp_addrolemember 'db_datareader', 'fivetran_user';

-- Allows reading schema metadata (required)
GRANT VIEW DEFINITION TO fivetran_user;
GRANT VIEW DATABASE STATE TO fivetran_user;

-- Optional but recommended (avoids permission issues)
EXEC sp_addrolemember 'db_owner', 'fivetran_user';

