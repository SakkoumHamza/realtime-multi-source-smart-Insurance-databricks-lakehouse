-- RESTORE DATABASE claims
-- FROM DISK = './sql_server/claims_dev.bak'
-- WITH REPLACE,
--      RECOVERY;

SELECT * FROM claims.staging.[policy];
