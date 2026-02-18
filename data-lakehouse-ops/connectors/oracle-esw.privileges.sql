-- Oracle privileges needed for Debezium Oracle (LogMiner) connector in ESWTEST PDB
--
-- Run these commands in the ESWTEST PDB (ALTER SESSION SET CONTAINER = ESWTEST;).
-- Use the least-privilege option that works for your environment.
--
-- Multitenant note (Oracle Free / CDB+PDB):
-- - Debezium LogMiner must connect to the CDB service (e.g. service name 'free') and set:
--     database.dbname=FREE
--     database.pdb.name=ESWTEST
-- - If the connector connects directly to the PDB service (e.g. 'eswtest'), LogMiner can fail with:
--     ORA-65040: Operation is not allowed from within a pluggable database
--
-- Current observed errors (from Kafka Connect task logs):
--   - After applying the include/exclude list filters, no changes will be captured.
--     (Debezium user cannot see the target tables in ALL_TABLES/metadata)
--   - ORA-41900: missing FLASHBACK privilege on "ESW"."<TABLE>"
--     (snapshot uses "AS OF SCN" queries)
--   - ORA-01031: insufficient privileges
--     SQL: CREATE TABLE "LOG_MINING_FLUSH" (LAST_SCN NUMBER(19,0))
--   - Supplemental logging not properly configured. Use: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA
--     (LogMiner streaming requires supplemental logging; see oracle-esw.supplemental-logging.sql)
--
-- Debezium uses LOG_MINING_FLUSH to force LGWR flush for LogMiner.

-- Set the Debezium username once and reuse it throughout this script.
-- Example values:
--   - C##DEBEZIUM  (common user for CDB/PDB)
--   - DEBEZIUM     (local user inside the PDB)
--   - ESW          (not recommended; usually the application schema)
-- Default matches the current connector config (database.user=esw).
DEFINE DEBEZIUM_USER = ESW_READONLY

-- Sanity checks (run before/after grants)
-- Confirm you are in the ESWTEST PDB:
SELECT sys_context('USERENV','CON_NAME') AS con_name FROM dual;
-- Confirm the Debezium user and its default tablespace:
SELECT username, default_tablespace, temporary_tablespace
FROM dba_users
WHERE username IN (UPPER('&&DEBEZIUM_USER'));

-- Core Debezium connector privileges (LogMiner)
-- Run as SYS (or another DBA) in ESWTEST PDB.
GRANT CREATE SESSION TO &&DEBEZIUM_USER;

-- LogMiner packages
GRANT EXECUTE ON DBMS_LOGMNR TO &&DEBEZIUM_USER;
GRANT EXECUTE ON DBMS_LOGMNR_D TO &&DEBEZIUM_USER;

-- Dynamic performance views used by Debezium during mining and snapshotting.
-- Prefer granting on the underlying V_$ views.
GRANT SELECT ON V_$DATABASE TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$INSTANCE TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$THREAD TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$PARAMETER TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$NLS_PARAMETERS TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$LOG TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$LOGFILE TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$ARCHIVED_LOG TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$ARCHIVE_DEST TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$TRANSACTION TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$MYSTAT TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$SESSION TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$PROCESS TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$LOGMNR_LOGS TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO &&DEBEZIUM_USER;
GRANT SELECT ON V_$LOGMNR_STATS TO &&DEBEZIUM_USER;

-- DBMS_METADATA is used to fetch CREATE TABLE DDL during schema snapshots.
-- Without this, Debezium can fail with ORA-31603 (object not found) even when the table exists.
GRANT EXECUTE ON DBMS_METADATA TO &&DEBEZIUM_USER;
GRANT SELECT_CATALOG_ROLE TO &&DEBEZIUM_USER;

-- Access to the 8 captured tables (required so Debezium can discover them, and so snapshots work)
-- Run as a privileged user in ESWTEST PDB:
GRANT SELECT ON ESW.TB_CB_LPCO TO &&DEBEZIUM_USER;
GRANT SELECT ON ESW.TB_CB_LPCO_AMDT_ATTCH_DOC TO &&DEBEZIUM_USER;
GRANT SELECT ON ESW.TB_CB_LPCO_ATTCH_DOC TO &&DEBEZIUM_USER;
GRANT SELECT ON ESW.TB_CB_LPCO_CMDT TO &&DEBEZIUM_USER;
GRANT SELECT ON ESW.TB_CB_LPCO_CMNT TO &&DEBEZIUM_USER;
GRANT SELECT ON ESW.TB_CB_LPCO_CNCL_ATTCH_DOC TO &&DEBEZIUM_USER;
GRANT SELECT ON ESW.TB_CB_LPCO_CSTMS TO &&DEBEZIUM_USER;
GRANT SELECT ON ESW.TB_CB_LPCO_MPNG TO &&DEBEZIUM_USER;

GRANT FLASHBACK ON ESW.TB_CB_LPCO TO &&DEBEZIUM_USER;
GRANT FLASHBACK ON ESW.TB_CB_LPCO_AMDT_ATTCH_DOC TO &&DEBEZIUM_USER;
GRANT FLASHBACK ON ESW.TB_CB_LPCO_ATTCH_DOC TO &&DEBEZIUM_USER;
GRANT FLASHBACK ON ESW.TB_CB_LPCO_CMDT TO &&DEBEZIUM_USER;
GRANT FLASHBACK ON ESW.TB_CB_LPCO_CMNT TO &&DEBEZIUM_USER;
GRANT FLASHBACK ON ESW.TB_CB_LPCO_CNCL_ATTCH_DOC TO &&DEBEZIUM_USER;
GRANT FLASHBACK ON ESW.TB_CB_LPCO_CSTMS TO &&DEBEZIUM_USER;
GRANT FLASHBACK ON ESW.TB_CB_LPCO_MPNG TO &&DEBEZIUM_USER;

-- Option A (recommended / simple): allow Debezium user to create its flush table
-- and store it in the USERS tablespace.
GRANT CREATE TABLE TO &&DEBEZIUM_USER;
ALTER USER &&DEBEZIUM_USER QUOTA 100M ON USERS;

-- If you don't know the tablespace, you can grant a broader quota:
-- ALTER USER DEBEZIUM QUOTA UNLIMITED ON USERS;
-- or as a last resort:
-- GRANT UNLIMITED TABLESPACE TO DEBEZIUM;

-- Option B (tighter on privileges): pre-create the flush table owned by DEBEZIUM
-- using a DBA account (e.g., SYS) while connected to the ESWTEST PDB.
--
-- This avoids granting CREATE TABLE to DEBEZIUM, but still requires that the
-- DEBEZIUM user has quota on the tablespace where the table is created.
--
-- Example (as SYS/DBA in ESWTEST PDB):
--   ALTER USER &&DEBEZIUM_USER QUOTA 100M ON USERS;
--   CREATE TABLE &&DEBEZIUM_USER.LOG_MINING_FLUSH (LAST_SCN NUMBER(19,0));
--
-- Debezium should then detect the table exists and skip creating it.

-- Optional: snapshot locking (only needed for snapshot.locking.mode=shared/custom)
-- GRANT LOCK ANY TABLE TO &&DEBEZIUM_USER;
-- If you later disable snapshot locking with snapshot.locking.mode=none, you may not need it.
