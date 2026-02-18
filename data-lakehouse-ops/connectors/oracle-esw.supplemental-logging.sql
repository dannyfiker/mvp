-- Enable supplemental logging required by Debezium Oracle (LogMiner)
--
-- Symptom in Kafka Connect task trace:
--   "Supplemental logging not properly configured. Use: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA"
--
-- IMPORTANT:
-- - Run step (2) as a privileged user (typically SYS / SYSDBA).
-- - In multitenant setups, connect to the correct database/service.
--   Debezium checks V$DATABASE on the database it connects to.

-- 1) Verify current status
-- Expect SUPPLEMENTAL_LOG_DATA_MIN = 'YES'
SELECT SUPPLEMENTAL_LOG_DATA_MIN,
			 SUPPLEMENTAL_LOG_DATA_PK,
			 SUPPLEMENTAL_LOG_DATA_UI,
			 SUPPLEMENTAL_LOG_DATA_FK,
			 SUPPLEMENTAL_LOG_DATA_ALL
FROM V$DATABASE;

-- 2) Enable minimal supplemental logging (required)
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- 3) Enable table-level supplemental logging for captured tables
--
-- Safer (but larger redo): ALL columns.
-- If you want less redo, and all tables have stable PKs, you can use (PRIMARY KEY) COLUMNS instead.
--
-- Run these while connected to the ESWTEST service/container so they apply to the ESW schema.
ALTER TABLE ESW.TB_CB_LPCO ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE ESW.TB_CB_LPCO_AMDT_ATTCH_DOC ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE ESW.TB_CB_LPCO_ATTCH_DOC ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE ESW.TB_CB_LPCO_CMDT ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE ESW.TB_CB_LPCO_CMNT ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE ESW.TB_CB_LPCO_CNCL_ATTCH_DOC ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE ESW.TB_CB_LPCO_CSTMS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE ESW.TB_CB_LPCO_MPNG ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- 4) Verify table-level log groups exist
-- Requires DBA_LOG_GROUPS access (run as SYS/DBA) or grant SELECT on DBA_LOG_GROUPS.
SELECT OWNER, TABLE_NAME, LOG_GROUP_NAME, LOG_GROUP_TYPE, ALWAYS
FROM DBA_LOG_GROUPS
WHERE OWNER = 'ESW'
	AND TABLE_NAME IN (
		'TB_CB_LPCO',
		'TB_CB_LPCO_AMDT_ATTCH_DOC',
		'TB_CB_LPCO_ATTCH_DOC',
		'TB_CB_LPCO_CMDT',
		'TB_CB_LPCO_CMNT',
		'TB_CB_LPCO_CNCL_ATTCH_DOC',
		'TB_CB_LPCO_CSTMS',
		'TB_CB_LPCO_MPNG'
	)
ORDER BY TABLE_NAME, LOG_GROUP_NAME;