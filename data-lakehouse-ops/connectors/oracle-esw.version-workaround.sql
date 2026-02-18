-- Debezium Oracle connector workaround for Oracle 23ai banner mismatch
--
-- Problem:
--   Debezium (2.7.x and 3.0.0.Final at least) resolves the Oracle version with:
--     SELECT BANNER_FULL FROM V$VERSION WHERE BANNER_FULL LIKE 'Oracle Database%'
--   and then falls back to:
--     SELECT BANNER FROM V$VERSION WHERE BANNER LIKE 'Oracle Database%'
--
--   Oracle 23ai Free reports a banner like:
--     "Oracle AI Database 26ai Free Release 23.26.0.0.0 ..."
--   which does NOT match LIKE 'Oracle Database%'.
--
-- Approach:
--   Create schema-local views named V$VERSION (and V_$VERSION for safety) in the
--   Debezium user schema. Oracle name resolution prefers schema objects over
--   public synonyms, so Debezium will read these views and successfully parse
--   a banner that starts with "Oracle Database".
--
-- Scope:
--   Only affects the DEBEZIUM user session; does not alter SYS views.
--
-- Run this in the Debezium user schema.
-- For multitenant setups where Debezium connects to the CDB service (e.g. 'free') with database.pdb.name=ESWTEST,
-- create these views in CDB$ROOT (and optionally also in ESWTEST for safety).

-- Debezium expects a banner string that begins with "Oracle Database" and includes
-- a numeric version it can parse. Adjust this to match your environment if desired.
DEFINE DEBEZIUM_ORACLE_BANNER_VERSION = 23.26.0.0.0

-- 1) As SYS (or another privileged user):
--    - If Debezium connects to the CDB service: create the views in CDB$ROOT for the Debezium user.
--    - Optionally also create them in ESWTEST.
--
-- 2) As the Debezium user (e.g. ESW_READONLY):
--    CREATE the views below.

-- If you previously created these views with a non-parseable banner (e.g.
-- "Oracle Database 23c Free Release - Debezium banner"), drop and recreate them.
BEGIN
  EXECUTE IMMEDIATE 'DROP VIEW V_$VERSION';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP VIEW V$VERSION';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

CREATE OR REPLACE VIEW V$VERSION AS
SELECT
  'Oracle Database 23c Free Release &&DEBEZIUM_ORACLE_BANNER_VERSION - Debezium compatibility banner' AS BANNER,
  'Oracle Database 23c Free Release &&DEBEZIUM_ORACLE_BANNER_VERSION - Debezium compatibility banner' AS BANNER_FULL
FROM dual;

-- Some tools query the underlying SYS view name directly; creating this too is harmless.
CREATE OR REPLACE VIEW V_$VERSION AS
SELECT
  'Oracle Database 23c Free Release &&DEBEZIUM_ORACLE_BANNER_VERSION - Debezium compatibility banner' AS BANNER,
  'Oracle Database 23c Free Release &&DEBEZIUM_ORACLE_BANNER_VERSION - Debezium compatibility banner' AS BANNER_FULL
FROM dual;

-- Optional quick verification (as DEBEZIUM):
--   SELECT banner_full FROM v$version WHERE banner_full LIKE 'Oracle Database%';
--   SELECT banner FROM v$version WHERE banner LIKE 'Oracle Database%';
--
-- Rollback:
--   DROP VIEW V_$VERSION;
--   DROP VIEW V$VERSION;
