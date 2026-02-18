# Oracle ESW → Silver topics + Iceberg columns (Approval)

This document defines the proposed *silver* topics and record fields (which become Iceberg table columns via the Iceberg Sink connector).

**Important safety:** the Streams app `et.gov.lakehouse.govaggregator.core.DebeziumToSilverApp` will not emit any `silver.oracle_esw.*` topics unless `SILVER_APPROVED=true`.

## Inputs (raw / bronze)
Produced by Debezium Oracle connector with RegexRouter:
- `oracle_esw.ESW.<TABLE>` → `raw-<TABLE>`

Captured tables (from connector privileges + checked-in schemas):
- `TB_CB_LPCO`
- `TB_CB_LPCO_AMDT_ATTCH_DOC`
- `TB_CB_LPCO_ATTCH_DOC`
- `TB_CB_LPCO_CMDT`
- `TB_CB_LPCO_CMNT`
- `TB_CB_LPCO_CNCL_ATTCH_DOC`
- `TB_CB_LPCO_CSTMS`
- `TB_CB_LPCO_MPNG`

## Outputs (silver topics)
Topic naming proposal:
- Input `raw-<TABLE>` → output `silver.oracle_esw.<TABLE>`

Record naming proposal (Avro):
- Namespace: `silver.oracle_esw`
- Name: `<TABLE>`

All output records include a routing field:
- `__iceberg_table` (STRING): the target Iceberg table identifier (example: `silver.tb_cb_lpco`)

And then a 1:1 projection of the Debezium `after` record fields, **lowercased** (example: `LPCO_NO` → `lpco_no`).

## Iceberg sink routing
The sink connector will be configured with:
- `iceberg.tables.dynamic-enabled=true`
- `iceberg.tables.route-field=__iceberg_table`

So each record decides which Iceberg table receives it.

## Per-table silver mapping (proposed)

### TB_CB_LPCO
- bronze topic: `raw-TB_CB_LPCO`
- silver topic: `silver.oracle_esw.TB_CB_LPCO`
- iceberg table: `silver.tb_cb_lpco`
- columns (32):
	- `__iceberg_table`
	- `lpco_no`
	- `lpco_sn`
	- `apfm_refno`
	- `apfm_refno_sn`
	- `lpco_id`
	- `lpco_cd`
	- `rst_tp_cd`
	- `lpco_issu_dt`
	- `lpco_exdt`
	- `isagc_cd`
	- `orginl_prt_cnt`
	- `copy_prt_cnt`
	- `cdrels_rqst_dt`
	- `cdrels_rqst_yn`
	- `cdrels_aprvl_yn`
	- `cdrels_perm_issu_dt`
	- `cdrels_perm_no`
	- `del_yn`
	- `frst_regst_id`
	- `frst_rgsr_dttm`
	- `last_modfr_id`
	- `last_mod_dttm`
	- `amdt_rsn_tp_cd`
	- `amdt_rsn_cn`
	- `amdt_dt`
	- `lpco_amdt_rst_cd`
	- `cncl_rsn_tp_cd`
	- `cncl_rsn_cn`
	- `cncl_dt`
	- `lpco_cncl_rst_cd`
	- `use_yn`

### TB_CB_LPCO_AMDT_ATTCH_DOC
- bronze topic: `raw-TB_CB_LPCO_AMDT_ATTCH_DOC`
- silver topic: `silver.oracle_esw.TB_CB_LPCO_AMDT_ATTCH_DOC`
- iceberg table: `silver.tb_cb_lpco_amdt_attch_doc`
- columns (12):
	- `__iceberg_table`
	- `lpco_amdt_attch_doc_sn`
	- `apfm_refno`
	- `lpco_no`
	- `lpco_sn`
	- `attch_file_id`
	- `lpco_amdt_rst_cd`
	- `del_yn`
	- `frst_regst_id`
	- `frst_rgsr_dttm`
	- `last_modfr_id`
	- `last_mod_dttm`

### TB_CB_LPCO_ATTCH_DOC
- bronze topic: `raw-TB_CB_LPCO_ATTCH_DOC`
- silver topic: `silver.oracle_esw.TB_CB_LPCO_ATTCH_DOC`
- iceberg table: `silver.tb_cb_lpco_attch_doc`
- columns (11):
	- `__iceberg_table`
	- `lpco_attch_doc_id`
	- `attch_doc_id`
	- `lpco_mpng_id`
	- `attch_doc_rqre_yn`
	- `use_yn`
	- `del_yn`
	- `frst_regst_id`
	- `frst_rgsr_dttm`
	- `last_modfr_id`
	- `last_mod_dttm`

### TB_CB_LPCO_CMDT
- bronze topic: `raw-TB_CB_LPCO_CMDT`
- silver topic: `silver.oracle_esw.TB_CB_LPCO_CMDT`
- iceberg table: `silver.tb_cb_lpco_cmdt`
- columns (18):
	- `__iceberg_table`
	- `apfm_refno`
	- `apfm_refno_sn`
	- `lpco_cmdt_sn`
	- `cmdt_sn`
	- `lpco_no`
	- `lpco_sn`
	- `rst_tp_cd`
	- `um_tp_cd`
	- `apv_unit_val`
	- `rjct_unit_val`
	- `rtent_unit_val`
	- `del_yn`
	- `frst_regst_id`
	- `frst_rgsr_dttm`
	- `last_modfr_id`
	- `last_mod_dttm`
	- `use_yn`

### TB_CB_LPCO_CMNT
- bronze topic: `raw-TB_CB_LPCO_CMNT`
- silver topic: `silver.oracle_esw.TB_CB_LPCO_CMNT`
- iceberg table: `silver.tb_cb_lpco_cmnt`
- columns (11):
	- `__iceberg_table`
	- `apfm_refno`
	- `apfm_refno_sn`
	- `cmnt_sn`
	- `cmnt_tp_cd`
	- `adstt_cn`
	- `del_yn`
	- `frst_regst_id`
	- `frst_rgsr_dttm`
	- `last_modfr_id`
	- `last_mod_dttm`

### TB_CB_LPCO_CNCL_ATTCH_DOC
- bronze topic: `raw-TB_CB_LPCO_CNCL_ATTCH_DOC`
- silver topic: `silver.oracle_esw.TB_CB_LPCO_CNCL_ATTCH_DOC`
- iceberg table: `silver.tb_cb_lpco_cncl_attch_doc`
- columns (12):
	- `__iceberg_table`
	- `lpco_cncl_attch_doc_sn`
	- `apfm_refno`
	- `lpco_no`
	- `lpco_sn`
	- `attch_file_id`
	- `lpco_cncl_rst_cd`
	- `del_yn`
	- `frst_regst_id`
	- `frst_rgsr_dttm`
	- `last_modfr_id`
	- `last_mod_dttm`

### TB_CB_LPCO_CSTMS
- bronze topic: `raw-TB_CB_LPCO_CSTMS`
- silver topic: `silver.oracle_esw.TB_CB_LPCO_CSTMS`
- iceberg table: `silver.tb_cb_lpco_cstms`
- columns (12):
	- `__iceberg_table`
	- `lpco_sn`
	- `lpco_no`
	- `lpco_id`
	- `used_yn`
	- `cstms_modfr_nm`
	- `cstms_mod_dttm`
	- `del_yn`
	- `frst_regst_id`
	- `frst_rgsr_dttm`
	- `last_modfr_id`
	- `last_mod_dttm`

### TB_CB_LPCO_MPNG
- bronze topic: `raw-TB_CB_LPCO_MPNG`
- silver topic: `silver.oracle_esw.TB_CB_LPCO_MPNG`
- iceberg table: `silver.tb_cb_lpco_mpng`
- columns (26):
	- `__iceberg_table`
	- `lpco_mpng_id`
	- `cbra_cd`
	- `apfm_cd`
	- `lpco_cd`
	- `cmdt_hs_grp_id`
	- `cmdt_hs_cd`
	- `cmdt_spcfn_cd`
	- `cd_grp_id`
	- `cd_val`
	- `lgsl_cn`
	- `apfm_url`
	- `vald_strt_dt`
	- `vald_end_dt`
	- `lpco_url`
	- `vald_prid_dd`
	- `use_yn`
	- `del_yn`
	- `frst_regst_id`
	- `frst_rgsr_dttm`
	- `last_modfr_id`
	- `last_mod_dttm`
	- `list_view_yn`
	- `mpng_tp_cd`
	- `sort_ord`
	- `rmk`

## Approval checklist
Please confirm you agree with:
1. Silver topic names: `silver.oracle_esw.<TABLE>`
2. Silver record field naming: all `after` columns lowercased
3. The extra routing column `__iceberg_table`

Once approved, we’ll set `SILVER_APPROVED=true` on the Streams container and start the sink connector.
