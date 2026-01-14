# Avro Schemas (Apicurio)

This folder is the repo-managed mirror of schemas stored in Apicurio Registry.

## Layout

Schemas are exported per Apicurio group:

- `data-lakehouse/avro/<group>/.../*.avsc`

For Debezium topics produced by this MVP (value-only Avro), schemas are stored as:

- `data-lakehouse/avro/pg1/<schema>/<table>.avsc`

## Export from Apicurio (recommended)

Exports all **value** artifacts in a group into `.avsc` files:

- `python3 data-lakehouse/scripts/registry/export_apicurio_schemas.py --group pg1`

To export key schemas too (not recommended for this MVP), use:

- `python3 data-lakehouse/scripts/registry/export_apicurio_schemas.py --group pg1 --include-keys`

Apicurio base URL can be overridden with:

- `APICURIO_URL=http://localhost:8081/apis/registry/v2 ...`

## Import/seed into Apicurio

Seeds (or updates) Apicurio from the checked-in schema files:

- `python3 data-lakehouse/scripts/registry/import_apicurio_schemas.py --group pg1`

This is safe to rerun: if an artifact already exists, the script adds a new version.
