FROM apache/superset:4.1.1

USER root

# Postgres driver for SQLAlchemy
RUN pip install --no-cache-dir psycopg2-binary

# Trino driver (SQLAlchemy dialect) so Superset can connect to Trino
RUN pip install --no-cache-dir sqlalchemy-trino

# For Google Sheets:
RUN pip install --no-cache-dir "shillelagh[gdrive]"

USER superset
