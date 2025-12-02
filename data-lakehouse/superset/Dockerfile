FROM apache/superset:4.1.1

USER root

# Postgres driver for SQLAlchemy
RUN pip install --no-cache-dir psycopg2-binary

# For Google Sheets:
RUN pip install --no-cache-dir "shillelagh[gdrive]"

USER superset
