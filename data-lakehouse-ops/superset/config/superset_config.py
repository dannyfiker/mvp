# --- DB (metadata in Postgres) ---
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://superset:superset@superset-db:5432/superset"

# --- Allow full HTML/JS in Markdown components ---
# This is the critical flag for Superset 4.1.1
HTML_SANITIZATION = False  # disable GitHub-style sanitization for Markdown, etc.

# Older / extra flags (harmless but keep them)
ENABLE_JAVASCRIPT_CONTROLS = True
ALLOW_JAVASCRIPT_IN_MARKDOWN = True
FEATURE_FLAGS = {
    "ALLOW_JAVASCRIPT_XSS": True,
}
