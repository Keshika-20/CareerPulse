# CareerPulse authentication and Big Data operations

## Authentication and authorization

CareerPulse uses Streamlit's built-in OpenID Connect (OIDC) authentication. It does not store plaintext passwords and does not provide a hard-coded demo login. Until an identity provider is configured, the workspace stays locked.

1. Create a Google OAuth client (or configure another OIDC provider). Set its authorized redirect URI to the local Streamlit URL, usually `http://localhost:8501/oauth2callback` for local development. Use HTTPS and the correct callback URL when deployed.
2. Create `.streamlit/secrets.toml` in the repository root (do not commit it). Example for Google:

```toml
[auth]
redirect_uri = "http://localhost:8501/oauth2callback"
cookie_secret = "REPLACE_WITH_A_LONG_RANDOM_SECRET"
client_id = "REPLACE_WITH_OIDC_CLIENT_ID"
client_secret = "REPLACE_WITH_OIDC_CLIENT_SECRET"
server_metadata_url = "https://accounts.google.com/.well-known/openid-configuration"

[roles]
admin_emails = ["your-admin-email@example.com"]
analyst_emails = ["your-analyst-email@example.com"]
viewer_emails = ["your-viewer-email@example.com"]
```

3. Generate a random cookie secret; never reuse your OAuth client secret as the cookie secret. Keep `.streamlit/secrets.toml` out of Git and configure equivalent secrets in your deployment environment.
4. Restart with `python -m streamlit run .\src\dashboard\app.py`. Users sign in through the configured identity provider and log out using the sidebar.

Role policy:
- `admin`: full workspace access, can execute ingestion and Spark batch runs.
- `analyst`: can inspect pipeline run history and events but cannot execute pipeline jobs.
- `viewer`: read-only overview, job explorer, Big Data status, and event views.
- Unknown authenticated emails default to `viewer`. Configure explicit allowlists; do not infer roles from user-supplied form values.

OIDC authenticates the user; role allowlists authorize workspace capabilities. For a public production deployment, also configure HTTPS, secret management, provider-side MFA, and appropriate database/file-system access. Streamlit's single-process/local database design is a student-project deployment, not a substitute for a multi-tenant backend service.

## Real public job sources

Copy `config/ingestion.multi-source.example.json` to a local ignored file, then enable only source accounts whose public board URLs you have verified. Greenhouse and Lever expose company-specific public job boards; neither endpoint is a global job-search API. Board identifiers are provider-specific and may change. Verify each board in a browser before enabling it:

- Greenhouse API: `https://boards-api.greenhouse.io/v1/boards/<board-token>/jobs?content=true`
- Lever global API: `https://api.lever.co/v0/postings/<site>?skip=0&limit=100`
- Lever EU API: `https://api.eu.lever.co/v0/postings/<site>?skip=0&limit=100`

Enable additional entries one at a time. A failed source is reported; successful accounts can still commit independently. Do not put the entire multi-source config into version control if it contains local account choices. Do not scrape LinkedIn/Indeed or bypass access controls. Only published jobs are ingested; missing salary/experience remains unknown instead of being invented.

## Big Data architecture: implemented vs next stage

```text
Public company job boards (Greenhouse / Lever)
    -> HTTP retries + response/schema/completeness checks
    -> normalized job schema + namespaced IDs
    -> SQLite transactional snapshots + lifecycle event log
    -> Streamlit job explorer / operational metrics
    -> optional Spark DataFrame batch aggregation

Next integration stage (not currently end-to-end):
Normalized lifecycle events -> Kafka topic -> consumer / Spark Structured Streaming
    -> partitioned Parquet / analytical tables -> trend and pipeline dashboards
```

Implemented now: multi-account public-board adapters, validation, normalization, SQLite snapshot/lifecycle storage, idempotent unchanged-job handling, closure evidence, dashboard queries, a separate Kafka producer utility, a separate Parquet writer, and optional Spark batch aggregation.

Not yet end-to-end: the public ingestion service does not publish its normalized lifecycle events to Kafka; no continuous Kafka consumer or Spark Structured Streaming query is wired into this ingestion flow; ingestion does not automatically maintain partitioned Parquet history. The Big Data page labels these boundaries accurately. These are the next engineering milestones, not capabilities to claim as complete.

## Run commands

```powershell
python -m src.ingestion.service --config .\config\ingestion.local.json
python -m src.ingestion.service --config .\config\ingestion.local.json --spark-batch
python -m streamlit run .\src\dashboard\app.py
```

Spark batch needs compatible Java/PySpark installed and configured. The normal ingestion command does not start Spark.
