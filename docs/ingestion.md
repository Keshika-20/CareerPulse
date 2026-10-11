# Public Job-Board Ingestion

CareerPulse's public-source ingestion is a local batch workflow. It is independent of the synthetic generator, Kafka producer, and existing Parquet helper.

## Scope and source limits

- Greenhouse uses the public Job Board API for a configured board token. The adapter requests `jobs?content=true`, validates each posting and requires `meta.total` to equal the returned job count.
- Lever uses the public Postings API for a configured company site and the global or EU host. It follows `skip`/`limit` pages to a short final page, rejects duplicate posting IDs, and requires two complete enumerations to return identical posting data before marking the scan complete.
- These checks make incomplete fetches fail closed, but neither provider documents a transactional point-in-time snapshot across concurrent listing changes. Matching Lever enumerations are a consistency check, not a provider guarantee. The two-successful-absence rule further reduces false closures but cannot prove an external API never omitted the same posting twice.
- These APIs are company-board endpoints, not global job search. Configure only public company accounts whose usage and retention terms you have checked. CareerPulse does not access LinkedIn or Indeed.

## Normalized listing

The normalized record retains the following fields:

```json
{
  "job_id": "lever:global:acme:posting-id",
  "source": "lever",
  "source_account_id": "global:acme",
  "source_job_id": "posting-id",
  "source_url": "https://jobs.lever.co/acme/posting-id",
  "source_updated_at": null,
  "company": "Acme",
  "title": "Platform Engineer",
  "location": "Remote",
  "experience": null,
  "salary": null,
  "salary_min": 90000,
  "salary_max": 130000,
  "salary_currency": "USD",
  "salary_interval": "year",
  "timestamp": "2026-01-02T00:00:00+00:00",
  "event": "NEW"
}
```

The stable ID includes the source and account namespace. `timestamp` is CareerPulse's UTC observation time; `source_updated_at` is optional and retained only when supplied and parseable. Salary bounds and units remain separate. Missing salary and experience are explicit `null`; no scalar salary or range midpoint is inferred. A missing location is represented as `Unspecified`.

Greenhouse can provide multiple pay ranges. Those are retained as `salary_ranges`, with flattened `salary_min`, `salary_max`, `salary_currency`, and `salary_interval` fields only when one range is present.

## State and lifecycle

SQLite is the durable source of truth. Each source-account scan applies its snapshot, generated lifecycle events, run statistics, and absence counters in one transaction.

- First sighting, including a reappearance after a recorded closure: `NEW`.
- Changed tracked listing details: `MODIFIED`.
- Unchanged listing: increments that run's `unchanged` count; no duplicate event history is written.
- First complete successful absence: increments evidence without closing the job.
- Second consecutive complete successful absence: emits `CLOSED` and marks the snapshot inactive.
- A listing seen again after only one absence resets its absence evidence.
- Fetch, pagination, schema, normalization, or completeness errors do not call the state update and therefore cannot advance the canonical snapshot or absence count.

The transaction is per source account. An account's run can commit independently of another account's failed run. The optional Spark step runs after SQLite commit; a Spark failure does not roll back already committed source lifecycle state and is reported rather than replaced by Python aggregation.

## Raw-response retention

Raw API response pages are stored in the SQLite database by default for **7 days**. Set `raw_retention_days` to `0` to disable raw-response retention; other non-negative integer values configure the retention window. Expired raw payloads are deleted during a later successful scan. The normalized active snapshot and bounded lifecycle history are retained independently of this raw-response window.

Raw responses may include complete job descriptions. Retain them only as long as needed and in accordance with applicable source terms. No applicant or account-holder data is requested by these listing endpoints.

## Configure and run

From the repository root in PowerShell:

```powershell
Copy-Item .\config\ingestion.example.json .\data\ingestion.local.json
```

Edit `data\ingestion.local.json`, enable only the public company boards you intend to query, and replace the example account values. The `data/` directory is ignored by Git.

Run ingestion without starting Java:

```powershell
python -m src.ingestion.service --config .\data\ingestion.local.json
```

The command prints per-account run counts and failures and exits nonzero if any configured account failed. Successful accounts can still commit independently.

To additionally run actual Spark DataFrame batch aggregation over lifecycle events from each committed scan:

```powershell
python -m src.ingestion.service --config .\data\ingestion.local.json --spark-batch
```

This option explicitly initializes a local Spark session. A PySpark or Java startup/processing error is not caught or replaced with a Python result. This is batch aggregation only, not Kafka ingestion or Structured Streaming.

The Streamlit dashboard uses `.\data\processed\ingestion.sqlite3` by default. If the SQLite database path differs, set the environment variable before starting it:

```powershell
$env:CAREERPULSE_SQLITE_PATH = ".\data\processed\ingestion.sqlite3"
python -m streamlit run .\src\dashboard\app.py
```

If no ingestion database exists, the dashboard explicitly labels its generated content as demo data. If a database exists but is not a valid CareerPulse ingestion database, the dashboard surfaces an error instead of falling back to demo content.

## Tests

Source-adapter tests use mocked HTTP responses and do not contact live APIs:

```powershell
python -m pytest .\tests\test_ingestion_sources.py .\tests\test_ingestion_sqlite.py .\tests\test_ingestion_service.py -q
```

Spark batch tests require a working compatible Java/PySpark environment and are run separately from pure-Python unit tests when diagnosing setup failures.
