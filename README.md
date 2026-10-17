# CareerPulse

CareerPulse is a local job-market ingestion and lifecycle analytics project. It can fetch configured public Greenhouse and Lever company boards, normalize their postings, track changes across runs in SQLite, and display persisted or synthetic data in Streamlit.

## Current Implementation Status

- Greenhouse and Lever public company-board adapters are implemented. They use bounded HTTP timeouts and retries, and source responses are validated before a scan is treated as complete.
- Listings are normalized to stable source/account/job IDs. Salary and experience may be unknown (`null`); provider salary ranges remain bounds and are not converted into an invented scalar salary.
- SQLite is the durable snapshot and lifecycle store. An absent listing is closed only after two consecutive complete successful scans for its source account.
- Spark is an explicit optional batch aggregation path (`--spark-batch`) over lifecycle events. It is not a streaming source or sink.
- The dashboard labels synthetic demo data separately from persisted ingestion data.

Kafka consumer integration, PySpark Structured Streaming, HBase persistence, ingestion-to-Parquet output, Spark SQL/RDD analytics, and machine-learning predictions are not implemented. The Kafka producer, in-memory HBase-shaped example, and Parquet helper are separate demonstrations and are not wired into the public-source ingestion path.

The historical architecture sections below describe a target design, not the current runtime pipeline.

## Current dashboard workspace

The Streamlit dashboard includes four views:

- **Overview:** active listings, represented companies and sources, lifecycle totals, company distribution, and recent changes.
- **Job explorer:** search and filter active jobs by title/company/location/source, inspect listing details, open the original provider posting, and export filtered results as CSV.
- **Pipeline control:** view the ingestion stages, launch the existing ingestion service from the UI, and inspect latest source-account scans and run history.
- **Lifecycle events:** filter the recent persisted NEW/MODIFIED/CLOSED audit trail and export it as CSV.

The dashboard reads the configured SQLite database in read-only mode. Use the sidebar's database field or `CAREERPULSE_SQLITE_PATH` to select the database. The pipeline control view runs the ordinary Python ingestion command; it does not silently enable Spark. Only configured and enabled public source accounts are fetched. If only one Greenhouse account is enabled, the dashboard will only contain that account's postings; add additional verified Greenhouse board tokens and/or Lever site identifiers to your local ingestion configuration to expand coverage.

## 1. Project Goal
The implemented workflow fetches public listings for configured company boards and tracks cross-run changes. A separate synthetic generator remains available for local demonstrations. The current ingestion workflow is batch-oriented, not real time. It can record:

- new postings
- modified postings
- closed postings
- duplicate postings
- hiring trend changes over time

## 2. Target High-Level Architecture (Not Implemented)

```text
Python Job Event Generator
        ↓
Apache Kafka
        ↓
PySpark Structured Streaming
        ↓
Validation + Cleaning + Change Detection + Aggregation
        ↓
    ┌───────────────┬───────────────┐
    │               │               │
    ▼               ▼               ▼
HBase            Parquet        DuckDB / Analytics Layer
    │               │               │
    └───────────────┴───────────────┘
        ↓
Spark SQL + Spark RDD + Simple ML
        ↓
Streamlit Dashboard
```

## 3. Pipeline Flow

1. Data Generation
   - Python generates semi-structured JSON job events.
   - Each record includes job metadata, timestamp, and event type.

2. Streaming Layer
   - Kafka acts as the ingestion transport.
   - Producer pushes streaming events continuously.

3. Ingestion and Validation
   - PySpark Structured Streaming reads Kafka topics.
   - Records are validated for schema, null values, duplicates, and malformed fields.

4. Change Detection
   - Job records are compared against prior state to detect:
     - NEW
     - MODIFIED
     - CLOSED
     - DUPLICATE

5. Storage Layer
   - HBase stores operational job history and state.
   - Parquet stores historical/raw event data for analytical querying.

6. Analytics Layer
   - Spark SQL performs trend aggregation over company, location, category, and salary.
   - Spark RDD performs top-k and trending-company analysis.
   - A simple ML model predicts demand or relevance from selected job attributes.

7. Visualization Layer
   - Streamlit renders real-time metrics, event feed, analytics, and model outcomes.

## 4. Core Components

### 4.1 Job Event Generator
- Produces realistic semistructured job data in JSON.
- Includes fields such as job_id, company, title, location, experience, salary, timestamp, and event.
- Simulates event types such as NEW, MODIFIED, CLOSED, DUPLICATE.

### 4.2 Kafka Producer/Consumer Layer
- Carries events from generator to Spark.
- Provides decoupling between event production and processing.
- Enables continuous data arrival and scaling to streaming workloads.

### 4.3 PySpark Structured Streaming
- Reads and processes Kafka streams.
- Performs ETL operations: filtering, cleansing, transformation, and enrichment.
- Builds real-time derived datasets for trend detection.

### 4.4 Validation and Change Detection
- Schema validation
- Missing-value checks
- Duplicate detection
- State comparison against previous job snapshots
- Event classification and archival

### 4.5 Storage Architecture
- HBase: row-based operational history for incremental updates and current state.
- Parquet: columnar storage for historical analytics and long-term data retention.

### 4.6 Analytics Engine
- Spark SQL for hiring trends by company, location, category, and time window.
- Spark RDD for top-k and trending-company calculations.
- ML model for demand/relevance forecasting.

### 4.7 Dashboard
- Live event stream
- New/modified/closed job counts
- Trend charts
- Top companies
- Salary analytics
- Model predictions

## 5. Example Event Schema

```json
{
  "job_id": "J1024",
  "company": "Google",
  "title": "Software Engineer",
  "location": "Bangalore",
  "experience": "0-2 years",
  "salary": 1200000,
  "timestamp": "2026-10-06T16:30:00Z",
  "event": "NEW"
}
```

## 6. Major Analytics Supported
- New jobs received in the last 5 minutes
- Top companies by new postings
- Hiring trends by company, location, and category
- Average salary by category
- Modified and closed posting counts
- Top-k trending companies using Spark RDD
- Predicted job demand/relevance using a simple ML model

## 7. Target Technology Mapping (Not Implemented)

| Layer | Technology | Responsibility |
|---|---|---|
| Event generation | Python | Simulate job-posting events |
| Streaming | Apache Kafka | Event transport and buffering |
| Processing | PySpark Structured Streaming | Stream ETL and validation |
| Operational storage | HBase | Current and historical job-state storage |
| Historical storage | Parquet | Raw and aggregated analytics data |
| Analytics | Spark SQL + RDD | Trend and ranking calculations |
| Prediction | Simple ML | Demand or relevance prediction |
| Dashboard | Streamlit | Real-time reporting |
| Optional analytics query engine | DuckDB | Local analytical exploration |

## 8. Recommended Folder Structure

```text
CareerPulse/
├── README.md
├── docs/
│   └── architecture.md
├── config/
│   ├── kafka_config.yaml
│   └── spark_config.yaml
├── data/
│   ├── raw/
│   └── processed/
├── src/
│   ├── generator/
│   ├── streaming/
│   ├── analytics/
│   └── dashboard/
├── scripts/
│   ├── start_kafka.sh
│   ├── start_spark.sh
│   └── run_dashboard.sh
├── tests/
│   ├── test_generator.py
│   ├── test_streaming.py
│   └── test_analytics.py
└── requirements.txt
```

## 9. Implementation Phases

### Phase 1: Foundations
- Set up Python project environment
- Create event generator
- Define JSON schema

### Phase 2: Streaming Layer
- Configure Kafka producer and consumer
- Validate streaming ingestion

### Phase 3: Spark Processing
- Build Structured Streaming jobs
- Implement validation, deduplication, and change detection

### Phase 4: Storage
- Connect to HBase for operational data
- Write and query Parquet for analytical history

### Phase 5: Analytics and ML
- Build Spark SQL aggregations
- Implement RDD transformations
- Train simple demand/relevance model

### Phase 6: Dashboard
- Visualize live feeds and trend metrics
- Present ML predictions and company insights

## 10. Non-Functional Focus
- Real-time processing of continuous events
- Fault-tolerant streaming pipeline
- Data consistency across HBase and Parquet
- Maintainable modular code structure
- Clear separation between ingestion, processing, storage, analytics, and UI

## 11. Summary
This is a summary of the aspirational target design above, not a statement about current implementation. For the actual ingestion and runtime workflow, see [the ingestion guide](./docs/ingestion.md) and [the runbook](./docs/runbook.md).
