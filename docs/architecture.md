# CareerPulse Architecture

## 1. System Overview
CareerPulse is a real-time analytics application for monitoring job-posting changes over time. It simulates semi-structured job events, streams them into Kafka, processes them with PySpark Structured Streaming, stores them in HBase and Parquet, and exposes insights through a Streamlit dashboard.

## 2. Business Objective
The system helps detect and analyze:
- new job opportunities
- modified job postings
- duplicated postings
- closed or removed positions
- company and location hiring trends

## 3. Architectural Layers

### 3.1 Source Layer
The source layer is a Python-based event generator that emits realistic job-posting data.

Responsibilities:
- produce valid JSON events
- inject realistic timestamps
- simulate event transitions (NEW, MODIFIED, DUPLICATE, CLOSED)
- maintain consistent schema across input streams

### 3.2 Transport Layer
Apache Kafka acts as the streaming buffer.

Responsibilities:
- receive events from the generator
- persist event stream in distributed topics
- decouple production from real-time processing
- support parallel consumption by Spark jobs

### 3.3 Processing Layer
PySpark Structured Streaming handles ingestion and transformation.

Responsibilities:
- read events from Kafka
- validate schema and fields
- clean and normalize records
- detect new, modified, closed, and duplicate jobs
- aggregate events into time-windowed analytics

### 3.4 Storage Layer
Two storage systems are used:

- HBase: stores operational job history and state updates
- Parquet: stores historical/raw records for analytical queries

### 3.5 Analytics Layer
This layer runs Spark SQL, Spark RDD, and simple ML operations.

Responsibilities:
- compute hiring trends over time
- aggregate data by company, location, and category
- evaluate top-k trending companies
- predict job demand or relevance

### 3.6 Presentation Layer
Streamlit provides the dashboard layer.

Responsibilities:
- display live events
- render trend charts and summaries
- present KPI cards
- show ML predictions and company analysis

## 4. End-to-End Pipeline

```text
Python Job Generator
        ↓
Kafka Topic
        ↓
PySpark Structured Streaming
        ↓
Validation + Cleaning + Deduplication + Change Detection
        ↓
        ├── HBase (operational history)
        ├── Parquet (historical/raw dataset)
        └── Spark Analytics Engine
                ├── Spark SQL
                ├── Spark RDD
                └── Simple ML model
                        ↓
                Streamlit Dashboard
```

## 5. Core Data Flow
1. Generator emits semi-structured JSON events.
2. Producer sends data to Kafka.
3. Spark consumer reads the Kafka stream.
4. Stream processor checks schema, missing values, and duplicates.
5. The system compares current job state with previous job state.
6. Results are stored in HBase and Parquet.
7. Analytics job reads storage and creates insights.
8. Dashboard queries processed results and displays updates.

## 6. Event Model
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

## 7. Processing Logic
The processing engine should do the following:
- validate required fields
- normalize text values
- check for duplicates
- compare against previous records
- mark event type as NEW, MODIFIED, CLOSED, or DUPLICATE
- update HBase state and append to historical Parquet data

## 8. Storage Model
### HBase
- row key: job_id
- columns: latest state, timestamp, company, title, location, status, change history
- supports fast lookups for job-state tracking

### Parquet
- stores immutable historical job events
- optimized for large-scale analytical reads
- suited for Spark-based aggregation and trend analysis

## 9. Analytics Use Cases
- new jobs in last 5 minutes
- top companies by postings
- company, location, and category hiring trends
- average salary by job category
- modified and closed counts
- top-k trending companies using RDD
- ML-based demand or relevance prediction

## 10. Dashboard Modules
- live event stream
- trend chart panel
- top companies list
- hiring distribution by location
- summary KPI cards
- model prediction display

## 11. Risk and Scope Control
This version intentionally avoids:
- large-scale web crawling
- complex microservice decomposition
- deep learning
- advanced fault-injection infrastructure
- complicated scheduler or API gateway

This keeps the architecture aligned with a medium-level, two-person project.

## 12. Final Design Summary
CareerPulse is designed as a compact but realistic Big Data architecture. It demonstrates end-to-end streaming, storage, ETL, analytics, and visualization while staying practical for implementation and demonstration.
# CareerPulse Project Overview and Architecture

CareerPulse is a local end-to-end streaming analytics project designed to simulate a realistic job market monitoring system. The goal is simple: generate job-posting events, validate them, detect change signals such as new, modified, duplicate, and closed records, store them for later analysis, and expose the results through a dashboard.

This repository is not a full production deployment with a real Kafka cluster or a real HBase database. Instead, it is a practical Big Data-style scaffold that demonstrates the architecture and logic of a real-time streaming pipeline in a local Python environment.

## 1. The business problem

A hiring platform or recruitment intelligence team wants to answer questions like:

- Which companies are hiring the most right now?
- Which locations are seeing the biggest demand?
- Which job listings are new, modified, or closed?
- How are salary levels and hiring trends changing over time?
- What are the most active hiring sectors or trends in the last few minutes?

CareerPulse models this by generating synthetic job events, feeding them through a streaming pipeline, and summarizing them into operational and analytical metrics.

## 2. High-level architecture

The architecture follows a standard streaming-data flow:

```text
Python job event generator
        ↓
Kafka-style producer / event stream
        ↓
PySpark stream processor
        ↓
Validation + deduplication + change detection
        ↓
Storage layer (in-memory HBase-style state + Parquet/JSONL output)
        ↓
Analytics layer (counts, company trends, salary summaries)
        ↓
Streamlit dashboard
```

In this project, the components are intentionally lightweight and local so they can be run on a laptop without a cluster. The design still mirrors the major layers you would see in a real production system: ingestion, processing, storage, analytics, and visualization.

## 3. What the project is doing from start to finish

The project lifecycle is:

1. Generate realistic job events.
2. Represent each event as a structured JSON record.
3. Validate each event for required fields and correct values.
4. Send it through the streaming layer.
5. Aggregate or classify it into NEW, MODIFIED, CLOSED, or DUPLICATE.
6. Save data to persistent output stores.
7. Compute hiring insights such as top companies and salary averages.
8. Show the results in a dashboard.

## 4. Core architectural layers

### 4.1 Event generation layer

The event generation logic lives in:

- src/generator/job_generator.py

This module creates synthetic job postings with fields such as:

- job_id
- company
- title
- location
- experience
- salary
- timestamp
- event

It picks values from realistic lists such as company names, locations, job titles, and experience ranges. Event types are chosen from:

- NEW
- MODIFIED
- CLOSED
- DUPLICATE

This is the starting point of the whole pipeline. Without this layer, there would be no stream to process.

### 4.2 Validation and data model layer

The shared model and validation rules live in:

- src/common/models.py

Important parts:

- JobEvent dataclass
- REQUIRED_FIELDS set
- VALID_EVENTS set
- JobEvent.from_dict() for parsing raw payloads
- validate_record() to guarantee the structure is sound

This is the “contract” of the pipeline. It enforces that every record includes the required fields and that event values are valid. This protects downstream processing from malformed data.

### 4.3 Streaming and processing layer

The main streaming logic lives in:

- src/streaming/spark_stream_processor.py

This module wraps a PySpark session when available and contains the main classification logic:

- process_batch() validates and summarizes records
- detect_change() determines whether a record is new, modified, closed, or duplicate relative to prior data
- summarize_event_stream() reduces a batch to event counts

The processing logic is intentionally simple but mirrors how a real streaming engine would behave:

- read incoming records
- filter invalid records
- aggregate valid ones by event type
- compare against prior state to detect transitions

Even though this is a local scaffold, the code shows the pattern you would use in a genuine Spark Structured Streaming job.

### 4.4 Storage layer

The storage layer is split across:

- src/storage/hbase_store.py
- src/storage/parquet_store.py

#### HBase-style state store

The HBase store is a local in-memory dictionary that mimics a row-based state store. It stores job records keyed by job_id and exposes methods:

- put_job()
- get_job()
- list_jobs()

This acts like the operational state table that would hold the current job snapshot or latest record for each job.

#### Parquet output store

The Parquet store writes records to Parquet when the pandas/pyarrow stack is available. If those packages are unavailable or the environment is minimal, it falls back to writing JSONL instead of failing.

This is the historical data layer used for offline analytics and long-term access.

### 4.5 Analytics layer

The analytics logic is implemented in:

- src/analytics/trend_analytics.py

It computes summaries such as:

- total_jobs
- new_jobs
- modified_jobs
- closed_jobs
- top companies by volume
- average salary by company

This layer turns raw events into business insight. It is the reason the project is not just a generator; it is a mini analytics engine for recruitment signals.

### 4.6 Dashboard layer

The dashboard is implemented in:

- src/dashboard/app.py

This file uses Streamlit to render:

- total jobs
- new jobs
- modified jobs
- closed jobs
- top company ranking
- salary distribution
- recent jobs in a dataframe

The dashboard is the user-facing layer. It converts processed analytics into a visual summary suitable for quick business review.

### 4.7 Orchestration layer

The full end-to-end pipeline is orchestrated by:

- scripts/run_pipeline.py

This script does the following in order:

1. creates a raw data directory
2. generates sample events
3. writes them to data/raw/job_events.jsonl
4. summarizes the event stream
5. writes processed data to data/processed/job_history.parquet
6. computes dashboard summary metrics
7. prints the results

This is the central script that binds all layers together.

## 5. Data flow: beginning to end

The actual runtime flow is:

1. Generate a job posting record.
2. The record is shaped like JSON.
3. validate_record() checks that all mandatory fields exist and values are valid.
4. summarize_event_stream() counts the event types.
5. detect_change() decides whether the record indicates a new or changed state.
6. The result is written to raw/processed output paths.
7. The analytics summary is created from the same records.
8. The Streamlit dashboard reads the summary and visualizes it.

A simplified example is:

```text
generate_job_event() -> {job_id, company, title, ... event: "NEW"}
      ↓
validate_record()
      ↓
process_batch() -> Counter({'NEW': 1})
      ↓
write_parquet_records() / write_events()
      ↓
build_summary() -> {total_jobs, top_companies, average_salary_by_company}
      ↓
Streamlit dashboard
```

## 6. Repository structure

The repository is organized around the standard lifecycle of a data pipeline:

```text
CareerPulse/
├── README.md
├── requirements.txt
├── config/
│   ├── kafka_config.yaml
│   └── spark_config.yaml
├── data/
│   ├── raw/
│   └── processed/
├── docs/
│   └── runbook.md
├── scripts/
│   └── run_pipeline.py
├── src/
│   ├── analytics/
│   │   └── trend_analytics.py
│   ├── common/
│   │   └── models.py
│   ├── dashboard/
│   │   └── app.py
│   ├── generator/
│   │   └── job_generator.py
│   ├── storage/
│   │   ├── hbase_store.py
│   │   └── parquet_store.py
│   └── streaming/
│       └── spark_stream_processor.py
├── tests/
│   ├── test_generator.py
│   └── test_streaming.py
└── .venv/
```

Each folder maps to a concern in the system:

- config: runtime settings for Kafka and Spark-like processing
- data: raw and processed outputs
- docs: project documentation
- scripts: orchestration scripts
- src: all domain logic
- tests: validation and regression checks

## 7. Configuration and environment

The project uses lightweight configuration files:

- config/kafka_config.yaml
- config/spark_config.yaml

These define topics, app names, checkpoint locations, and batch durations. The Kafka config includes a `job_events` topic and local bootstrap server `localhost:9092`.

The Spark config includes an application name and local master config using `local[*]`, which makes the pipeline run a Spark session locally rather than requiring a cluster.

## 8. Dependencies

The requirements file includes the core stack for this project:

- kafka-python
- pandas
- pyarrow
- streamlit
- pytest
- pyspark

This combination supports:

- event publishing
- data transformation and processing
- file output and Parquet handling
- visualization
- testing

## 9. What is simulated versus real

This project is intentionally a simulation, not a production deployment. The code mirrors real-world Big Data workflows without the operational cost of launching full infrastructure.

### Simulated components

- Kafka: producer logic exists, but it can fall back to stdout if Kafka is unavailable.
- HBase: implemented as in-memory state instead of a real HBase cluster.
- Spark: PySpark is used when installed, but the project also works in a simpler local mode.
- Parquet: real Parquet output is attempted when available; otherwise the fallback is JSONL.

### Real-world logic represented

The code still shows the architecture of a real system:

- event generation
- message transport
- ingestion
- validation
- state tracking
- batch summarization
- long-term storage
- analytics
- UI reporting

## 10. File-by-file responsibilities

### README.md

This gives the high-level summary of the project goal and the architecture. It explains the overall vision of the system before the code is examined.

### src/generator/job_generator.py

This is the birth stage of the dataset. It creates job-related events with consistent shape and realistic values.

### src/common/models.py

This defines the schema and validation rules that every event must satisfy. It is the safety net of the pipeline.

### src/streaming/spark_stream_processor.py

This is the central processing engine. It supports the transformation and classification of raw inputs into meaningful event states.

### src/storage/hbase_store.py

This is the current-state store. It stores the latest state for each job by unique job_id.

### src/storage/parquet_store.py

This writes raw or processed data to Parquet when the environment supports it.

### src/analytics/trend_analytics.py

This creates the intelligence layer. It aggregates events into job-market insight.

### src/dashboard/app.py

This is the final presentation layer. It converts the analyzed data into a real-time dashboard.

### scripts/run_pipeline.py

This is the orchestrator that runs the whole pipeline end-to-end.

### tests/test_generator.py and tests/test_streaming.py

These verify that generated events match the expected schema and that stream processing produces valid counts.

## 11. Example event lifecycle

A single event might flow like this:

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

After validation:

- all fields exist
- salary is numeric
- event type is recognized
- the data is accepted for processing

After stream processing:

- summary counts update
- quarterly or time-based trend data can be created
- the dashboard shows a fresh hiring signal

## 12. Why this architecture is useful

This architecture teaches the right design principles for real-time analytics systems:

- separate sources of truth for raw data and processed data
- validate early to keep bad records out of the pipeline
- detect state change rather than just counting records
- store both operational and analytical artifacts
- expose insight through a simple but effective UI

The project is small enough for local experimentation, but it still follows the structure of a serious data platform.

## 13. Local workflow

To run the project, the usual sequence is:

```powershell
cd "C:\Users\kekum\OneDrive\Desktop\CareerPulse"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pytest -q
python .\scripts\run_pipeline.py
streamlit run .\src\dashboard\app.py
```

This flow does the following:

- creates the environment
- installs dependencies
- checks the project is behaving correctly
- generates events
- processes them
- produces output files
- shows the dashboard

## 14. Validation and quality checks

The project includes automated tests that confirm:

- generated job events have the required fields
- batch generation returns the correct count
- validation accepts correct records
- event summaries behave as expected

This matters because event-driven systems fail quickly when the payload contract is broken.

## 15. Limitations and future improvements

This repository is intentionally a learning or demo scaffold. It does not yet include:

- a real Kafka broker integration in production mode
- a real HBase cluster
- true Spark Structured Streaming micro-batching at scale
- a real ML pipeline with model training and deployment
- distributed storage and orchestration

A next step for a production-grade version would be to add:

- Kafka producers and consumers with topic partitions
- a Spark Structured Streaming job reading from Kafka
- stateful aggregations with checkpointing
- HBase or Cassandra persistence
- Parquet table partitioning by date
- ML-based demand prediction and alerting
- deployment with Docker Compose or Kubernetes

## 16. Final summary

CareerPulse is a complete beginner-to-midlevel Big Data project that demonstrates how a real-time job market intelligence system can be built. It covers the full lifecycle from event generation to operational storage, analytics, and dashboard presentation.

The important idea is that the system is not just about generating random data. It is about showing how a realistic pipeline organizes itself:

- raw events are created
- records are validated
- the stream is processed and classified
- state is tracked
- analytics are derived
- insights are visualized

That end-to-end flow is the heart of the project and the core architectural pattern this repository is trying to teach.

## 17. Quick reference

If you want to understand the project in one sentence:

CareerPulse is a local streaming analytics demo that simulates job-posting change detection and recruitment trend analysis using Python, Kafka-inspired message flow, PySpark, storage adapters, and a Streamlit dashboard.

This is enough to explain the system from the beginning of data generation to the end of dashboard insight.
