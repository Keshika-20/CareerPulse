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
