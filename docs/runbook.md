# CareerPulse Runbook

This document explains exactly how to set up, run, and validate the CareerPulse project on a local machine.

Important:
- This project has not been committed.
- The instructions below are written for Windows PowerShell, which matches the current environment.
- The project is implemented as a local Big Data pipeline scaffold. It includes code for event generation, Kafka-style publishing, Spark stream processing, storage adapters, analytics, and a Streamlit dashboard.

## 1. Prerequisites

Before running the project, make sure the following are installed on your machine:

- Python 3.10 or newer
- pip (Python package installer)
- Git (optional, for version control only)
- Kafka broker (optional for real broker-based streaming)
- Java JDK (recommended if you want true PySpark integration)

Check Python and Java:

```powershell
python --version
java -version
```

If Java is not installed, install a JDK before attempting Spark-based streaming tasks.

## 2. Open PowerShell in the Project Folder

From the project root:

```powershell
cd "C:\Users\kekum\OneDrive\Desktop\CareerPulse"
```

Verify the files exist:

```powershell
Get-ChildItem
```

You should see the folders:
- config/
- data/
- docs/
- scripts/
- src/
- tests/
- requirements.txt

## 3. Create a Virtual Environment

Create a virtual environment inside the project folder:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks the script due to execution policy, run this once:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

Confirm the virtual environment is active:

```powershell
python --version
```

You should see the Python version from the virtual environment.

## 4. Install Project Dependencies

Install the Python packages:

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

This installs dependencies such as:
- kafka-python
- pandas
- pyarrow
- streamlit
- pytest
- pyspark

If installation fails due to a package restriction, try upgrading pip first and then rerun the install command.

## 5. Check the Project Structure

The project includes these main folders:

```powershell
Get-ChildItem .\src
Get-ChildItem .\config
Get-ChildItem .\tests
```

The most important folders and files:

- src/generator/job_generator.py - Creates synthetic job events
- src/streaming/kafka_producer.py - Sends events to Kafka or prints to stdout if Kafka is unavailable
- src/streaming/spark_stream_processor.py - Validates stream records and detects event changes
- src/storage/hbase_store.py - Simulated HBase job state storage
- src/storage/parquet_store.py - Writes job records to Parquet/JSONL
- src/analytics/trend_analytics.py - Computes hiring metrics
- src/dashboard/app.py - Streamlit dashboard
- scripts/run_pipeline.py - Runs the pipeline end-to-end

## 6. Run the Test Suite

Validate the project before running the full pipeline:

```powershell
python -m pytest -q
```

If you want to run only the relevant tests:

```powershell
python -m pytest tests/test_generator.py tests/test_streaming.py -q
```

Expected result:
- 5 tests pass

## 7. Generate Job Event Data

This generates a JSONL file with realistic job events.

```powershell
python .\src\generator\job_generator.py --count 10 --output .\data\raw\job_events.jsonl
```

To generate a specific event type for every record:

```powershell
python .\src\generator\job_generator.py --count 20 --event-type NEW --output .\data\raw\new_jobs.jsonl
```

Possible event types:
- NEW
- MODIFIED
- CLOSED
- DUPLICATE

The generated file is JSON Lines format, one JSON object per line.

## 8. Run the Kafka Producer

The producer sends job events to Kafka when a Kafka broker is available.

```powershell
python .\src\streaming\kafka_producer.py --bootstrap-servers localhost:9092 --topic job_events --count 25
```

Notes:
- If Kafka is not running, the code falls back to printing the events to stdout instead of crashing.
- This is useful for local testing when you want to validate the producer flow without setting up a full broker.

## 9. Run the Pipeline End-to-End

This script generates sample events, summarizes stream results, writes data to Parquet, and prints analytics output.

```powershell
python .\scripts\run_pipeline.py
```

What it does:
1. Creates random job events
2. Saves them in data/raw/
3. Validates the records
4. Computes stream summary counts
5. Writes processed data to data/processed/
6. Builds dashboard summary metrics

You should see output similar to:

```text
Stream summary: {'DUPLICATE': 9, 'MODIFIED': 5, 'CLOSED': 7, 'NEW': 9}
Dashboard summary: {...}
Raw event file: data\raw\job_events.jsonl
Processed data file: data\processed\job_history.parquet
```

## 10. Run the Dashboard

Start the Streamlit app from the project root:

```powershell
streamlit run .\src\dashboard\app.py
```

If Streamlit is not recognized, try:

```powershell
python -m streamlit run .\src\dashboard\app.py
```

After the app starts, the terminal will show a local URL like:

```text
Local URL: http://localhost:8501
```

Open that URL in your browser to view the dashboard.

## 11. Explore Output Files

After running the pipeline, inspect the generated files:

```powershell
Get-ChildItem .\data
Get-ChildItem .\data\raw
Get-ChildItem .\data\processed
```

Typical output files include:
- data/raw/job_events.jsonl
- data/processed/job_history.parquet
- data/processed/demo_jobs.jsonl

## 12. Optional: Start Kafka Manually

If you want actual Kafka-based streaming rather than the fallback behavior, start a Kafka broker on your machine.

Typical local setup:

```powershell
# Example placeholder - depends on your Kafka installation
# Start ZooKeeper
# Start Kafka server
# Create a topic
```

Then send data to the topic using the producer script.

Example:

```powershell
python .\src\streaming\kafka_producer.py --bootstrap-servers localhost:9092 --topic job_events --count 50
```

## 13. Optional: Run the Generator with a Fixed Event Type

This lets you simulate one kind of job event at a time:

```powershell
python .\src\generator\job_generator.py --count 50 --event-type MODIFIED --output .\data\raw\modified_jobs.jsonl
```

## 14. Troubleshooting

### Problem: `ModuleNotFoundError: No module named 'src'`

This usually happens when running a script from the wrong working directory.

Fix:

```powershell
cd "C:\Users\kekum\OneDrive\Desktop\CareerPulse"
python .\scripts\run_pipeline.py
```

### Problem: `streamlit` not found

Install dependencies again:

```powershell
python -m pip install -r requirements.txt
```

### Problem: Kafka connection errors

The code gracefully falls back to stdout when Kafka is not available, but for true streaming you need a running Kafka broker.

### Problem: PySpark JDK or environment issues

Install a JDK and confirm it is reachable:

```powershell
java -version
```

### Problem: tests fail

Run the exact tests again:

```powershell
python -m pytest tests/test_generator.py tests/test_streaming.py -q
```

## 15. Recommended Local Workflow

For a normal development cycle, run the project in this sequence:

```powershell
cd "C:\Users\kekum\OneDrive\Desktop\CareerPulse"
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pytest -q
python .\scripts\run_pipeline.py
streamlit run .\src\dashboard\app.py
```

## 16. Summary

This project can be run in three main modes:

1. Offline local simulation
   - generate records
   - run pipeline directly
   - view dashboard

2. Kafka producer mode
   - send events to Kafka
   - use it with an active broker

3. Analytics validation mode
   - run tests
   - inspect JSONL and Parquet outputs
   - confirm metrics are generated correctly

This gives you a working local Big Data architecture scaffold for CareerPulse without needing a full production-scale infrastructure setup.
