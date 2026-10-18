# syntax=docker/dockerfile:1
# Linux Spark runtime for the CareerPulse streaming job and batch analytics.
# Spark's local filesystem layer needs Hadoop winutils on Windows; running
# Spark here avoids that and matches a typical cluster environment.
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY docker/spark/requirements.txt /tmp/requirements.txt
# Optional pre-downloaded wheels (git-ignored), e.g. a locally built PySpark
# wheel, are preferred over PyPI. The PySpark download is large, so a long
# timeout, retries and a BuildKit cache keep slow connections from failing.
COPY docker/spark/wheels/ /tmp/wheels/
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --default-timeout=600 --retries 10 --find-links /tmp/wheels -r /tmp/requirements.txt \
    # A wheel built on Windows drops the executable bit on Spark's launchers.
    && chmod +x "$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"/bin/* \
    && rm -rf /tmp/wheels

COPY src /app/src
COPY scripts /app/scripts

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python

CMD ["python", "-m", "src.streaming.spark_kafka_consumer", "--help"]
