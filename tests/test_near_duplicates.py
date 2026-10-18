"""MinHash LSH near-duplicate posting detection over the event lake."""

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")

from src.analytics.near_duplicates import find_near_duplicates, open_postings  # noqa: E402
from src.analytics.run_analytics import run_all  # noqa: E402
from src.common.spark_session import build_spark_session  # noqa: E402
from src.storage import parquet_store  # noqa: E402

START = datetime(2026, 10, 6, 8, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    session = build_spark_session("CareerPulse near-duplicate tests", "local[2]")
    yield session
    session.stop()


def _event(job_id, title, *, company="Google", location="Bangalore", experience="2-4 years",
           event="NEW", minutes=0, source="greenhouse"):
    return {
        "event_id": f"{job_id}:{minutes}", "job_id": job_id, "company": company, "title": title,
        "location": location, "experience": experience, "salary": 1500000.0, "category": "Data",
        "source": source, "event": event, "event_time": (START + timedelta(minutes=minutes)).isoformat(),
    }


def _frame(spark, records):
    import pandas as pd

    frame = pd.DataFrame(records)
    frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
    return spark.createDataFrame(frame)


RECORDS = [
    _event("G1", "Senior Data Engineer"),
    _event("L1", "Sr. Data Engineer", source="lever"),                       # abbreviation, other source
    _event("G2", "Senior Data Engineer, Platform"),                          # extra words
    _event("G3", "Product Manager"),                                         # different role
    _event("A1", "Senior Data Engineer", company="Amazon", location="Pune"),  # different company and city
    _event("G4", "Senior Data Engineer", minutes=5),
    _event("G4", "Senior Data Engineer", event="CLOSED", minutes=10),         # closed: not open any more
    _event("G5", "Data Analyst", minutes=0),
    _event("G5", "Senior Data Engineer", event="MODIFIED", minutes=5),       # latest state counts
    _event("G5", "Senior Data Engineer", event="DUPLICATE", minutes=6),
]


def test_open_postings_use_latest_state_and_drop_closed(spark):
    postings = {row["job_id"]: row for row in open_postings(_frame(spark, RECORDS)).collect()}
    assert set(postings) == {"G1", "L1", "G2", "G3", "A1", "G5"}
    assert postings["G5"]["title"] == "Senior Data Engineer"


def test_minhash_lsh_finds_near_duplicates(spark):
    result = find_near_duplicates(_frame(spark, RECORDS), min_similarity=0.7, num_hash_tables=10)
    pairs = {(row["job_id_a"], row["job_id_b"]): row for row in result["pairs"]}

    # Identical after normalisation ("Sr." -> "senior"), across sources.
    assert pairs[("G1", "L1")]["similarity"] == 1.0
    assert pairs[("G1", "L1")]["same_source"] is False
    assert pairs[("G1", "G5")]["same_source"] is True
    # Extra title words lower the similarity but stay above the threshold.
    assert 0.7 <= pairs[("G1", "G2")]["similarity"] < 1.0
    # Different role or different company/location: not reported. Closed G4 is excluded.
    assert not any("G3" in key or "A1" in key or "G4" in key for key in pairs)
    assert all(row["similarity"] >= 0.7 for row in result["pairs"])

    summary = result["summary"]
    assert summary["open_postings"] == 6
    assert summary["pairs_found"] == len(pairs)
    assert summary["jobs_involved"] == 4  # G1, L1, G2, G5


def test_lsh_matches_exact_all_pairs_jaccard(spark):
    """On generated postings, LSH recovers (almost) every pair an exact O(n^2) scan finds."""
    import random

    from src.analytics.near_duplicates import shingle_postings

    rng = random.Random(7)
    titles = ["Data Engineer", "Senior Data Engineer", "Backend Engineer", "Sr Backend Engineer",
              "Product Manager", "Data Analyst", "ML Engineer", "Machine Learning Engineer"]
    records = [
        _event(f"J{index}", rng.choice(titles), company=rng.choice(["Google", "Amazon", "Uber"]),
               location=rng.choice(["Pune", "Remote"]), experience=rng.choice(["0-2 years", "2-4 years"]))
        for index in range(120)
    ]
    frame = _frame(spark, records)
    threshold = 0.8

    shingles = {
        row["job_id"]: set(row["shingles"])
        for row in shingle_postings(open_postings(frame)).select("job_id", "shingles").collect()
    }
    ids = sorted(shingles)
    exact = {
        (a, b)
        for i, a in enumerate(ids) for b in ids[i + 1:]
        if len(shingles[a] & shingles[b]) / len(shingles[a] | shingles[b]) >= threshold
    }

    result = find_near_duplicates(frame, min_similarity=threshold, max_pairs=100000)
    found = {(row["job_id_a"], row["job_id_b"]) for row in result["pairs"]}
    assert found <= exact  # candidates are verified, so no false positives
    assert len(found) >= 0.98 * len(exact)


def test_near_duplicates_with_too_few_postings(spark):
    result = find_near_duplicates(_frame(spark, [_event("G1", "Data Engineer")]))
    assert result["pairs"] == []
    assert result["summary"]["open_postings"] == 1
    with pytest.raises(ValueError):
        find_near_duplicates(_frame(spark, RECORDS), min_similarity=0)


def test_run_all_writes_near_duplicate_table(spark, tmp_path):
    parquet_store.append_events(RECORDS, tmp_path)
    result = run_all(spark, tmp_path, with_ml=False, near_duplicate_similarity=0.7)
    assert result["summary"]["near_duplicates"]["pairs_found"] == len(result["outputs"]["near_duplicate_postings"])
    assert (tmp_path / parquet_store.ANALYTICS_DATASET / "near_duplicate_postings.parquet").exists()
