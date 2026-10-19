"""Near-duplicate job postings with MinHash locality-sensitive hashing.

Change detection only catches a duplicate when the same ``job_id`` repeats
with identical fields. The same opening is often posted again under another
id, by another source or with slightly different wording ("Sr. Data Engineer"
vs "Senior Data Engineer, Platform"). This job finds such pairs among the
currently open postings.

Each posting becomes a set of character 3-gram shingles over its normalised
company, title, location and experience. Comparing every pair would be
O(n^2); Spark MLlib's ``MinHashLSH`` instead hashes each set with
``num_hash_tables`` random min-hash functions and only compares postings that
collide in at least one table. Two postings with Jaccard similarity ``s``
become a candidate with probability ``1 - (1 - s)^num_hash_tables`` (0.9997
for s = 0.8 and 5 tables), and every candidate's exact Jaccard distance is
computed, so reported pairs are never false positives; a true pair is missed
only with the small probability above.
"""

from __future__ import annotations

from typing import Any, Dict, List

from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SHINGLE_SIZE = 3
NUM_FEATURES = 1 << 18
# 0.85 rather than 0.8: postings differing only in experience band ("0-2 years"
# vs "2-4 years") score about 0.8 and are different roles.
DEFAULT_MIN_SIMILARITY = 0.85
DEFAULT_HASH_TABLES = 5
DEFAULT_MAX_PAIRS = 500

# Common title abbreviations, expanded before shingling (word-boundary regex -> replacement).
ABBREVIATIONS = [
    (r"\bsr\b", "senior"),
    (r"\bjr\b", "junior"),
    (r"\beng\b", "engineer"),
    (r"\bmgr\b", "manager"),
    (r"\bdev\b", "developer"),
    (r"\bml\b", "machine learning"),
]


def open_postings(events_df: DataFrame) -> DataFrame:
    """Latest state of every job whose most recent lifecycle event is not CLOSED.

    DUPLICATE events never change state, so they are ignored when finding the
    latest event.
    """
    if "source" not in events_df.columns:
        events_df = events_df.withColumn("source", F.lit(None).cast("string"))
    latest = Window.partitionBy("job_id").orderBy(F.col("event_time").desc())
    return (
        events_df
        .where(F.col("event") != "DUPLICATE")
        .withColumn("rn", F.row_number().over(latest))
        .where((F.col("rn") == 1) & (F.col("event") != "CLOSED"))
        .select("job_id", "company", "title", "location", "experience", "source")
    )


def normalized_text(*columns) -> Any:
    """Lower-case, strip punctuation, expand abbreviations and collapse spaces."""
    text = F.lower(F.concat_ws(" ", *[F.coalesce(column, F.lit("")) for column in columns]))
    text = F.regexp_replace(text, r"[^a-z0-9+#]+", " ")
    for pattern, replacement in ABBREVIATIONS:
        text = F.regexp_replace(text, pattern, replacement)
    return F.trim(F.regexp_replace(text, r"\s+", " "))


def shingle_postings(postings: DataFrame) -> DataFrame:
    """Add the posting's character shingles as a sparse binary vector."""
    text = normalized_text(F.col("company"), F.col("title"), F.col("location"), F.col("experience"))
    shingled = (
        postings
        .withColumn("text", text)
        .where(F.length("text") >= SHINGLE_SIZE)
        .withColumn(
            "shingles",
            F.array_distinct(F.expr(
                f"transform(sequence(1, length(text) - {SHINGLE_SIZE - 1}), i -> substring(text, i, {SHINGLE_SIZE}))"
            )),
        )
    )
    return HashingTF(inputCol="shingles", outputCol="features", numFeatures=NUM_FEATURES, binary=True).transform(shingled)


def find_near_duplicates(
    events_df: DataFrame,
    *,
    min_similarity: float = DEFAULT_MIN_SIMILARITY,
    num_hash_tables: int = DEFAULT_HASH_TABLES,
    max_pairs: int = DEFAULT_MAX_PAIRS,
    seed: int = 42,
) -> Dict[str, Any]:
    """Pairs of open postings whose shingle sets have Jaccard similarity >= ``min_similarity``."""
    if not 0.0 < min_similarity <= 1.0:
        raise ValueError("min_similarity must be in (0, 1].")
    vectors = shingle_postings(open_postings(events_df)).cache()
    postings = vectors.count()
    summary = {
        "method": "Spark MLlib MinHashLSH on character 3-gram shingles",
        "open_postings": postings,
        "min_similarity": min_similarity,
        "num_hash_tables": num_hash_tables,
        "pairs_found": 0,
        "jobs_involved": 0,
    }
    if postings < 2:
        vectors.unpersist()
        return {"summary": summary, "pairs": []}

    model = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=num_hash_tables, seed=seed).fit(vectors)
    a, b = F.col("datasetA"), F.col("datasetB")
    # The join keeps distance < threshold (exclusive) and 1 - s is inexact in
    # floating point, so join slightly wide and apply the inclusive bound here.
    pairs = (
        model.approxSimilarityJoin(vectors, vectors, 1.0 - min_similarity + 1e-9, distCol="jaccard_distance")
        .where((a["job_id"] < b["job_id"]) & (1.0 - F.col("jaccard_distance") >= min_similarity - 1e-12))
        .select(
            a["job_id"].alias("job_id_a"),
            b["job_id"].alias("job_id_b"),
            F.round(1.0 - F.col("jaccard_distance"), 4).alias("similarity"),
            a["company"].alias("company_a"),
            b["company"].alias("company_b"),
            a["title"].alias("title_a"),
            b["title"].alias("title_b"),
            a["location"].alias("location_a"),
            b["location"].alias("location_b"),
            a["source"].alias("source_a"),
            b["source"].alias("source_b"),
        )
        .withColumn("same_source", F.col("source_a").eqNullSafe(F.col("source_b")))
        .cache()
    )
    totals = pairs.agg(
        F.count(F.lit(1)).alias("pairs"),
        F.size(F.array_union(F.collect_set("job_id_a"), F.collect_set("job_id_b"))).alias("jobs"),
    ).first()
    top = pairs.orderBy(F.col("similarity").desc(), "job_id_a", "job_id_b").limit(max_pairs).collect()
    summary["pairs_found"] = int(totals["pairs"])
    summary["jobs_involved"] = int(totals["jobs"]) if totals["pairs"] else 0
    pairs.unpersist()
    vectors.unpersist()
    return {"summary": summary, "pairs": [row.asDict() for row in top]}


def pair_records(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    return list(result.get("pairs", []))
