"""Streamlit view for the streaming pipeline: live events, trends, Spark outputs, HBase."""

from __future__ import annotations

import html
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.analytics.duckdb_queries import LakeQueries

DEFAULT_LAKE = "./data/lake"


def hbase_settings() -> Dict[str, Any]:
    return {
        "mode": os.environ.get("CAREERPULSE_HBASE_MODE", "local"),
        "host": os.environ.get("CAREERPULSE_HBASE_HOST", "localhost"),
        "port": int(os.environ.get("CAREERPULSE_HBASE_PORT", "9090")),
        "path": os.environ.get("CAREERPULSE_HBASE_PATH", "./data/hbase/careerpulse_hbase.sqlite3"),
        "table_prefix": os.environ.get("CAREERPULSE_HBASE_TABLE_PREFIX", ""),
    }


def open_hbase_store(root: Path):
    """Open the HBase store read-only (emulator) or via Thrift; None if unavailable."""
    from src.storage.hbase_store import HBaseJobStore

    settings = hbase_settings()
    if settings["mode"] == "local":
        path = Path(settings["path"])
        if not path.is_absolute():
            path = root / path
        if not path.exists():
            return None
        return HBaseJobStore.connect("local", path=path, read_only=True, table_prefix=settings["table_prefix"])
    return HBaseJobStore.connect(
        "thrift", host=settings["host"], port=settings["port"], table_prefix=settings["table_prefix"]
    )


def pivot(rows: List[Dict[str, Any]], index: str, columns: str, values: str):
    import pandas as pd

    if not rows:
        return None
    frame = pd.DataFrame(rows)
    return frame.pivot_table(index=index, columns=columns, values=values, aggfunc="sum", fill_value=0).sort_index()


def _fmt(value: Any) -> str:
    if value is None:
        return "—"
    return str(value)[:19].replace("T", " ")


def render_streaming_analytics(st, root: Path, lake_path: str) -> None:
    import pandas as pd

    st.markdown('<div class="cp-eyebrow">Kafka · Structured Streaming · HBase · Parquet · DuckDB</div>', unsafe_allow_html=True)
    st.title("Streaming analytics")
    st.markdown(
        '<div class="cp-subtitle">Live job events processed by the streaming pipeline, hiring trends, '
        'Spark SQL / RDD results and the demand model.</div>',
        unsafe_allow_html=True,
    )

    lake = Path(lake_path)
    if not lake.is_absolute():
        lake = root / lake
    auto_refresh = st.toggle("Auto-refresh every 5 seconds", value=False)

    @st.fragment(run_every="5s" if auto_refresh else None)
    def live_section() -> None:
        queries = LakeQueries(lake)
        try:
            _render_live(st, pd, queries, lake)
        finally:
            queries.close()

    live_section()

    queries = LakeQueries(lake)
    try:
        _render_spark_outputs(st, pd, queries)
    finally:
        queries.close()
    _render_hbase(st, pd, root)


def _render_live(st, pd, queries: LakeQueries, lake: Path) -> None:
    status = queries.pipeline_status()
    if not queries.has_events():
        st.info(
            f"No processed events in `{lake}` yet. Start the streaming job "
            "(`docker compose --profile pipeline up -d --build`) and the producer "
            "(`python -m src.streaming.kafka_producer --stream`), or run "
            "`python scripts/run_pipeline.py` for a local batch."
        )
        return

    kpis = queries.kpis(recent_minutes=5)
    totals = (status or {}).get("totals", {})
    cols = st.columns(6)
    cols[0].metric("New jobs · last 5 min", f"{kpis['new_recent']:,}", help="NEW postings the pipeline processed in the last 5 minutes (wall clock).")
    cols[1].metric("Jobs tracked", f"{kpis['jobs_tracked']:,}")
    cols[2].metric("Modified", f"{kpis['modified']:,}")
    cols[3].metric("Closed", f"{kpis['closed']:,}")
    cols[4].metric("Duplicates", f"{kpis['duplicates']:,}")
    cols[5].metric("Rejected by validation", f"{int(totals.get('INVALID', 0)):,}")
    if status:
        st.caption(
            f"Last micro-batch #{status.get('batch_id')} from {status.get('source')} at {_fmt(status.get('updated_at'))} UTC · "
            f"processed {status.get('processed')} · rejected {status.get('rejected')} · "
            f"latest event time {_fmt(kpis.get('latest_event'))} UTC"
        )

    alerts = queries.alerts(20)
    st.markdown("### Alerts · monitoring & notification")
    if alerts:
        critical = sum(1 for alert in alerts if alert["severity"] == "critical")
        st.caption(
            f"{len(alerts)} most recent alerts ({critical} critical). Also published to the `job-alerts` Kafka "
            "topic (tail it with `python -m src.streaming.alert_consumer`) and to the optional webhook."
        )
        for alert in alerts[:3]:
            show = st.error if alert["severity"] == "critical" else st.warning
            show(f"**{alert['alert_type']}** · {alert['message']} · {_fmt(alert['window_start'] or alert['created_at'])} UTC")
        st.dataframe(
            [{
                "Raised (UTC)": _fmt(alert["created_at"]),
                "Type": alert["alert_type"],
                "Severity": alert["severity"],
                "Subject": alert["subject"],
                "Observed": alert["observed"],
                "Threshold": alert["threshold"],
                "Baseline": alert["baseline"],
                "Message": alert["message"],
            } for alert in alerts],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("No alerts: no hiring surges or data-quality problems detected.")

    st.markdown("### Live event feed")
    recent = queries.recent_events(40)
    st.dataframe(
        [{
            "Detected": row["event"],
            "Claimed": row["claimed_event"],
            "Job": row["job_id"],
            "Company": row["company"],
            "Role": row["title"],
            "Category": row["category"],
            "Location": row["location"],
            "Salary": row["salary"],
            "Changed": row["changed_fields"] or "",
            "Event time (UTC)": _fmt(row["event_time"]),
        } for row in recent],
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("### Streaming windows")
    windows = queries.window_counts()
    chart = pivot(windows, "window_start", "event", "count")
    if chart is not None:
        st.caption("Sliding event-time windows (watermarked) computed by Spark Structured Streaming.")
        st.line_chart(chart)
        with_distinct = [row for row in windows if row.get("unique_jobs") is not None]
        if with_distinct:
            st.markdown("#### Distinct jobs per window")
            st.caption("HyperLogLog++ estimates (`approx_count_distinct`, ~2% relative error); exact distinct counts are not supported in streaming aggregations.")
            st.line_chart(pivot(with_distinct, "window_start", "event", "unique_jobs"))
    else:
        st.info("Window aggregates appear once the Structured Streaming job has emitted windows.")

    st.markdown("### Category metrics · last 5 minutes")
    latest = queries.latest_category_window()
    if latest:
        st.caption(
            f"Sliding window {_fmt(latest[0]['window_start'])}–{_fmt(latest[0]['window_end'])[11:]} UTC: "
            "new postings, closures, average advertised salary and active companies per category."
        )
        st.dataframe(
            [{
                "Category": row["category"],
                "New postings": row["new_postings"],
                "Closed": row["closed_postings"],
                "Events": row["events"],
                "Avg salary": row["avg_salary"],
                "Active companies": row["active_companies"],
            } for row in latest],
            use_container_width=True,
            hide_index=True,
        )
        category_chart = pivot(queries.category_windows(), "window_start", "category", "new_postings")
        if category_chart is not None:
            st.line_chart(category_chart)
    else:
        st.info("Category window metrics appear once windows have been computed.")

    left, right = st.columns(2)
    with left:
        st.markdown("#### Top companies by new postings")
        top = queries.top_companies(10)
        if top:
            st.bar_chart(pd.DataFrame(top).set_index("company"))
    with right:
        st.markdown("#### Average salary by category")
        salary = queries.avg_salary_by_category()
        if salary:
            st.bar_chart(pd.DataFrame(salary).set_index("category")[["avg_salary"]])

    skills = queries.top_skills(15)
    modes = queries.work_mode_mix()
    if skills or modes:
        left, right = st.columns([1.4, 1])
        with left:
            st.markdown("#### Most requested skills")
            st.caption("Declared skills plus skills extracted from free-text descriptions.")
            if skills:
                st.bar_chart(pd.DataFrame(skills).set_index("skill"))
        with right:
            st.markdown("#### Work mode of new postings")
            if modes:
                st.bar_chart(pd.DataFrame(modes).set_index("work_mode"))

    st.markdown("### Hiring trends")
    dim_col, bucket_col = st.columns(2)
    dimension = dim_col.selectbox("Dimension", ["category", "company", "location"], key="cp_trend_dim")
    bucket = bucket_col.selectbox("Time bucket", ["hour", "minute", "day"], key="cp_trend_bucket")
    trend = pivot(queries.trend(dimension, bucket), "bucket", "label", "new_postings")
    if trend is not None:
        st.line_chart(trend)
    st.dataframe(queries.breakdown(dimension), use_container_width=True, hide_index=True)

    archive = queries.raw_archive_stats()
    st.caption(
        f"Cold storage: {archive['messages']:,} raw messages archived in {archive['files']:,} Parquet files "
        f"(every message as received, valid or not)."
    )
    quarantine = queries.quarantine_summary()
    with st.expander(f"Validation quarantine · {sum(row['records'] for row in quarantine):,} rejected records"):
        if quarantine:
            st.dataframe(quarantine, use_container_width=True, hide_index=True)
            st.dataframe(queries.quarantine_samples(), use_container_width=True, hide_index=True)
        else:
            st.write("No records have failed validation.")


def _render_spark_outputs(st, pd, queries: LakeQueries) -> None:
    st.markdown("### Spark batch analytics")
    summary = queries.analytics_summary()
    if not summary:
        st.info("Run `python -m src.analytics.run_analytics` (or the `spark-analytics` container) to compute Spark SQL, RDD and ML results.")
        return
    st.caption(f"Computed {_fmt(summary.get('generated_at'))} UTC over {summary.get('events_analyzed', 0):,} processed events.")

    left, right = st.columns(2)
    with left:
        st.markdown("#### Top-k trending companies · Spark RDD")
        trending = queries.analytics_table("rdd_trending_companies")
        if trending:
            st.dataframe(trending, use_container_width=True, hide_index=True)
            st.caption(f"Momentum = NEW postings in the latest {summary.get('trend_window_minutes')} min minus the window before (map → filter → reduceByKey → takeOrdered).")
    with right:
        st.markdown("#### Top-k companies · Spark RDD")
        st.dataframe(queries.analytics_table("rdd_top_companies"), use_container_width=True, hide_index=True)

    st.markdown("#### Job demand forecast · Spark MLlib")
    model = summary.get("demand_model") or {}
    if model.get("status") == "trained":
        cols = st.columns(4)
        cols[0].metric("Test RMSE", model.get("rmse"))
        cols[1].metric("Naive baseline RMSE", model.get("baseline_rmse"))
        cols[2].metric("MAE", model.get("mae"))
        cols[3].metric("R²", model.get("r2"))
        st.caption(f"{model.get('model')} · target: {model.get('target')} · trained on {model.get('training_rows')} rows, tested on {model.get('test_rows')}.")
        predictions = queries.analytics_table("demand_predictions")
        if predictions:
            st.bar_chart(pd.DataFrame(predictions).set_index("category")[["last_bucket_new", "predicted_next_new"]])
    elif model:
        st.warning(model.get("message") or "The demand model was not trained.")

    duplicates = summary.get("near_duplicates") or {}
    if duplicates:
        st.markdown("#### Near-duplicate postings · MinHash LSH")
        st.caption(
            f"{duplicates.get('pairs_found', 0):,} pairs across {duplicates.get('jobs_involved', 0):,} of "
            f"{duplicates.get('open_postings', 0):,} open postings with Jaccard similarity ≥ {duplicates.get('min_similarity')} "
            f"on character 3-grams ({duplicates.get('num_hash_tables')} hash tables; exact similarity is computed for every candidate)."
        )
        pairs = queries.analytics_table("near_duplicate_postings")
        if pairs:
            st.dataframe(pairs, use_container_width=True, hide_index=True)

    quality = queries.analytics_table("data_quality_daily")
    if quality:
        st.markdown("#### Data quality by day · batch job over the raw archive")
        st.caption("The cold-storage archive replayed through the same Spark ETL rules as the stream.")
        st.dataframe(quality, use_container_width=True, hide_index=True)

    with st.expander("Spark SQL hiring-trend tables"):
        for name in ("event_counts", "recent_new_jobs", "top_companies_new", "avg_salary_by_category",
                     "modified_closed_by_company", "hiring_by_category_location", "top_skills",
                     "skills_by_category", "avg_salary_by_skill", "work_mode_mix"):
            st.markdown(f"**{name}**")
            st.dataframe(queries.analytics_table(name), use_container_width=True, hide_index=True)


def _render_hbase(st, pd, root: Path) -> None:
    st.markdown("### HBase job history")
    settings = hbase_settings()
    try:
        store = open_hbase_store(root)
    except Exception as exc:
        st.warning(f"HBase is not reachable ({settings['mode']}): {exc}")
        return
    if store is None:
        st.info("No HBase data yet. The pipeline creates it on its first micro-batch.")
        return
    try:
        st.caption(
            "Row key = job_id · `info:*` current state (versioned cells) · `events:<epoch_ms>#<event_id>` wide-row history and replay ledger · "
            f"backend: {'HBase Thrift ' + settings['host'] + ':' + str(settings['port']) if settings['mode'] == 'thrift' else 'embedded HBase emulator'}"
        )
        stats = store.company_stats()
        if stats:
            st.markdown("#### Company counters (`company_stats`, HBase atomic counters)")
            st.dataframe(pd.DataFrame(stats).fillna(0), use_container_width=True, hide_index=True)
        job_id = st.text_input("Look up a job_id", placeholder="e.g. J111001", key="cp_hbase_job").strip()
        if job_id:
            state: Optional[Dict[str, Any]] = store.get_job(job_id)
            if state is None:
                st.warning(f"No row for {html.escape(job_id)}.")
            else:
                left, right = st.columns([1, 1.4])
                with left:
                    st.markdown("**Current state (`info`)**")
                    st.json(state)
                    st.markdown("**`info:salary` cell versions**")
                    st.dataframe(store.field_versions(job_id, "salary"), use_container_width=True, hide_index=True)
                with right:
                    st.markdown("**Lifecycle history (`events`)**")
                    st.dataframe(store.get_history(job_id), use_container_width=True, hide_index=True)
    finally:
        store.close()
