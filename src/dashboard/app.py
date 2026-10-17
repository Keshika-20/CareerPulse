from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator.job_generator import generate_batch

try:
    import streamlit as st
except ImportError:  # pragma: no cover
    st = None


DEFAULT_DB = "./data/processed/ingestion.sqlite3"
EVENT_TYPES = ("NEW", "MODIFIED", "CLOSED")


def _read_json_lines(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                try:
                    value = json.loads(line)
                    if isinstance(value, dict):
                        rows.append(value)
                except json.JSONDecodeError:
                    continue
    return rows


def load_demo_records() -> List[Dict[str, Any]]:
    """Load saved demo records, creating them only when no usable demo file exists."""
    data_path = ROOT / "data" / "processed" / "demo_jobs.jsonl"
    existing = _read_json_lines(data_path)
    if existing:
        return existing
    generated = generate_batch(count=25)
    data_path.parent.mkdir(parents=True, exist_ok=True)
    with data_path.open("w", encoding="utf-8") as handle:
        for event in generated:
            handle.write(json.dumps(event) + "\n")
    return generated


def _database_uri(path: Path) -> str:
    return path.resolve().as_uri() + "?mode=ro"


def load_ingested_dashboard_data(database_path: str | Path) -> Dict[str, Any] | None:
    """Read-only dashboard query layer for the durable SQLite ingestion store."""
    path = Path(database_path)
    if not path.exists():
        return None

    with sqlite3.connect(_database_uri(path), uri=True, timeout=10) as connection:
        connection.row_factory = sqlite3.Row
        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        required = {"job_snapshots", "lifecycle_events", "ingestion_runs"}
        if not required.issubset(tables):
            raise RuntimeError(
                f"{path} exists but is not a valid CareerPulse ingestion database."
            )

        active_jobs = [
            json.loads(row["payload_json"])
            for row in connection.execute(
                """
                SELECT payload_json FROM job_snapshots
                WHERE is_active = 1 ORDER BY last_seen_at DESC
                """
            )
        ]
        recent_events = []
        for row in connection.execute(
            """
            SELECT e.event_id, e.event_type, e.event_at, e.job_id, e.payload_json,
                   r.source, r.account_id, r.run_id
            FROM lifecycle_events e
            JOIN ingestion_runs r ON r.run_id = e.run_id
            ORDER BY e.event_id DESC LIMIT 500
            """
        ):
            try:
                payload = json.loads(row["payload_json"])
            except (TypeError, json.JSONDecodeError):
                payload = {}
            recent_events.append({
                **payload,
                "event_id": row["event_id"],
                "event": row["event_type"],
                "event_at": row["event_at"],
                "job_id": row["job_id"],
                "source": row["source"],
                "account_id": row["account_id"],
                "run_id": row["run_id"],
            })

        event_counts = {
            row["event_type"]: int(row["count"])
            for row in connection.execute(
                "SELECT event_type, COUNT(*) AS count FROM lifecycle_events GROUP BY event_type"
            )
        }
        runs = [
            dict(row)
            for row in connection.execute(
                """
                SELECT run_id, source, account_id, observed_at, fetched_count,
                       new_count, modified_count, unchanged_count, closed_count
                FROM ingestion_runs ORDER BY observed_at DESC LIMIT 100
                """
            )
        ]
        total_snapshots = int(connection.execute(
            "SELECT COUNT(*) FROM job_snapshots"
        ).fetchone()[0])
        raw_count = 0
        if "raw_snapshots" in tables:
            raw_count = int(connection.execute("SELECT COUNT(*) FROM raw_snapshots").fetchone()[0])

    latest_by_account: Dict[str, Dict[str, Any]] = {}
    for run in runs:
        key = f'{run["source"]}:{run["account_id"]}'
        latest_by_account.setdefault(key, run)
    return {
        "active_jobs": active_jobs,
        "recent_events": recent_events,
        "event_counts": event_counts,
        "runs": runs,
        "latest_by_account": list(latest_by_account.values()),
        "total_snapshots": total_snapshots,
        "raw_snapshot_count": raw_count,
    }


def format_salary(job: Dict[str, Any]) -> str:
    minimum = job.get("salary_min")
    maximum = job.get("salary_max")
    currency = job.get("salary_currency") or ""
    interval = job.get("salary_interval") or ""
    if minimum is not None and maximum is not None:
        amount = f"{minimum:,.0f}–{maximum:,.0f}"
    elif minimum is not None:
        amount = f"From {minimum:,.0f}"
    elif maximum is not None:
        amount = f"Up to {maximum:,.0f}"
    elif job.get("salary") is not None:
        amount = f"{job['salary']:,.0f}"
    else:
        return "Not disclosed"
    return " ".join(part for part in (currency, amount, f"/ {interval}" if interval else "") if part)


def _safe_date(value: Any) -> str:
    if not value:
        return "—"
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.strftime("%d %b %Y · %H:%M UTC")
    except (ValueError, TypeError):
        return str(value)


def _run_ingestion(config_path: str) -> tuple[int, str]:
    """Execute the existing ingestion service without enabling optional Spark."""
    path = Path(config_path)
    if not path.is_absolute():
        path = ROOT / path
    if not path.exists():
        return 2, f"Config file not found: {path}"
    command = [sys.executable, "-m", "src.ingestion.service", "--config", str(path)]
    if spark_batch:
        command.append("--spark-batch")
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return 124, "Ingestion exceeded the 5-minute dashboard timeout."
    output = (result.stdout or "").strip()
    errors = (result.stderr or "").strip()
    return result.returncode, output or errors or f"Process exited with code {result.returncode}."


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@400;500;600;700&display=swap');
        :root { --cp-ink: #eaf0fb; --cp-muted: #91a0b8; --cp-line: rgba(148,163,184,.17); }
        .stApp { background: radial-gradient(ellipse at 12% 0%, #17243a 0%, #0b1020 42%, #080c16 100%); color: var(--cp-ink); }
        [data-testid="stHeader"] { background: rgba(8,12,22,.78); }
        [data-testid="stSidebar"] { background: #0b1120; border-right: 1px solid var(--cp-line); }
        html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
        h1, h2, h3 { font-family: 'Space Grotesk', sans-serif !important; letter-spacing: -.035em; }
        h1 { font-size: 2.25rem !important; font-weight: 700 !important; }
        [data-testid="stMetric"] { background: linear-gradient(145deg, rgba(23,35,57,.94), rgba(14,22,38,.96)); border: 1px solid var(--cp-line); padding: 18px 20px; border-radius: 16px; box-shadow: 0 10px 35px rgba(0,0,0,.12); }
        [data-testid="stMetricLabel"] { color: #a7b5ca !important; font-size: .83rem !important; }
        [data-testid="stMetricValue"] { color: #f5f8ff !important; font-family: 'Space Grotesk', sans-serif; }
        div[data-testid="stVerticalBlockBorderWrapper"] { border-color: var(--cp-line) !important; }
        .cp-eyebrow { color: #70e1c1; text-transform: uppercase; letter-spacing: .18em; font-size: .72rem; font-weight: 700; margin-bottom: .4rem; }
        .cp-subtitle { color: #a4b1c7; font-size: 1rem; margin-top: -.7rem; margin-bottom: 1.35rem; }
        .cp-card { background: linear-gradient(145deg, rgba(22,34,55,.92), rgba(12,19,33,.96)); border: 1px solid var(--cp-line); border-radius: 16px; padding: 18px 20px; margin-bottom: 12px; }
        .cp-job-title { color: #f4f7ff; font-weight: 700; font-size: 1.03rem; margin-bottom: 5px; }
        .cp-muted { color: #94a3b8; font-size: .86rem; }
        .cp-chip { display: inline-block; border: 1px solid rgba(112,225,193,.28); background: rgba(112,225,193,.08); color: #8cebd0; padding: 3px 9px; border-radius: 999px; font-size: .72rem; margin-right: 5px; }
        .cp-brand { font-family: 'Space Grotesk', sans-serif; font-size: 1.3rem; font-weight: 700; color: #f4f7ff; }
        .cp-brand span { color: #70e1c1; }
        .cp-side-copy { color: #8493aa; font-size: .78rem; line-height: 1.5; }
        div.stButton > button { border-radius: 10px; border: 1px solid rgba(112,225,193,.3); font-weight: 700; }
        div.stButton > button[kind="primary"] { background: #70e1c1; color: #08131b; border: 0; }
        .stTextInput input, .stSelectbox div[data-baseweb="select"] > div, .stNumberInput input { background: #111a2c; }
        [data-testid="stDataFrame"] { border: 1px solid var(--cp-line); border-radius: 12px; overflow: hidden; }
        hr { border-color: var(--cp-line); }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_job_cards(jobs: List[Dict[str, Any]]) -> None:
    if not jobs:
        st.info("No jobs match these filters. Try widening your search.")
        return
    for job in jobs:
        title = job.get("title") or "Untitled role"
        company = job.get("company") or "Unknown company"
        source = str(job.get("source") or "unknown").upper()
        location = job.get("location") or "Unspecified"
        salary = format_salary(job)
        url = job.get("source_url")
        st.markdown(
            f"""
            <div class="cp-card">
              <div class="cp-job-title">{_escape(title)}</div>
              <div style="color:#b5c3d8;font-weight:600;margin-bottom:10px;">{_escape(company)}</div>
              <span class="cp-chip">{_escape(source)}</span>
              <span class="cp-muted">⌖ {_escape(location)}</span>
              <div class="cp-muted" style="margin-top:10px;">Salary: {_escape(salary)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if url and str(url).startswith(("https://", "http://")):
            st.link_button("View original posting ↗", str(url))


def _escape(value: Any) -> str:
    # Escape source-provided text before embedding it in dashboard HTML.
    import html
    return html.escape(str(value if value is not None else ""))


def _render_overview(data: Dict[str, Any], database_path: str) -> None:
    jobs = data["active_jobs"]
    events = data["event_counts"]
    companies = len({str(job.get("company", "")).strip() for job in jobs if job.get("company")})
    sources = len({job.get("source") for job in jobs if job.get("source")})
    latest_run = data["runs"][0] if data["runs"] else None

    st.markdown('<div class="cp-eyebrow">Career intelligence · Live workspace</div>', unsafe_allow_html=True)
    st.title("Your job market, in focus.")
    st.markdown('<div class="cp-subtitle">Track openings, detect changes, and monitor the data pipeline from one workspace.</div>', unsafe_allow_html=True)
    if latest_run:
        st.caption(f"Last recorded ingestion: {_safe_date(latest_run.get('observed_at'))} · Database: `{database_path}`")
    else:
        st.caption(f"Database connected · No ingestion runs recorded yet · `{database_path}`")

    metric_cols = st.columns(4)
    metric_cols[0].metric("Active openings", f"{len(jobs):,}")
    metric_cols[1].metric("Companies", f"{companies:,}")
    metric_cols[2].metric("Sources represented", f"{sources:,}")
    metric_cols[3].metric("Lifecycle events", f"{sum(events.values()):,}")

    st.markdown("### Market snapshot")
    left, right = st.columns([1.2, 1])
    with left:
        st.markdown("#### Openings by company")
        counts: Dict[str, int] = {}
        for job in jobs:
            company = str(job.get("company") or "Unknown")
            counts[company] = counts.get(company, 0) + 1
        top = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:10]
        if top:
            st.bar_chart({"Open roles": {company: count for company, count in top}})
        else:
            st.info("Ingest job boards to populate the company chart.")
    with right:
        st.markdown("#### Lifecycle activity")
        activity = {event: int(events.get(event, 0)) for event in EVENT_TYPES}
        if sum(activity.values()):
            st.bar_chart({"Events": activity})
        else:
            st.info("Lifecycle events appear after the first successful scan.")

    st.markdown("### Recently observed changes")
    recent = data["recent_events"][:8]
    if recent:
        st.dataframe(
            [{
                "Event": item.get("event"),
                "Role": item.get("title"),
                "Company": item.get("company"),
                "Source": item.get("source"),
                "Observed": _safe_date(item.get("event_at") or item.get("timestamp")),
            } for item in recent],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No lifecycle events yet.")


def _render_explorer(data: Dict[str, Any]) -> None:
    st.markdown('<div class="cp-eyebrow">Discover</div>', unsafe_allow_html=True)
    st.title("Job explorer")
    st.markdown('<div class="cp-subtitle">Search current active listings across every configured source account.</div>', unsafe_allow_html=True)
    jobs = data["active_jobs"]
    companies = sorted({str(j.get("company") or "Unknown") for j in jobs})
    sources = sorted({str(j.get("source") or "unknown") for j in jobs})
    locations = sorted({str(j.get("location") or "Unspecified") for j in jobs})
    search_col, company_col, source_col = st.columns([1.5, 1, 1])
    with search_col:
        query = st.text_input("Search roles", placeholder="e.g. software engineer, data, remote")
    with company_col:
        company_filter = st.selectbox("Company", ["All companies", *companies])
    with source_col:
        source_filter = st.selectbox("Source", ["All sources", *sources])
    location_col, sort_col = st.columns(2)
    with location_col:
        location_filter = st.selectbox("Location", ["All locations", *locations])
    with sort_col:
        sort = st.selectbox("Sort by", ["Most recently seen", "Company A–Z", "Role A–Z", "Salary: high to low"])

    filtered = []
    query_lower = query.strip().lower()
    for job in jobs:
        haystack = " ".join(str(job.get(k) or "") for k in ("title", "company", "location", "experience", "source"))
        if query_lower and query_lower not in haystack.lower():
            continue
        if company_filter != "All companies" and job.get("company") != company_filter:
            continue
        if source_filter != "All sources" and job.get("source") != source_filter:
            continue
        if location_filter != "All locations" and (job.get("location") or "Unspecified") != location_filter:
            continue
        filtered.append(job)

    if sort == "Company A–Z":
        filtered.sort(key=lambda j: str(j.get("company") or "").lower())
    elif sort == "Role A–Z":
        filtered.sort(key=lambda j: str(j.get("title") or "").lower())
    elif sort == "Salary: high to low":
        filtered.sort(key=lambda j: float(j.get("salary") or j.get("salary_max") or 0), reverse=True)
    else:
        filtered.sort(key=lambda j: str(j.get("timestamp") or ""), reverse=True)

    st.caption(f"Showing {len(filtered):,} of {len(jobs):,} active openings")
    if filtered:
        table = [{
            "Role": j.get("title"),
            "Company": j.get("company"),
            "Location": j.get("location"),
            "Source": j.get("source"),
            "Salary": format_salary(j),
            "Experience": j.get("experience") or "Not specified",
            "Last observed": _safe_date(j.get("timestamp")),
        } for j in filtered]
        st.dataframe(table, use_container_width=True, hide_index=True)
        csv = __import__("pandas").DataFrame([{
            "job_id": j.get("job_id"),
            "title": j.get("title"),
            "company": j.get("company"),
            "location": j.get("location"),
            "source": j.get("source"),
            "source_url": j.get("source_url"),
            "salary_min": j.get("salary_min"),
            "salary_max": j.get("salary_max"),
            "salary_currency": j.get("salary_currency"),
            "salary_interval": j.get("salary_interval"),
            "experience": j.get("experience"),
            "timestamp": j.get("timestamp"),
        } for j in filtered]).to_csv(index=False).encode("utf-8")
        st.download_button("Export filtered jobs as CSV", csv, "careerpulse_jobs.csv", "text/csv")
        st.markdown("### Listing details")
        options = {f'{j.get("title", "Untitled")} · {j.get("company", "Unknown")} · {j.get("source", "")}': j for j in filtered}
        selected_label = st.selectbox("Choose a listing", list(options))
        selected = options[selected_label]
        detail_left, detail_right = st.columns([2, 1])
        with detail_left:
            st.markdown(f"#### {_escape(selected.get('title'))}")
            st.write(f"**Company:** {selected.get('company') or 'Unknown'}")
            st.write(f"**Location:** {selected.get('location') or 'Unspecified'}")
            st.write(f"**Experience:** {selected.get('experience') or 'Not specified'}")
            st.write(f"**Salary:** {format_salary(selected)}")
            st.write(f"**Source updated:** {_safe_date(selected.get('source_updated_at'))}")
        with detail_right:
            st.write(f"**Source:** {selected.get('source')}")
            st.write(f"**Observed:** {_safe_date(selected.get('timestamp'))}")
            if selected.get("source_url"):
                st.link_button("Open original posting ↗", selected["source_url"])
    else:
        st.info("No active listings are available. Enable more source accounts and run ingestion.")


def _render_pipeline(data: Dict[str, Any], database_path: str, *, can_run: bool = False) -> None:
    st.markdown('<div class="cp-eyebrow">Operations</div>', unsafe_allow_html=True)
    st.title("Pipeline control")
    st.markdown('<div class="cp-subtitle">Run the public-board ingestion service and inspect persisted source-account runs.</div>', unsafe_allow_html=True)

    st.markdown("#### How the pipeline works")
    stages = st.columns(5)
    stage_info = [
        ("01 · Fetch", "Greenhouse / Lever APIs"),
        ("02 · Validate", "Schema and completeness checks"),
        ("03 · Normalize", "Shared job record"),
        ("04 · Persist", "SQLite snapshots + events"),
        ("05 · Explore", "Dashboard + optional Spark batch"),
    ]
    for col, (title, description) in zip(stages, stage_info):
        with col:
            st.markdown(f'<div class="cp-card"><div class="cp-eyebrow">{title}</div><div class="cp-muted">{description}</div></div>', unsafe_allow_html=True)

    default_config = next((p for p in (
        ROOT / "config" / "ingestion.local.json",
        ROOT / "data" / "ingestion.local.json",
        ROOT / "config" / "ingestion.example.json",
    ) if p.exists()), ROOT / "config" / "ingestion.local.json")
    st.markdown("#### Run ingestion")
    config_path = st.text_input("Configuration file", value=str(default_config.relative_to(ROOT) if default_config.is_relative_to(ROOT) else default_config))
    st.caption("Only enabled public source accounts in this configuration are queried. Pipeline execution is restricted to administrators.")
    if not can_run:
        st.info("Read-only access: ask an administrator to run ingestion or grant you the appropriate role.")
    if can_run and st.button("Run ingestion now", type="primary", use_container_width=True):
        with st.spinner("Fetching configured public job boards and applying complete scans to SQLite…"):
            return_code, output = _run_ingestion(config_path)
        st.session_state["cp_last_ingestion_output"] = output
        st.session_state["cp_last_ingestion_code"] = return_code
        if return_code == 0:
            st.success("Ingestion command completed successfully.")
        else:
            st.error(f"Ingestion command exited with code {return_code}. Review the output below.")
        st.rerun()

    if "cp_last_ingestion_output" in st.session_state:
        with st.expander("Latest command output", expanded=True):
            st.code(st.session_state["cp_last_ingestion_output"], language="json" if st.session_state.get("cp_last_ingestion_code") == 0 else "text")

    st.markdown("#### Source-account status")
    if data["latest_by_account"]:
        st.dataframe([{
            "Source": r["source"],
            "Account": r["account_id"],
            "Last scan": _safe_date(r["observed_at"]),
            "Fetched": r["fetched_count"],
            "New": r["new_count"],
            "Modified": r["modified_count"],
            "Unchanged": r["unchanged_count"],
            "Closed": r["closed_count"],
        } for r in data["latest_by_account"]], use_container_width=True, hide_index=True)
    else:
        st.info("No successful ingestion runs recorded in this database yet.")

    st.markdown("#### Recent runs")
    if data["runs"]:
        st.dataframe([{
            "Run ID": r["run_id"],
            "Source": r["source"],
            "Account": r["account_id"],
            "Observed at": _safe_date(r["observed_at"]),
            "Fetched": r["fetched_count"],
            "New": r["new_count"],
            "Modified": r["modified_count"],
            "Unchanged": r["unchanged_count"],
            "Closed": r["closed_count"],
        } for r in data["runs"]], use_container_width=True, hide_index=True)
    else:
        st.info("Run ingestion to create the first run record.")


def _render_events(data: Dict[str, Any]) -> None:
    st.markdown('<div class="cp-eyebrow">Audit trail</div>', unsafe_allow_html=True)
    st.title("Lifecycle events")
    st.markdown('<div class="cp-subtitle">A traceable history of newly discovered, changed, and closed listings.</div>', unsafe_allow_html=True)
    events = data["recent_events"]
    event_filter = st.multiselect("Event type", list(EVENT_TYPES), default=list(EVENT_TYPES))
    source_options = sorted({str(event.get("source") or "unknown") for event in events})
    source_filter = st.selectbox("Source account", ["All sources", *source_options])
    filtered = [e for e in events if e.get("event") in event_filter and (
        source_filter == "All sources" or e.get("source") == source_filter
    )]
    st.caption(f"Showing {len(filtered):,} events · Latest 500 are loaded from SQLite")
    if filtered:
        st.dataframe([{
            "Type": e.get("event"),
            "Role": e.get("title"),
            "Company": e.get("company"),
            "Location": e.get("location"),
            "Source": e.get("source"),
            "Observed at": _safe_date(e.get("event_at") or e.get("timestamp")),
            "Job ID": e.get("job_id"),
        } for e in filtered], use_container_width=True, hide_index=True)
        export = __import__("pandas").DataFrame(filtered).to_csv(index=False).encode("utf-8")
        st.download_button("Export event history as CSV", export, "careerpulse_events.csv", "text/csv")
    else:
        st.info("No events match the selected filters.")


def resolve_role(email: str, role_config: Dict[str, Any]) -> str:
    """Resolve least-privilege workspace role from explicit email allowlists."""
    normalized = email.strip().lower()
    if not normalized:
        return "viewer"
    for role in ("admin", "analyst", "viewer"):
        values = role_config.get(f"{role}_emails", [])
        if isinstance(values, (list, tuple, set)) and normalized in {
            str(value).strip().lower() for value in values
        }:
            return role
    return "viewer"


def _auth_configuration_available() -> bool:
    try:
        auth = st.secrets.get("auth", {})
        return bool(
            auth.get("redirect_uri")
            and auth.get("client_id")
            and auth.get("client_secret")
            and auth.get("server_metadata_url")
        )
    except Exception:
        return False


def _render_login_gate() -> bool:
    """Return True only when an OIDC-authenticated user may enter the workspace."""
    try:
        user = st.user
        if bool(user.is_logged_in):
            return True
    except Exception:
        user = None

    st.markdown('<div class="cp-eyebrow">Secure workspace</div>', unsafe_allow_html=True)
    st.title("CareerPulse")
    st.markdown(
        '<div class="cp-subtitle">Job-market intelligence powered by a traceable Big Data pipeline.</div>',
        unsafe_allow_html=True,
    )
    left, right = st.columns([1.15, 1])
    with left:
        st.markdown("### One workspace. A complete data story.")
        st.markdown(
            """
            - **Discover:** real public job postings from configured company boards.
            - **Understand:** lifecycle changes, company trends, and source health.
            - **Operate:** ingestion, data quality, Spark batch processing, and storage.
            """
        )
        st.caption("Access is authenticated through OpenID Connect (OIDC). Roles are assigned by administrator-managed email allowlists.")
    with right:
        st.markdown('<div class="cp-card">', unsafe_allow_html=True)
        st.markdown("#### Sign in to CareerPulse")
        if _auth_configuration_available():
            st.write("Continue with your configured identity provider.")
            if st.button("Sign in securely", type="primary", use_container_width=True):
                st.login()
        else:
            st.error("Authentication is not configured yet. The dashboard remains locked until OIDC is configured.")
            st.markdown("Create `.streamlit/secrets.toml` from `docs/authentication-and-big-data.md`, add your identity-provider credentials, then restart Streamlit.")
        st.markdown('</div>', unsafe_allow_html=True)
    return False


def _render_big_data(data: Dict[str, Any], database_path: str, *, can_run: bool = False) -> None:
    st.markdown('<div class="cp-eyebrow">Data engineering · Core platform</div>', unsafe_allow_html=True)
    st.title("Big Data operations")
    st.markdown('<div class="cp-subtitle">Trace data from source acquisition through quality gates, durable state, analytical processing, and product metrics.</div>', unsafe_allow_html=True)

    jobs = data["active_jobs"]
    runs = data["runs"]
    total_fetched = sum(int(run.get("fetched_count") or 0) for run in runs)
    total_new = sum(int(run.get("new_count") or 0) for run in runs)
    total_modified = sum(int(run.get("modified_count") or 0) for run in runs)
    raw_count = int(data.get("raw_snapshot_count") or 0)
    metrics = st.columns(4)
    metrics[0].metric("Active normalized records", f"{len(jobs):,}")
    metrics[1].metric("Recorded source runs", f"{len(runs):,}")
    metrics[2].metric("Fetched across retained runs", f"{total_fetched:,}")
    metrics[3].metric("Raw response snapshots", f"{raw_count:,}")

    st.markdown("### Reference architecture")
    stages = [
        ("01 · Source connectors", "Public Greenhouse + Lever APIs", "Configured", "#70e1c1"),
        ("02 · Quality gates", "Retries, schema checks, completeness, duplicate IDs", "Fail closed", "#70e1c1"),
        ("03 · Data contract", "Normalize into a shared job schema", "Implemented", "#70e1c1"),
        ("04 · State + events", "SQLite snapshots, raw payload retention, lifecycle events", "Implemented", "#70e1c1"),
        ("05 · Batch analytics", "Spark DataFrame aggregation + dashboard metrics", "Opt-in Spark batch", "#f6c76b"),
        ("06 · Distributed stream", "Kafka topic → consumer / Structured Streaming", "Not end-to-end wired yet", "#f6c76b"),
        ("07 · Analytical lake", "Partitioned Parquet / historical analytical tables", "Separate helper exists; ingestion export not automated", "#f6c76b"),
    ]
    for title, description, status, accent in stages:
        st.markdown(
            f'<div class="cp-card" style="display:flex;gap:16px;align-items:center;justify-content:space-between;">'
            f'<div><div class="cp-job-title">{_escape(title)}</div><div class="cp-muted">{_escape(description)}</div></div>'
            f'<div style="color:{accent};font-size:.78rem;font-weight:700;min-width:190px;text-align:right;">{_escape(status)}</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("### Processing controls")
    st.caption("Spark runs as a real batch job when selected; it is not described as continuous streaming. Running it requires a working Java + PySpark environment.")
    default_config = next((p for p in (
        ROOT / "config" / "ingestion.local.json",
        ROOT / "data" / "ingestion.local.json",
        ROOT / "config" / "ingestion.example.json",
    ) if p.exists()), ROOT / "config" / "ingestion.local.json")
    try:
        config_display = str(default_config.relative_to(ROOT))
    except ValueError:
        config_display = str(default_config)
    config_path = st.text_input("Ingestion configuration", value=config_display, key="bigdata_config_path")
    if not can_run:
        st.info("Read-only role. An administrator can execute the ingestion or Spark batch job.")
    else:
        ingest_col, spark_col = st.columns(2)
        with ingest_col:
            if st.button("Run source ingestion", type="primary", use_container_width=True, key="bigdata_ingest"):
                with st.spinner("Fetching sources, validating complete scans, and committing lifecycle changes…"):
                    code, output = _run_ingestion(config_path, spark_batch=False)
                st.session_state["cp_bigdata_output"] = output
                st.session_state["cp_bigdata_code"] = code
                st.rerun()
        with spark_col:
            if st.button("Run ingestion + Spark batch", use_container_width=True, key="bigdata_spark"):
                with st.spinner("Running ingestion and Spark DataFrame aggregation…"):
                    code, output = _run_ingestion(config_path, spark_batch=True)
                st.session_state["cp_bigdata_output"] = output
                st.session_state["cp_bigdata_code"] = code
                st.rerun()
    if "cp_bigdata_output" in st.session_state:
        if st.session_state.get("cp_bigdata_code") == 0:
            st.success("The last pipeline command completed successfully.")
        else:
            st.error(f"The last pipeline command exited with code {st.session_state.get('cp_bigdata_code')}.")
        st.code(st.session_state["cp_bigdata_output"], language="json")

    st.markdown("### Storage and processing inventory")
    storage_cols = st.columns(3)
    with storage_cols[0]:
        st.markdown("#### Operational state")
        st.write("SQLite")
        st.caption("Current active snapshots, absence counters, run summaries, and lifecycle audit history.")
        st.code(str(database_path), language="text")
    with storage_cols[1]:
        st.markdown("#### Analytical storage")
        parquet_path = ROOT / "data" / "processed" / "job_history.parquet"
        if parquet_path.exists():
            st.success(f"Parquet file found · {parquet_path.stat().st_size:,} bytes")
        else:
            st.warning("No job_history.parquet file found at the default path.")
        st.caption("The existing Parquet writer is available; public ingestion does not automatically refresh this file yet.")
    with storage_cols[2]:
        st.markdown("#### Event transport")
        st.write("Kafka producer module present")
        st.caption("The current producer is a separate utility and is not wired to source ingestion. No live broker status is inferred.")

    st.markdown("### Source throughput · retained run history")
    if runs:
        st.bar_chart({
            "Fetched": {f'{r.get("source")} · {r.get("account_id")} · {str(r.get("observed_at", ""))[:10]}': int(r.get("fetched_count") or 0) for r in reversed(runs[:30])}
        })
        st.dataframe([{
            "Source": r.get("source"), "Account": r.get("account_id"),
            "Observed": _safe_date(r.get("observed_at")), "Fetched": r.get("fetched_count"),
            "New": r.get("new_count"), "Modified": r.get("modified_count"),
            "Unchanged": r.get("unchanged_count"), "Closed": r.get("closed_count"),
        } for r in runs[:30]], use_container_width=True, hide_index=True)
    else:
        st.info("No real ingestion runs have been recorded yet. Run a configured public source to populate operational metrics.")


def render_dashboard() -> None:
    if st is None:
        print("Streamlit is not installed. Install requirements.txt to run the dashboard.")
        return
    st.set_page_config(
        page_title="CareerPulse · Job Intelligence",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _inject_styles()

    # Authentication is fail-closed: no dashboard data or controls render before OIDC login.
    if not _render_login_gate():
        return

    try:
        email = str(st.user.get("email", ""))
    except Exception:
        email = ""
    try:
        role_config = st.secrets.get("roles", {})
    except Exception:
        role_config = {}
    role = resolve_role(email, role_config)
    allowed_pages = ["Overview", "Job explorer", "Big Data operations", "Lifecycle events"]
    if role in {"admin", "analyst"}:
        allowed_pages.append("Pipeline control")

    with st.sidebar:
        st.markdown('<div class="cp-brand">Career<span>Pulse</span></div>', unsafe_allow_html=True)
        st.markdown('<div class="cp-side-copy">JOB MARKET INTELLIGENCE<br/>Big Data · Discovery · Lifecycle</div>', unsafe_allow_html=True)
        st.divider()
        st.markdown(f"**{_escape(email or 'Authenticated user')}**")
        st.caption(f"Workspace role · {role.upper()}")
        if st.button("Log out", use_container_width=True):
            st.logout()
        st.divider()
        page = st.radio("Workspace", allowed_pages, label_visibility="collapsed")
        st.divider()
        database_path = st.text_input(
            "SQLite database",
            value=os.environ.get("CAREERPULSE_SQLITE_PATH", DEFAULT_DB),
            help="Path to the SQLite database created by the ingestion service.",
        )
        st.caption("Public Greenhouse + Lever boards · OIDC access control")
        if st.button("Refresh workspace", use_container_width=True):
            st.rerun()

    resolved_db = Path(database_path)
    if not resolved_db.is_absolute():
        resolved_db = ROOT / resolved_db
    try:
        data = load_ingested_dashboard_data(resolved_db)
    except (OSError, sqlite3.Error, RuntimeError, json.JSONDecodeError) as exc:
        st.error(f"Unable to read the configured ingestion database: {exc}")
        st.stop()

    if data is None:
        st.warning("No ingestion database found. Synthetic demo records are not shown as real job listings.")
        data = {
            "active_jobs": [], "recent_events": [], "event_counts": {}, "runs": [],
            "latest_by_account": [], "total_snapshots": 0, "raw_snapshot_count": 0,
        }
        st.caption("No live source data yet · Configure public board accounts and run ingestion.")
    if page == "Overview":
        _render_overview(data, str(resolved_db))
    elif page == "Job explorer":
        _render_explorer(data)
    elif page == "Big Data operations":
        _render_big_data(data, str(resolved_db), can_run=(role == "admin"))
    elif page == "Pipeline control":
        _render_pipeline(data, str(resolved_db), can_run=(role == "admin"))
    else:
        _render_events(data)


def main() -> None:
    if st is None:
        print("Optional dependency missing: streamlit")
        return
    render_dashboard()


if __name__ == "__main__":
    main()
