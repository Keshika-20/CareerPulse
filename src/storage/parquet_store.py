from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None


def write_parquet_records(records: Iterable[Dict[str, Any]], output_path: str) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = list(records)
    if pd is not None:
        frame = pd.DataFrame(rows)
        frame.to_parquet(path, index=False)
        return path

    json_path = path.with_suffix(".jsonl")
    with json_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
    return json_path


if __name__ == "__main__":
    sample = [{"job_id": "J1", "company": "Google", "title": "Engineer", "location": "Bangalore", "experience": "0-2 years", "salary": 1200000, "timestamp": "2026-10-06T09:00:00Z", "event": "NEW"}]
    result = write_parquet_records(sample, "./data/processed/sample_jobs.parquet")
    print(result)
