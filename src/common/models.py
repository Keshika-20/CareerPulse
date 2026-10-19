from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

REQUIRED_FIELDS = {
    "job_id",
    "company",
    "title",
    "location",
    "experience",
    "salary",
    "timestamp",
    "event",
}

VALID_EVENTS = {"NEW", "MODIFIED", "CLOSED", "DUPLICATE"}

# Fields whose change makes a posting MODIFIED; timestamps and source metadata do not.
TRACKED_FIELDS = ("company", "title", "location", "experience", "salary")

# Ordered (category, pattern) rules applied to job titles. The first match wins, so
# specific families (ML, data) are listed before the generic engineering rule. The
# patterns are also compiled into Spark `rlike` expressions, so they must stay
# compatible with both Python `re` and Java regular expressions.
CATEGORY_RULES = (
    ("AI / ML", r"\b(machine learning|ml|ai|deep learning|data scientist|nlp|computer vision)\b"),
    ("Data", r"\b(data|analytics|analyst|bi|business intelligence|etl)\b"),
    ("DevOps / Cloud", r"\b(devops|cloud|sre|site reliability|infrastructure|platform|kubernetes)\b"),
    ("Security", r"\b(security|secops|appsec)\b"),
    ("Quality", r"\b(qa|quality|test|tester|sdet)\b"),
    ("Product", r"\b(product manager|product owner|program manager|project manager)\b"),
    ("Design", r"\b(designer|design|ux|ui)\b"),
    ("Engineering", r"\b(engineer|engineering|developer|software|backend|frontend|full[ -]?stack|mobile|android|ios)\b"),
    ("Sales & Marketing", r"\b(sales|account executive|marketing|growth|business development)\b"),
)
DEFAULT_CATEGORY = "Other"
_COMPILED_CATEGORY_RULES = tuple(
    (category, re.compile(pattern, re.IGNORECASE)) for category, pattern in CATEGORY_RULES
)


def derive_category(title: Any) -> str:
    """Map a free-text job title to a coarse hiring category."""
    text = str(title or "")
    for category, pattern in _COMPILED_CATEGORY_RULES:
        if pattern.search(text):
            return category
    return DEFAULT_CATEGORY


# Skills recognised in free-text job descriptions (canonical name -> pattern).
# Like CATEGORY_RULES, the patterns must work in both Python and Java regex.
SKILL_PATTERNS = (
    ("python", r"\bpython\b"),
    ("java", r"\bjava\b"),
    ("scala", r"\bscala\b"),
    ("sql", r"\bsql\b"),
    ("spark", r"\b(apache )?spark\b"),
    ("kafka", r"\bkafka\b"),
    ("hadoop", r"\bhadoop\b"),
    ("airflow", r"\bairflow\b"),
    ("aws", r"\b(aws|amazon web services)\b"),
    ("azure", r"\bazure\b"),
    ("gcp", r"\b(gcp|google cloud)\b"),
    ("docker", r"\bdocker\b"),
    ("kubernetes", r"\b(kubernetes|k8s)\b"),
    ("terraform", r"\bterraform\b"),
    ("linux", r"\blinux\b"),
    ("javascript", r"\bjavascript\b"),
    ("typescript", r"\btypescript\b"),
    ("react", r"\breact\b"),
    ("node.js", r"\bnode(\.js)?\b"),
    ("go", r"\b(golang|go lang)\b"),
    ("machine learning", r"\bmachine learning\b"),
    ("pytorch", r"\bpytorch\b"),
    ("tensorflow", r"\btensorflow\b"),
    ("nlp", r"\bnlp\b"),
    ("tableau", r"\btableau\b"),
    ("power bi", r"\bpower ?bi\b"),
    ("excel", r"\bexcel\b"),
    ("selenium", r"\bselenium\b"),
    ("figma", r"\bfigma\b"),
    ("ci/cd", r"\bci/cd\b"),
)
SKILL_ALIASES = {
    "py": "python",
    "k8s": "kubernetes",
    "js": "javascript",
    "ts": "typescript",
    "node": "node.js",
    "nodejs": "node.js",
    "golang": "go",
    "ml": "machine learning",
    "powerbi": "power bi",
    "amazon web services": "aws",
    "google cloud": "gcp",
    "apache spark": "spark",
    "pyspark": "spark",
}
EMPLOYMENT_TYPES = {
    "fulltime": "Full-time",
    "ft": "Full-time",
    "permanent": "Full-time",
    "parttime": "Part-time",
    "pt": "Part-time",
    "contract": "Contract",
    "contractor": "Contract",
    "temporary": "Contract",
    "intern": "Internship",
    "internship": "Internship",
}
WORK_MODES = {
    "remote": "Remote",
    "wfh": "Remote",
    "workfromhome": "Remote",
    "hybrid": "Hybrid",
    "onsite": "Onsite",
    "inoffice": "Onsite",
    "office": "Onsite",
}
_COMPILED_SKILLS = tuple((skill, re.compile(pattern, re.IGNORECASE)) for skill, pattern in SKILL_PATTERNS)
_LETTERS_ONLY = re.compile(r"[^a-z]")


def _canonical(value: Any, mapping: Dict[str, str]) -> Optional[str]:
    if value is None or not str(value).strip():
        return None
    return mapping.get(_LETTERS_ONLY.sub("", str(value).lower()), "Other")


def standardize_employment_type(value: Any) -> Optional[str]:
    return _canonical(value, EMPLOYMENT_TYPES)


def standardize_work_mode(value: Any, location: Any = None) -> Optional[str]:
    mode = _canonical(value, WORK_MODES)
    if mode is None and str(location or "").strip().lower() == "remote":
        return "Remote"
    return mode


def normalize_skill(value: Any) -> Optional[str]:
    if value is None or isinstance(value, (dict, list)):
        return None
    # str(True).lower() == "true", matching Spark's string coercion of JSON booleans.
    text = re.sub(r"\s+", " ", str(value)).strip().lower()
    if not text:
        return None
    return SKILL_ALIASES.get(text, text)


def extract_skills(description: Any) -> list:
    """Canonical skills mentioned in unstructured description text."""
    text = str(description or "")
    return [skill for skill, pattern in _COMPILED_SKILLS if pattern.search(text)]


def merge_skills(declared: Any, description: Any) -> list:
    """Declared skills (normalised, aliases resolved) plus skills found in the text."""
    declared_list = declared if isinstance(declared, list) else []
    skills = {skill for skill in (normalize_skill(item) for item in declared_list) if skill}
    skills.update(extract_skills(description))
    return sorted(skills)


def job_fingerprint(record: Dict[str, Any]) -> str:
    """Stable hash of the tracked job details, used to spot duplicate postings."""
    values = {field: record.get(field) for field in TRACKED_FIELDS}
    if values["salary"] is not None:
        values["salary"] = float(values["salary"])
    encoded = json.dumps(values, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


@dataclass
class JobEvent:
    job_id: str
    company: str
    title: str
    location: str
    experience: Optional[str]
    salary: Optional[float]
    timestamp: str
    event: str

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "JobEvent":
        clean = defaultdict_to_dict(payload)
        return cls(
            job_id=str(clean["job_id"]).strip(),
            company=str(clean["company"]).strip(),
            title=str(clean["title"]).strip(),
            location=str(clean["location"]).strip(),
            experience=(
                str(clean["experience"]).strip()
                if clean["experience"] is not None
                else None
            ),
            salary=float(clean["salary"]) if clean["salary"] is not None else None,
            timestamp=str(clean["timestamp"]),
            event=str(clean["event"]).upper(),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "company": self.company,
            "title": self.title,
            "location": self.location,
            "experience": self.experience,
            "salary": self.salary,
            "timestamp": self.timestamp,
            "event": self.event,
        }


def defaultdict_to_dict(value: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(value, dict):
        return {str(k): v for k, v in value.items()}
    raise TypeError("Job event payload must be a dictionary.")


def normalize_timestamp(timestamp: str) -> str:
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except ValueError:
        return timestamp


def validate_record(record: Dict[str, Any]) -> Tuple[bool, str | None]:
    if not isinstance(record, dict):
        return False, "Record must be a dictionary."

    nullable_fields = {"experience", "salary"}
    missing = sorted(
        field
        for field in REQUIRED_FIELDS
        if field not in record
        or record[field] == ""
        or (record[field] is None and field not in nullable_fields)
    )
    if missing:
        return False, f"Missing required fields: {', '.join(missing)}"

    for field in ("job_id", "company", "title", "location", "event"):
        if not isinstance(record[field], str):
            return False, f"Field '{field}' must be a string."

    if record["experience"] is not None and not isinstance(record["experience"], str):
        return False, "Field 'experience' must be a string or None."

    salary = record["salary"]
    if salary is not None:
        if isinstance(salary, bool) or not isinstance(salary, (int, float)):
            return False, "Field 'salary' must be a non-negative finite number or None."
        try:
            valid_salary = math.isfinite(salary) and salary >= 0
        except OverflowError:
            valid_salary = False
        if not valid_salary:
            return False, "Field 'salary' must be a non-negative finite number or None."

    if record["event"].upper() not in VALID_EVENTS:
        return False, "Event type must be one of NEW, MODIFIED, CLOSED, DUPLICATE."

    return True, None
