"""
part1_data_quality.py
---------------------
Exploratory Data Quality Analysis for the PII Detection & Data Quality
Validation Pipeline project.

Profiles the raw customer CSV for:
  - Completeness (missing value percentages)
  - Data type correctness
  - Phone and date format diversity
  - customer_id uniqueness
  - Invalid values (literal strings, out-of-range, future dates)
  - account_status categorical validity
  - Severity classification of all issues

Exposes run_quality_analysis(df) for import by the pipeline orchestrator,
and can also be executed standalone via __main__.
"""

import re
import os
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Tuple, Any


# ---------------------------------------------------------------------------
# Expected schema metadata
# ---------------------------------------------------------------------------
EXPECTED_TYPES: Dict[str, Tuple[str, str]] = {
    "customer_id":    ("int64",       "INT"),
    "first_name":     ("object",      "STRING"),
    "last_name":      ("object",      "STRING"),
    "email":          ("object",      "STRING"),
    "phone":          ("object",      "STRING"),
    "date_of_birth":  ("datetime64",  "DATE"),
    "address":        ("object",      "STRING"),
    "income":         ("float64",     "NUMERIC"),
    "account_status": ("object",      "STRING"),
    "created_date":   ("datetime64",  "DATE"),
}

VALID_ACCOUNT_STATUSES = {"active", "inactive", "suspended"}

PHONE_PATTERNS: Dict[str, str] = {
    "XXX-XXX-XXXX":      r"^\d{3}-\d{3}-\d{4}$",
    "(XXX) XXX-XXXX":    r"^\(\d{3}\) \d{3}-\d{4}$",
    "XXX.XXX.XXXX":      r"^\d{3}\.\d{3}\.\d{4}$",
    "XXXXXXXXXX":        r"^\d{10}$",
    "+1-XXX-XXX-XXXX":   r"^\+1-\d{3}-\d{3}-\d{4}$",
}

DATE_PATTERNS: Dict[str, str] = {
    "YYYY-MM-DD":            r"^\d{4}-\d{2}-\d{2}$",
    "MM/DD/YYYY":            r"^\d{2}/\d{2}/\d{4}$",
    "invalid_date (literal)": r"^invalid_date$",
}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _is_blank(val: Any) -> bool:
    """Return True if val is NaN or an empty/whitespace-only string."""
    if pd.isna(val):
        return True
    return str(val).strip() == ""


def check_completeness(df: pd.DataFrame) -> Dict[str, Dict]:
    """
    Calculate completeness for every column.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data loaded as strings (dtype=object).

    Returns
    -------
    dict
        Mapping column -> {total, missing, complete_pct}.
    """
    total = len(df)
    results: Dict[str, Dict] = {}
    for col in df.columns:
        missing = int(df[col].apply(_is_blank).sum())
        pct = round((total - missing) / total * 100, 1)
        results[col] = {"total": total, "missing": missing, "complete_pct": pct}
    return results


def check_data_types(df: pd.DataFrame) -> Dict[str, Dict]:
    """
    Compare detected pandas dtype against the expected schema type.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data.

    Returns
    -------
    dict
        Mapping column -> {detected, expected, correct, note}.
    """
    results: Dict[str, Dict] = {}
    for col in df.columns:
        detected = str(df[col].dtype)
        expected_dtype, expected_label = EXPECTED_TYPES.get(col, ("object", "UNKNOWN"))
        # Broad match: int64 starts with 'int', float64 starts with 'float', etc.
        correct = detected.startswith(expected_dtype.split("6")[0].split("[")[0])
        results[col] = {
            "detected": detected,
            "expected": expected_label,
            "correct": correct,
            "note": "" if correct else f"should be {expected_label}",
        }
    return results


def detect_phone_formats(df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Scan the phone column and classify each value by format.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    dict
        Mapping format_name -> list of example values.
    """
    format_map: Dict[str, List[str]] = {}
    for val in df["phone"].dropna():
        v = str(val).strip()
        if not v:
            continue
        matched = False
        for name, pattern in PHONE_PATTERNS.items():
            if re.match(pattern, v):
                format_map.setdefault(name, []).append(v)
                matched = True
                break
        if not matched:
            format_map.setdefault("Other / Unrecognised", []).append(v)
    return format_map


def detect_date_formats(df: pd.DataFrame) -> Dict[str, Dict[str, List[str]]]:
    """
    Scan date_of_birth and created_date columns for format diversity.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    dict
        Mapping column -> {format_name -> [example values]}.
    """
    date_cols = ["date_of_birth", "created_date"]
    results: Dict[str, Dict[str, List[str]]] = {}
    for col in date_cols:
        if col not in df.columns:
            continue
        col_map: Dict[str, List[str]] = {}
        for val in df[col].dropna():
            v = str(val).strip()
            if not v:
                continue
            matched = False
            for name, pattern in DATE_PATTERNS.items():
                if re.match(pattern, v, re.IGNORECASE):
                    col_map.setdefault(name, []).append(v)
                    matched = True
                    break
            if not matched:
                col_map.setdefault("Other / Unparseable", []).append(v)
        results[col] = col_map
    return results


def check_uniqueness(df: pd.DataFrame) -> Dict:
    """
    Check whether customer_id is unique across the dataset.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    dict
        is_unique, duplicate count, and list of duplicated id rows.
    """
    ids = df["customer_id"].dropna().astype(str)
    dupe_mask = df["customer_id"].astype(str).duplicated(keep=False)
    dupes = df[dupe_mask][["customer_id"]].copy()
    dupes.index = dupes.index + 2  # 1-based row number for readability
    return {
        "is_unique": not dupe_mask.any(),
        "duplicate_count": int(dupe_mask.sum()),
        "duplicated_rows": dupes.to_dict("records"),
        "duplicated_ids": list(df[dupe_mask]["customer_id"].unique()),
    }


def check_invalid_values(df: pd.DataFrame) -> List[Dict]:
    """
    Detect specific invalid-value conditions in the dataset.

    Checks performed:
    - Literal "invalid_date" strings in date columns
    - Negative income values
    - Income exceeding $10 million
    - Dates of birth implying age > 150 years
    - Future created_date values

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    list of dicts, each describing one issue instance.
    """
    issues: List[Dict] = []
    today = date.today()

    # Literal "invalid_date" in date columns
    for col in ["date_of_birth", "created_date"]:
        if col in df.columns:
            for idx, val in df[col].items():
                if str(val).strip().lower() == "invalid_date":
                    issues.append({
                        "type": "invalid_date_string",
                        "column": col,
                        "row": int(idx) + 2,
                        "value": val,
                        "severity": "Critical",
                    })

    # Income checks
    for idx, val in df["income"].items():
        v = str(val).strip()
        if not v:
            continue
        try:
            num = float(v)
        except ValueError:
            issues.append({
                "type": "non_numeric_income",
                "column": "income",
                "row": int(idx) + 2,
                "value": val,
                "severity": "Critical",
            })
            continue
        if num < 0:
            issues.append({
                "type": "negative_income",
                "column": "income",
                "row": int(idx) + 2,
                "value": val,
                "severity": "High",
            })
        if num > 10_000_000:
            issues.append({
                "type": "income_exceeds_10M",
                "column": "income",
                "row": int(idx) + 2,
                "value": val,
                "severity": "Medium",
            })

    # date_of_birth: age > 150
    for idx, val in df["date_of_birth"].items():
        v = str(val).strip()
        if not v or v.lower() == "invalid_date":
            continue
        try:
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
                try:
                    dob = datetime.strptime(v, fmt).date()
                    break
                except ValueError:
                    dob = None
            if dob:
                age = (today - dob).days / 365.25
                if age > 150:
                    issues.append({
                        "type": "extreme_age",
                        "column": "date_of_birth",
                        "row": int(idx) + 2,
                        "value": val,
                        "severity": "High",
                        "age_years": round(age, 1),
                    })
        except Exception:
            pass

    # created_date: future dates
    for idx, val in df["created_date"].items():
        v = str(val).strip()
        if not v or v.lower() == "invalid_date":
            continue
        try:
            for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
                try:
                    cd = datetime.strptime(v, fmt).date()
                    break
                except ValueError:
                    cd = None
            if cd and cd > today:
                issues.append({
                    "type": "future_created_date",
                    "column": "created_date",
                    "row": int(idx) + 2,
                    "value": val,
                    "severity": "Medium",
                })
        except Exception:
            pass

    return issues


def check_account_status(df: pd.DataFrame) -> List[Dict]:
    """
    Verify account_status contains only allowed values.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    list of dicts for each invalid value found.
    """
    issues: List[Dict] = []
    for idx, val in df["account_status"].items():
        v = str(val).strip().lower()
        if not v or pd.isna(val):
            continue  # nulls handled by completeness check
        if v not in VALID_ACCOUNT_STATUSES:
            issues.append({
                "type": "invalid_account_status",
                "column": "account_status",
                "row": int(idx) + 2,
                "value": val,
                "severity": "High",
            })
    return issues


def check_name_casing(df: pd.DataFrame) -> List[Dict]:
    """
    Detect names that are fully uppercase or fully lowercase.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    list of dicts for each casing issue.
    """
    issues: List[Dict] = []
    for col in ["first_name", "last_name"]:
        for idx, val in df[col].items():
            v = str(val).strip()
            if not v:
                continue
            if v.isupper() and len(v) > 1:
                issues.append({
                    "type": "name_all_caps",
                    "column": col,
                    "row": int(idx) + 2,
                    "value": val,
                    "severity": "Medium",
                })
            elif v.islower() and len(v) > 1:
                issues.append({
                    "type": "name_all_lower",
                    "column": col,
                    "row": int(idx) + 2,
                    "value": val,
                    "severity": "Medium",
                })
    return issues


def build_report(
    completeness: Dict,
    type_info: Dict,
    phone_formats: Dict,
    date_formats: Dict,
    uniqueness: Dict,
    invalid_vals: List[Dict],
    status_issues: List[Dict],
    name_issues: List[Dict],
    df: pd.DataFrame,
) -> str:
    """
    Assemble all analysis results into a human-readable report string.

    Parameters
    ----------
    completeness : dict
    type_info : dict
    phone_formats : dict
    date_formats : dict
    uniqueness : dict
    invalid_vals : list
    status_issues : list
    name_issues : list
    df : pd.DataFrame

    Returns
    -------
    str
        Formatted DATA QUALITY PROFILE REPORT.
    """
    lines: List[str] = []
    lines.append("DATA QUALITY PROFILE REPORT")
    lines.append("============================")
    lines.append("")

    # --- COMPLETENESS ---
    lines.append("COMPLETENESS:")
    for col, info in completeness.items():
        pct = info["complete_pct"]
        miss = info["missing"]
        marker = "[OK]" if miss == 0 else "[FAIL]"
        note = f"({miss} missing)" if miss > 0 else "(no missing values)"
        lines.append(f"  - {col}: {pct}% {note} {marker}")
    lines.append("")

    # --- DATA TYPES ---
    lines.append("DATA TYPES:")
    for col, info in type_info.items():
        mark = "[OK]" if info["correct"] else "[FAIL]"
        note = f"  [{info['note']}]" if info["note"] else ""
        lines.append(
            f"  - {col}: {info['detected'].upper()} {mark}{note}"
        )
    lines.append("")

    # --- FORMAT ISSUES ---
    lines.append("FORMAT ISSUES:")
    lines.append("  Phone formats detected:")
    for fmt, examples in phone_formats.items():
        ex = examples[:2]
        lines.append(f"    - {fmt}: {len(examples)} row(s) — e.g. {ex}")
    lines.append("")
    for col, fmt_map in date_formats.items():
        lines.append(f"  Date formats in '{col}':")
        for fmt, examples in fmt_map.items():
            ex = examples[:2]
            lines.append(f"    - {fmt}: {len(examples)} row(s) — e.g. {ex}")
    lines.append("")

    # --- UNIQUENESS ---
    lines.append("UNIQUENESS:")
    if uniqueness["is_unique"]:
        lines.append("  - customer_id: UNIQUE [OK]")
    else:
        lines.append(
            f"  - customer_id: NOT UNIQUE [FAIL] "
            f"({uniqueness['duplicate_count']} duplicate row(s) found)"
        )
        lines.append(f"    Duplicated IDs: {uniqueness['duplicated_ids']}")
        lines.append(f"    Affected rows: {uniqueness['duplicated_rows']}")
    lines.append("")

    # --- QUALITY ISSUES ---
    all_issues = invalid_vals + status_issues + name_issues
    # Phone format issues (non-standard)
    for fmt, examples in phone_formats.items():
        if fmt not in ("XXX-XXX-XXXX",):
            for ex in examples:
                row_mask = df["phone"].astype(str).str.strip() == ex
                row_nums = [i + 2 for i in df.index[row_mask].tolist()]
                all_issues.append({
                    "type": "non_standard_phone_format",
                    "column": "phone",
                    "row": row_nums[0] if row_nums else "?",
                    "value": ex,
                    "severity": "Medium",
                })

    if not all_issues:
        lines.append("QUALITY ISSUES: None detected.")
    else:
        lines.append("QUALITY ISSUES:")
        for i, issue in enumerate(all_issues, 1):
            desc = issue["type"].replace("_", " ").title()
            col = issue.get("column", "?")
            row = issue.get("row", "?")
            val = issue.get("value", "?")
            sev = issue.get("severity", "?")
            extra = ""
            if "age_years" in issue:
                extra = f" (age ~{issue['age_years']} years)"
            lines.append(
                f"  {i}. [{sev}] {desc} in '{col}', "
                f"Row {row}: '{val}'{extra}"
            )
    lines.append("")

    # --- SEVERITY SUMMARY ---
    severity_buckets: Dict[str, List[str]] = {
        "Critical": [], "High": [], "Medium": []
    }
    for issue in all_issues:
        sev = issue.get("severity", "Medium")
        desc = (
            f"{issue['type'].replace('_', ' ').title()} "
            f"in '{issue.get('column', '?')}' Row {issue.get('row', '?')}"
        )
        if sev in severity_buckets:
            severity_buckets[sev].append(desc)

    lines.append("SEVERITY:")
    level_descs = {
        "Critical": "blocks processing",
        "High":     "data incorrect",
        "Medium":   "needs cleaning",
    }
    for level in ("Critical", "High", "Medium"):
        bucket = severity_buckets[level]
        level_desc = level_descs[level]
        lines.append(f"  - {level} ({level_desc}): {len(bucket)} issue(s)")
        for item in bucket:
            lines.append(f"      * {item}")
    lines.append("")

    # --- SUMMARY ---
    total_rows = len(df)
    rows_with_issues: set = set()
    for issue in all_issues:
        r = issue.get("row")
        if isinstance(r, int):
            rows_with_issues.add(r)
    # Also include rows with missing values
    miss_rows = set()
    for col, info in completeness.items():
        if info["missing"] > 0:
            blank_idx = df[df[col].apply(_is_blank)].index
            for idx in blank_idx:
                miss_rows.add(idx + 2)
    if not uniqueness["is_unique"]:
        for rec in uniqueness["duplicated_rows"]:
            pass  # already counted in issues

    all_problem_rows = rows_with_issues | miss_rows
    clean_rows = total_rows - len(all_problem_rows)

    lines.append("SUMMARY:")
    lines.append(f"  - Total rows: {total_rows}")
    lines.append(f"  - Total columns: {len(df.columns)}")
    lines.append(
        f"  - Rows with at least one issue: {len(all_problem_rows)} "
        f"({round(len(all_problem_rows)/total_rows*100, 1)}%)"
    )
    lines.append(
        f"  - Clean rows: {max(0, clean_rows)} "
        f"({round(max(0, clean_rows)/total_rows*100, 1)}%)"
    )

    return "\n".join(lines)


def run_quality_analysis(
    df: pd.DataFrame, output_dir: str = "."
) -> Tuple[str, Dict]:
    """
    Run the full data quality analysis and write data_quality_report.txt.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data loaded as strings (dtype=object).
    output_dir : str
        Directory to write the report file into.

    Returns
    -------
    (report_text, findings_dict)
        report_text : str – full formatted report
        findings_dict : dict – structured findings for downstream use
    """
    completeness = check_completeness(df)
    type_info = check_data_types(df)
    phone_formats = detect_phone_formats(df)
    date_formats = detect_date_formats(df)
    uniqueness = check_uniqueness(df)
    invalid_vals = check_invalid_values(df)
    status_issues = check_account_status(df)
    name_issues = check_name_casing(df)

    report = build_report(
        completeness, type_info, phone_formats, date_formats,
        uniqueness, invalid_vals, status_issues, name_issues, df
    )

    out_path = os.path.join(output_dir, "data_quality_report.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)

    findings = {
        "completeness": completeness,
        "type_info": type_info,
        "phone_formats": phone_formats,
        "date_formats": date_formats,
        "uniqueness": uniqueness,
        "invalid_vals": invalid_vals,
        "status_issues": status_issues,
        "name_issues": name_issues,
    }
    return report, findings


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    csv_path = sys.argv[1] if len(sys.argv) > 1 else "customers_raw.csv"
    print(f"[Part 1] Loading '{csv_path}' ...")
    raw_df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    # Replace empty strings with NaN so isna() works consistently
    raw_df.replace("", pd.NA, inplace=True)
    print(f"[Part 1] Loaded {len(raw_df)} rows × {len(raw_df.columns)} columns.")

    report_text, _ = run_quality_analysis(raw_df, output_dir=".")
    sys.stdout.buffer.write((report_text + "\n").encode("utf-8"))
    print("\n[Part 1] data_quality_report.txt written.")
