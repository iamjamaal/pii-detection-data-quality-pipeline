"""
part3_validator.py
------------------
Custom data validation framework for the customer dataset.

Defines and applies validation rules for all 10 columns using only
the Python standard library and pandas. Collects every rule violation
with full context (row, column, value, rule) and produces a structured
validation_results.txt report.

Exposes run_validation(df) for import by the pipeline orchestrator,
and can also be executed standalone via __main__.
"""

import re
import os
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Tuple, Any


# ---------------------------------------------------------------------------
# Validation rules registry
# ---------------------------------------------------------------------------
# Each entry: (rule_name, checker_function_name)
# The checker receives (value, df, row_idx) and returns (passed: bool, msg: str)

EMAIL_REGEX = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)

VALID_STATUSES = {"active", "inactive", "suspended"}
NAME_REGEX = re.compile(r"^[A-Za-z\s'\-]{2,50}$")


def _parse_date(val: Any) -> Tuple[bool, Any]:
    """
    Attempt to parse val as a date using common formats.

    Parameters
    ----------
    val : any

    Returns
    -------
    (success: bool, parsed_date or None)
    """
    if pd.isna(val) or str(val).strip() == "":
        return False, None
    v = str(val).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return True, datetime.strptime(v, fmt).date()
        except ValueError:
            pass
    return False, None


# ---------------------------------------------------------------------------
# Per-column validators
# ---------------------------------------------------------------------------

def validate_customer_id(
    series: pd.Series, df: pd.DataFrame
) -> List[Dict]:
    """
    Validate customer_id: must be a positive integer, must be unique.

    Parameters
    ----------
    series : pd.Series  (customer_id column)
    df : pd.DataFrame

    Returns
    -------
    list of failure dicts with keys: row, column, value, rule.
    """
    failures: List[Dict] = []
    seen_ids: Dict[str, int] = {}

    for idx, val in series.items():
        row = int(idx) + 2
        v = str(val).strip() if not pd.isna(val) else ""

        # Must be a positive integer
        if not v:
            failures.append({
                "row": row, "column": "customer_id",
                "value": val, "rule": "Must be a positive integer (missing)"
            })
            continue
        try:
            num = int(float(v))
            if num <= 0:
                failures.append({
                    "row": row, "column": "customer_id",
                    "value": val, "rule": "Must be a positive integer (value <= 0)"
                })
                continue
        except (ValueError, TypeError):
            failures.append({
                "row": row, "column": "customer_id",
                "value": val, "rule": "Must be a positive integer (non-numeric)"
            })
            continue

        # Uniqueness check
        if v in seen_ids:
            failures.append({
                "row": row, "column": "customer_id",
                "value": val,
                "rule": f"Must be unique (duplicate of row {seen_ids[v]})"
            })
        else:
            seen_ids[v] = row

    return failures


def validate_name(series: pd.Series, col_name: str) -> List[Dict]:
    """
    Validate a name column: non-null, 2–50 chars, letters/hyphens/apostrophes.

    Parameters
    ----------
    series : pd.Series
    col_name : str

    Returns
    -------
    list of failure dicts.
    """
    failures: List[Dict] = []
    for idx, val in series.items():
        row = int(idx) + 2
        if pd.isna(val) or str(val).strip() == "":
            failures.append({
                "row": row, "column": col_name,
                "value": val, "rule": "Must be non-empty"
            })
            continue
        v = str(val).strip()
        if not (2 <= len(v) <= 50):
            failures.append({
                "row": row, "column": col_name,
                "value": val, "rule": "Length must be between 2 and 50 characters"
            })
        if not NAME_REGEX.match(v):
            failures.append({
                "row": row, "column": col_name,
                "value": val,
                "rule": "Must contain only letters, spaces, hyphens, or apostrophes"
            })
    return failures


def validate_email(series: pd.Series) -> List[Dict]:
    """
    Validate email: must match standard email format.

    Parameters
    ----------
    series : pd.Series

    Returns
    -------
    list of failure dicts.
    """
    failures: List[Dict] = []
    for idx, val in series.items():
        row = int(idx) + 2
        if pd.isna(val) or str(val).strip() == "":
            failures.append({
                "row": row, "column": "email",
                "value": val, "rule": "Must be non-empty"
            })
            continue
        v = str(val).strip()
        if not EMAIL_REGEX.match(v):
            failures.append({
                "row": row, "column": "email",
                "value": val, "rule": "Must be a valid email address format"
            })
    return failures


def validate_phone(series: pd.Series) -> List[Dict]:
    """
    Validate phone: when stripped of all non-digit chars, must be 10–15 digits.

    Parameters
    ----------
    series : pd.Series

    Returns
    -------
    list of failure dicts.
    """
    failures: List[Dict] = []
    for idx, val in series.items():
        row = int(idx) + 2
        if pd.isna(val) or str(val).strip() == "":
            failures.append({
                "row": row, "column": "phone",
                "value": val, "rule": "Must be non-empty"
            })
            continue
        digits_only = re.sub(r"\D", "", str(val))
        if not (10 <= len(digits_only) <= 15):
            failures.append({
                "row": row, "column": "phone",
                "value": val,
                "rule": f"Stripped digit count must be 10–15 (got {len(digits_only)})"
            })
    return failures


def validate_date_column(
    series: pd.Series, col_name: str, allow_future: bool = False
) -> List[Dict]:
    """
    Validate a date column: must be parseable, not in the future (unless allowed),
    and for date_of_birth the age must be 0–150 years.

    Parameters
    ----------
    series : pd.Series
    col_name : str
    allow_future : bool  – if False, future dates are flagged.

    Returns
    -------
    list of failure dicts.
    """
    failures: List[Dict] = []
    today = date.today()

    for idx, val in series.items():
        row = int(idx) + 2
        if pd.isna(val) or str(val).strip() == "":
            # Missing is OK here (completeness handles it); skip
            continue
        v = str(val).strip()

        # Literal "invalid_date"
        if v.lower() == "invalid_date":
            failures.append({
                "row": row, "column": col_name,
                "value": val, "rule": "Not a valid date (literal 'invalid_date' string)"
            })
            continue

        ok, parsed = _parse_date(v)
        if not ok:
            failures.append({
                "row": row, "column": col_name,
                "value": val, "rule": "Could not be parsed as a valid date"
            })
            continue

        if not allow_future and parsed > today:
            failures.append({
                "row": row, "column": col_name,
                "value": val, "rule": "Date must not be in the future"
            })

        # Age range for date_of_birth
        if col_name == "date_of_birth":
            age = (today - parsed).days / 365.25
            if age > 150:
                failures.append({
                    "row": row, "column": col_name,
                    "value": val,
                    "rule": f"Date of birth implies age > 150 years (~{age:.1f} years)"
                })
            elif age < 0:
                failures.append({
                    "row": row, "column": col_name,
                    "value": val, "rule": "Date of birth is in the future"
                })

    return failures


def validate_address(series: pd.Series) -> List[Dict]:
    """
    Validate address: must be non-null and non-empty string.

    Parameters
    ----------
    series : pd.Series

    Returns
    -------
    list of failure dicts.
    """
    failures: List[Dict] = []
    for idx, val in series.items():
        row = int(idx) + 2
        if pd.isna(val) or str(val).strip() == "":
            failures.append({
                "row": row, "column": "address",
                "value": val, "rule": "Must be non-empty"
            })
    return failures


def validate_income(series: pd.Series) -> List[Dict]:
    """
    Validate income: must be numeric, non-negative, and ≤ $10,000,000.

    Parameters
    ----------
    series : pd.Series

    Returns
    -------
    list of failure dicts.
    """
    failures: List[Dict] = []
    for idx, val in series.items():
        row = int(idx) + 2
        if pd.isna(val) or str(val).strip() == "":
            failures.append({
                "row": row, "column": "income",
                "value": val, "rule": "Must be non-empty numeric value"
            })
            continue
        try:
            num = float(str(val).strip())
        except (ValueError, TypeError):
            failures.append({
                "row": row, "column": "income",
                "value": val, "rule": "Must be a numeric value"
            })
            continue
        if num < 0:
            failures.append({
                "row": row, "column": "income",
                "value": val, "rule": "Income must be non-negative"
            })
        if num > 10_000_000:
            failures.append({
                "row": row, "column": "income",
                "value": val, "rule": "Income exceeds $10,000,000 upper bound"
            })
    return failures


def validate_account_status(series: pd.Series) -> List[Dict]:
    """
    Validate account_status: must be one of 'active', 'inactive', 'suspended'.

    Parameters
    ----------
    series : pd.Series

    Returns
    -------
    list of failure dicts.
    """
    failures: List[Dict] = []
    for idx, val in series.items():
        row = int(idx) + 2
        if pd.isna(val) or str(val).strip() == "":
            failures.append({
                "row": row, "column": "account_status",
                "value": val,
                "rule": "Must be one of: active, inactive, suspended (missing)"
            })
            continue
        v = str(val).strip().lower()
        if v not in VALID_STATUSES:
            failures.append({
                "row": row, "column": "account_status",
                "value": val,
                "rule": f"Must be one of: active, inactive, suspended (got '{val}')"
            })
    return failures


# ---------------------------------------------------------------------------
# Main validation runner
# ---------------------------------------------------------------------------

def run_all_validators(df: pd.DataFrame) -> Dict[str, List[Dict]]:
    """
    Run all validators and return failures grouped by column.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    dict mapping column_name -> list of failure dicts.
    """
    failures_by_col: Dict[str, List[Dict]] = {}

    checks = [
        ("customer_id",    validate_customer_id(df["customer_id"], df)),
        ("first_name",     validate_name(df["first_name"],  "first_name")),
        ("last_name",      validate_name(df["last_name"],   "last_name")),
        ("email",          validate_email(df["email"])),
        ("phone",          validate_phone(df["phone"])),
        ("date_of_birth",  validate_date_column(df["date_of_birth"],  "date_of_birth")),
        ("created_date",   validate_date_column(df["created_date"],   "created_date")),
        ("address",        validate_address(df["address"])),
        ("income",         validate_income(df["income"])),
        ("account_status", validate_account_status(df["account_status"])),
    ]

    for col_name, col_failures in checks:
        if col_failures:
            failures_by_col[col_name] = col_failures

    return failures_by_col


def build_report(
    df: pd.DataFrame,
    failures_by_col: Dict[str, List[Dict]]
) -> str:
    """
    Assemble validation results into a formatted report string.

    Parameters
    ----------
    df : pd.DataFrame
    failures_by_col : dict

    Returns
    -------
    str
    """
    total_rows = len(df)
    # Rows that failed (1-based row numbers)
    failed_rows: set = set()
    for col_failures in failures_by_col.values():
        for f in col_failures:
            failed_rows.add(f["row"])

    passed_rows = total_rows - len(failed_rows)
    total_failures = sum(len(v) for v in failures_by_col.values())

    lines: List[str] = []
    lines.append("VALIDATION RESULTS")
    lines.append("===================")
    lines.append("")
    lines.append(f"PASS: {passed_rows} rows passed all checks")
    lines.append(f"FAIL: {len(failed_rows)} rows failed at least one check")
    lines.append("")

    if failures_by_col:
        lines.append("FAILURES BY COLUMN:")
        lines.append("-" * 50)
        for col in [
            "customer_id", "first_name", "last_name", "email", "phone",
            "date_of_birth", "created_date", "address", "income", "account_status"
        ]:
            if col not in failures_by_col:
                continue
            lines.append(f"\n{col}:")
            for f in failures_by_col[col]:
                val_display = repr(str(f["value"])) if not pd.isna(f["value"]) else "NULL/NaN"
                lines.append(f"  - Row {f['row']}: {val_display} ({f['rule']})")
    else:
        lines.append("No failures found across all columns.")

    lines.append("")

    # --- SUMMARY TABLE ---
    col_order = [
        "customer_id", "first_name", "last_name", "email", "phone",
        "date_of_birth", "created_date", "address", "income", "account_status"
    ]
    lines.append("SUMMARY TABLE:")
    header = f"  {'Column':<20} {'Rules Checked':<20} {'Pass':>5} {'Fail':>5}"
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))

    rules_map = {
        "customer_id":    "Positive int, unique",
        "first_name":     "Non-empty, 2-50 chars, letters only",
        "last_name":      "Non-empty, 2-50 chars, letters only",
        "email":          "Valid email format",
        "phone":          "10-15 digits when stripped",
        "date_of_birth":  "Valid date, age 0-150 years",
        "created_date":   "Valid date, not in future",
        "address":        "Non-empty string",
        "income":         "Non-negative, ≤ $10M",
        "account_status": "active|inactive|suspended",
    }

    for col in col_order:
        col_failures = failures_by_col.get(col, [])
        fail_count = len(col_failures)
        pass_count = total_rows - fail_count
        rule_desc = rules_map.get(col, "—")
        lines.append(
            f"  {col:<20} {rule_desc:<20} {pass_count:>5} {fail_count:>5}"
        )

    lines.append("")
    lines.append(
        f"OVERALL: {total_failures} total validation failure(s) "
        f"across {len(failed_rows)} row(s)"
    )

    return "\n".join(lines)


def run_validation(
    df: pd.DataFrame, output_dir: str = "."
) -> Tuple[str, Dict]:
    """
    Run full validation and write validation_results.txt.

    Parameters
    ----------
    df : pd.DataFrame
        Raw or cleaned data (dtype=object).
    output_dir : str

    Returns
    -------
    (report_text, failures_by_col)
    """
    failures_by_col = run_all_validators(df)
    report = build_report(df, failures_by_col)

    out_path = os.path.join(output_dir, "validation_results.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)

    return report, failures_by_col


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    csv_path = sys.argv[1] if len(sys.argv) > 1 else "customers_raw.csv"
    print(f"[Part 3] Loading '{csv_path}' ...")
    raw_df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    raw_df.replace("", pd.NA, inplace=True)
    print(f"[Part 3] Loaded {len(raw_df)} rows × {len(raw_df.columns)} columns.")

    report_text, failures = run_validation(raw_df, output_dir=".")
    print(report_text)
    print(f"\n[Part 3] validation_results.txt written.")
    total = sum(len(v) for v in failures.values())
    print(f"[Part 3] Total validation failures: {total}")
