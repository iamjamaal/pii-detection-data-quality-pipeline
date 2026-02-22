"""
part2_pii_detection.py
-----------------------
PII (Personally Identifiable Information) detection for the customer dataset.

Classifies columns by PII risk level, uses regex to find email and phone
patterns in every cell, enumerates PII per row, and quantifies the overall
breach exposure risk.

Exposes run_pii_detection(df) for import by the pipeline orchestrator,
and can also be executed standalone via __main__.
"""

import re
import os
import pandas as pd
from typing import Dict, List, Tuple, Set


# ---------------------------------------------------------------------------
# PII column classification
# ---------------------------------------------------------------------------

PII_COLUMNS: Dict[str, Dict] = {
    "first_name":    {"category": "Names",               "risk": "HIGH"},
    "last_name":     {"category": "Names",               "risk": "HIGH"},
    "email":         {"category": "Contact Info",        "risk": "HIGH"},
    "phone":         {"category": "Contact Info",        "risk": "HIGH"},
    "date_of_birth": {"category": "Sensitive Personal",  "risk": "HIGH"},
    "address":       {"category": "Sensitive Personal",  "risk": "HIGH"},
    "income":        {"category": "Financial",           "risk": "MEDIUM"},
}

NON_PII_COLUMNS: List[str] = ["customer_id", "account_status", "created_date"]

# Regex patterns for dynamic PII detection within cell values
EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE
)

PHONE_PATTERN = re.compile(
    r"(?:\+1[-.\s]?)?"             # optional country code
    r"(?:\(\d{3}\)|\d{3})"        # area code
    r"[-.\s]?"                     # separator
    r"\d{3}"                       # exchange
    r"[-.\s]?"                     # separator
    r"\d{4}"                       # subscriber
)

DATE_PATTERN = re.compile(
    r"\b\d{4}-\d{2}-\d{2}\b"      # YYYY-MM-DD
    r"|"
    r"\b\d{2}/\d{2}/\d{4}\b"      # MM/DD/YYYY
)


# ---------------------------------------------------------------------------
# Detection functions
# ---------------------------------------------------------------------------

def detect_email_pii(df: pd.DataFrame) -> List[int]:
    """
    Find row indices where the email column contains a valid email address.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    list of int row indices (0-based).
    """
    found: List[int] = []
    for idx, val in df["email"].items():
        v = str(val).strip()
        if v and EMAIL_PATTERN.search(v):
            found.append(int(idx))
    return found


def detect_phone_pii(df: pd.DataFrame) -> List[int]:
    """
    Find row indices where the phone column contains a detectable phone number.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    list of int row indices (0-based).
    """
    found: List[int] = []
    for idx, val in df["phone"].items():
        v = str(val).strip()
        if v and PHONE_PATTERN.search(v):
            found.append(int(idx))
    return found


def detect_address_pii(df: pd.DataFrame) -> List[int]:
    """
    Find row indices where the address column is non-empty.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    list of int row indices (0-based).
    """
    found: List[int] = []
    for idx, val in df["address"].items():
        v = str(val).strip()
        if v and not pd.isna(val):
            found.append(int(idx))
    return found


def detect_dob_pii(df: pd.DataFrame) -> List[int]:
    """
    Find row indices where date_of_birth has a non-empty, non-NaN value.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    list of int row indices (0-based).
    """
    found: List[int] = []
    for idx, val in df["date_of_birth"].items():
        v = str(val).strip()
        if v and not pd.isna(val):
            found.append(int(idx))
    return found


def detect_name_pii(df: pd.DataFrame) -> List[int]:
    """
    Find row indices where either first_name or last_name is present.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    list of int row indices (0-based).
    """
    found: Set[int] = set()
    for col in ("first_name", "last_name"):
        for idx, val in df[col].items():
            v = str(val).strip()
            if v and not pd.isna(val):
                found.add(int(idx))
    return sorted(found)


def detect_income_pii(df: pd.DataFrame) -> List[int]:
    """
    Find row indices where income is non-empty.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    list of int row indices (0-based).
    """
    found: List[int] = []
    for idx, val in df["income"].items():
        v = str(val).strip()
        if v and not pd.isna(val):
            found.append(int(idx))
    return found


def build_row_pii_inventory(
    df: pd.DataFrame,
    email_rows: List[int],
    phone_rows: List[int],
    address_rows: List[int],
    dob_rows: List[int],
    name_rows: List[int],
    income_rows: List[int],
) -> Dict[int, List[str]]:
    """
    Build a per-row inventory of PII types present.

    Parameters
    ----------
    df : pd.DataFrame
    *_rows : lists of 0-based row indices for each PII type

    Returns
    -------
    dict mapping 0-based row index -> list of PII type labels.
    """
    inventory: Dict[int, List[str]] = {i: [] for i in range(len(df))}
    for idx in name_rows:
        inventory[idx].append("Name (first/last)")
    for idx in email_rows:
        inventory[idx].append("Email")
    for idx in phone_rows:
        inventory[idx].append("Phone")
    for idx in address_rows:
        inventory[idx].append("Address")
    for idx in dob_rows:
        inventory[idx].append("Date of Birth")
    for idx in income_rows:
        inventory[idx].append("Income")
    return inventory


def build_report(
    df: pd.DataFrame,
    email_rows: List[int],
    phone_rows: List[int],
    address_rows: List[int],
    dob_rows: List[int],
    name_rows: List[int],
    income_rows: List[int],
    row_inventory: Dict[int, List[str]],
) -> str:
    """
    Assemble PII detection findings into a formatted report string.

    Parameters
    ----------
    df : pd.DataFrame
    *_rows : lists of row indices
    row_inventory : per-row PII type list

    Returns
    -------
    str
    """
    total = len(df)
    lines: List[str] = []
    lines.append("PII DETECTION REPORT")
    lines.append("======================")
    lines.append("")

    # --- RISK ASSESSMENT ---
    lines.append("RISK ASSESSMENT:")
    high_pii = [c for c, m in PII_COLUMNS.items() if m["risk"] == "HIGH"]
    med_pii  = [c for c, m in PII_COLUMNS.items() if m["risk"] == "MEDIUM"]
    lines.append(
        f"  - HIGH: {', '.join(high_pii)}\n"
        f"    (Direct identifiers — names, contact details, DOB, address\n"
        f"     combine to uniquely identify and locate any individual.)"
    )
    lines.append(
        f"  - MEDIUM: {', '.join(med_pii)}\n"
        f"    (Financial sensitivity — income reveals economic status\n"
        f"     and is protected under many privacy regulations.)"
    )
    lines.append("")

    # --- DETECTED PII ---
    def pct(n: int) -> str:
        return f"{round(n / total * 100, 1)}%"

    lines.append("DETECTED PII:")
    lines.append(f"  - Emails found:         {len(email_rows):>3} out of {total} rows ({pct(len(email_rows))})")
    lines.append(f"  - Phone numbers found:  {len(phone_rows):>3} out of {total} rows ({pct(len(phone_rows))})")
    lines.append(f"  - Addresses found:      {len(address_rows):>3} out of {total} rows ({pct(len(address_rows))})")
    lines.append(f"  - Dates of birth found: {len(dob_rows):>3} out of {total} rows ({pct(len(dob_rows))})")
    lines.append(f"  - Names found:          {len(name_rows):>3} out of {total} rows ({pct(len(name_rows))})")
    lines.append(f"  - Income data found:    {len(income_rows):>3} out of {total} rows ({pct(len(income_rows))})")
    lines.append("")

    # --- PII BY ROW ---
    lines.append("PII BY ROW:")
    for i in range(total):
        row_num = i + 2   # human-readable: header is row 1
        cust_id = str(df.iloc[i]["customer_id"]).strip()
        pii_types = row_inventory[i]
        if pii_types:
            lines.append(f"  - Row {row_num:>2} (ID={cust_id}): {', '.join(pii_types)}")
        else:
            lines.append(f"  - Row {row_num:>2} (ID={cust_id}): No PII detected")
    lines.append("")

    # --- COLUMN PII CLASSIFICATION ---
    lines.append("COLUMN PII CLASSIFICATION:")
    lines.append(f"  {'Column':<20} {'Category':<22} {'Risk'}")
    lines.append(f"  {'-'*20} {'-'*22} {'-'*6}")
    for col, meta in PII_COLUMNS.items():
        lines.append(f"  {col:<20} {meta['category']:<22} {meta['risk']}")
    for col in NON_PII_COLUMNS:
        lines.append(f"  {col:<20} {'Non-PII':<22} {'NONE'}")
    lines.append("")

    # --- EXPOSURE RISK ---
    lines.append("EXPOSURE RISK:")
    lines.append("  If this dataset were breached, attackers could:")
    lines.append("  - Phish customers (have full email addresses)")
    lines.append("  - Spoof identities (have names + DOB + address)")
    lines.append("  - Social engineer targets (have phone numbers)")
    lines.append("  - Financial profiling (have income levels)")
    lines.append(
        f"  - At-risk individuals: ALL {total} customers "
        f"({pct(total)} of dataset)"
    )
    lines.append("")

    # --- MITIGATION ---
    lines.append(
        "MITIGATION: Mask all PII before sharing with analytics teams.\n"
        "  Apply column-level masking (names, emails, phones, addresses, DOBs).\n"
        "  Retain only customer_id, income, account_status, created_date for\n"
        "  analytics purposes — or tokenise income into brackets."
    )

    return "\n".join(lines)


def run_pii_detection(
    df: pd.DataFrame, output_dir: str = "."
) -> Tuple[str, Dict]:
    """
    Run PII detection and write pii_detection_report.txt.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data (dtype=object / string columns).
    output_dir : str
        Directory for the output report file.

    Returns
    -------
    (report_text, findings_dict)
    """
    email_rows   = detect_email_pii(df)
    phone_rows   = detect_phone_pii(df)
    address_rows = detect_address_pii(df)
    dob_rows     = detect_dob_pii(df)
    name_rows    = detect_name_pii(df)
    income_rows  = detect_income_pii(df)

    row_inventory = build_row_pii_inventory(
        df, email_rows, phone_rows, address_rows,
        dob_rows, name_rows, income_rows
    )

    report = build_report(
        df, email_rows, phone_rows, address_rows,
        dob_rows, name_rows, income_rows, row_inventory
    )

    out_path = os.path.join(output_dir, "pii_detection_report.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)

    findings = {
        "email_rows":    email_rows,
        "phone_rows":    phone_rows,
        "address_rows":  address_rows,
        "dob_rows":      dob_rows,
        "name_rows":     name_rows,
        "income_rows":   income_rows,
        "row_inventory": row_inventory,
    }
    return report, findings


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    csv_path = sys.argv[1] if len(sys.argv) > 1 else "customers_raw.csv"
    print(f"[Part 2] Loading '{csv_path}' ...")
    raw_df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    raw_df.replace("", pd.NA, inplace=True)
    print(f"[Part 2] Loaded {len(raw_df)} rows × {len(raw_df.columns)} columns.")

    report_text, _ = run_pii_detection(raw_df, output_dir=".")
    sys.stdout.buffer.write((report_text + "\n").encode("utf-8"))
    print("\n[Part 2] pii_detection_report.txt written.")
