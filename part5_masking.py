"""
part5_masking.py
----------------
PII masking for the cleaned customer dataset.

Implements column-specific masking functions that hide sensitive data while
preserving data structure and format. Saves the masked dataset to
customers_masked.csv and produces a before/after comparison in masked_sample.txt.

Columns masked:
  first_name   : "John"              -> "J***"
  last_name    : "Doe"               -> "D***"
  email        : "j.doe@gmail.com"   -> "j***@gmail.com"
  phone        : "555-123-4567"      -> "***-***-4567"
  address      : any non-empty val   -> "[MASKED ADDRESS]"
  date_of_birth: "1985-03-15"        -> "1985-**-**"

Columns NOT masked:
  customer_id, income, account_status, created_date

Exposes run_masking(df) for import by the pipeline orchestrator,
and can also be executed standalone via __main__.
"""

import os
import re
import pandas as pd
from typing import Any, Tuple



# Masking functions
def mask_name(val: Any) -> str:
    """
    Mask a name value: keep first character, replace the rest with '***'.

    Examples
    --------
    "John"     -> "J***"
    "[UNKNOWN]" -> "[UNKNOWN]"  (placeholder not masked)
    NaN        -> ""

    Parameters
    ----------
    val : any

    Returns
    -------
    str
    """
    if pd.isna(val) or str(val).strip() == "":
        return ""
    v = str(val).strip()
    if v in ("[UNKNOWN]",):
        return v  # preserve fill placeholders
    if len(v) <= 1:
        return v + "***"
    return v[0] + "***"


def mask_email(val: Any) -> str:
    """
    Mask an email address: keep first char of local part, mask rest, preserve domain.

    Examples
    --------
    "john.doe@gmail.com"   -> "j***@gmail.com"
    "PATRICIA@company.com" -> "P***@company.com"
    NaN                    -> ""

    Parameters
    ----------
    val : any

    Returns
    -------
    str
    """
    if pd.isna(val) or str(val).strip() == "":
        return ""
    v = str(val).strip()
    if "@" not in v:
        return v  # not a recognisable email — return as-is
    local, domain = v.split("@", 1)
    masked_local = local[0] + "***" if local else "***"
    return f"{masked_local}@{domain}"


def mask_phone(val: Any) -> str:
    """
    Mask a phone number: hide all but the last 4 digits, preserve XXX-XXX-XXXX format.

    Examples
    --------
    "555-123-4567" -> "***-***-4567"
    "5551234567"   -> "***-***-4567"  (normalised first)
    NaN            -> ""

    Parameters
    ----------
    val : any

    Returns
    -------
    str
    """
    if pd.isna(val) or str(val).strip() == "":
        return ""
    v = str(val).strip()
    # Strip to digits
    digits = re.sub(r"\D", "", v)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"***-***-{digits[6:]}"
    # Fallback: can't determine structure — mask everything except last 4 chars
    if len(v) >= 4:
        return "***" + v[-4:]
    return "****"


def mask_address(val: Any) -> str:
    """
    Mask an address: replace any non-empty value with '[MASKED ADDRESS]'.

    Parameters
    ----------
    val : any

    Returns
    -------
    str
    """
    if pd.isna(val) or str(val).strip() in ("", "[UNKNOWN]"):
        return str(val).strip() if not pd.isna(val) else ""
    return "[MASKED ADDRESS]"


def mask_dob(val: Any) -> str:
    """
    Mask a date of birth: preserve year only, replace month/day with '**-**'.

    Examples
    --------
    "1985-03-15"  -> "1985-**-**"
    NaN           -> ""

    Parameters
    ----------
    val : any

    Returns
    -------
    str
    """
    if pd.isna(val) or str(val).strip() == "":
        return ""
    v = str(val).strip()
    # Expect YYYY-MM-DD after cleaning
    parts = v.split("-")
    if len(parts) >= 1 and len(parts[0]) == 4:
        return f"{parts[0]}-**-**"
    # Fallback for unexpected format
    return "****-**-**"





# Apply masking to DataFrame

def apply_masking(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all masking functions to a copy of the cleaned DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned customer data.

    Returns
    -------
    pd.DataFrame
        New DataFrame with PII columns masked.
    """
    masked = df.copy()

    masked["first_name"]    = masked["first_name"].apply(mask_name)
    masked["last_name"]     = masked["last_name"].apply(mask_name)
    masked["email"]         = masked["email"].apply(mask_email)
    masked["phone"]         = masked["phone"].apply(mask_phone)
    masked["address"]       = masked["address"].apply(mask_address)
    masked["date_of_birth"] = masked["date_of_birth"].apply(mask_dob)

    # customer_id, income, account_status, created_date — untouched

    return masked




# Report builder

def build_sample_report(
    cleaned_df: pd.DataFrame,
    masked_df: pd.DataFrame,
    n: int = 3,
) -> str:
    """
    Build a before/after sample comparison text report.

    Parameters
    ----------
    cleaned_df : pd.DataFrame
    masked_df : pd.DataFrame
    n : int – number of rows to show (default 3)

    Returns
    -------
    str
    """
    def df_to_lines(df: pd.DataFrame, rows: int) -> list:
        cols = ",".join(df.columns)
        result = [cols]
        for i, row in df.head(rows).iterrows():
            result.append(",".join(str(v) for v in row))
        return result

    lines = []
    lines.append(f"BEFORE MASKING (first {n} rows):")
    lines.append("-" * 50)
    for ln in df_to_lines(cleaned_df, n):
        lines.append(ln)
    lines.append("")
    lines.append(f"AFTER MASKING (first {n} rows):")
    lines.append("-" * 50)
    for ln in df_to_lines(masked_df, n):
        lines.append(ln)
    lines.append("")
    lines.append("ANALYSIS:")
    lines.append(
        f"  - Data structure preserved "
        f"(still {len(masked_df)} rows, {len(masked_df.columns)} columns)"
    )
    lines.append(
        "  - PII masked: first_name, last_name, email, phone, address, date_of_birth"
    )
    lines.append(
        "  - Business data intact: customer_id, income, account_status, created_date"
    )
    lines.append(
        "  - Use case: Safe for analytics team (GDPR / CCPA compliant sharing)"
    )

    masked_cols = ["first_name", "last_name", "email", "phone", "address", "date_of_birth"]
    intact_cols = ["customer_id", "income", "account_status", "created_date"]
    lines.append("")
    lines.append("MASKING SUMMARY:")
    lines.append(f"  Masked columns  ({len(masked_cols)}): {', '.join(masked_cols)}")
    lines.append(f"  Intact columns  ({len(intact_cols)}): {', '.join(intact_cols)}")
    lines.append(f"  Total rows processed: {len(masked_df)}")

    return "\n".join(lines)




# Main entry point for the module
def run_masking(
    df: pd.DataFrame, output_dir: str = "."
) -> Tuple[pd.DataFrame, str]:
    """
    Mask all PII in the cleaned DataFrame, save outputs, and return results.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned customer data (output of Part 4).
    output_dir : str
        Directory to write masked_sample.txt and customers_masked.csv.

    Returns
    -------
    (masked_df, sample_report_text)
    """
    masked_df = apply_masking(df)

    sample_report = build_sample_report(df, masked_df, n=3)

    # Write masked_sample.txt
    sample_path = os.path.join(output_dir, "masked_sample.txt")
    with open(sample_path, "w", encoding="utf-8") as f:
        f.write(sample_report)

    # Write customers_masked.csv
    masked_csv_path = os.path.join(output_dir, "customers_masked.csv")
    masked_df.to_csv(masked_csv_path, index=False)

    return masked_df, sample_report




# Standalone entry point
if __name__ == "__main__":
    import sys

    csv_path = sys.argv[1] if len(sys.argv) > 1 else "customers_cleaned.csv"
    print(f"[Part 5] Loading '{csv_path}' ...")

    try:
        cleaned = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        cleaned.replace("", pd.NA, inplace=True)
    except FileNotFoundError:
        print(
            f"[Part 5] '{csv_path}' not found. "
            "Run part4_cleaning.py first to generate the cleaned CSV."
        )
        raise SystemExit(1)

    print(f"[Part 5] Loaded {len(cleaned)} rows × {len(cleaned.columns)} columns.")

    masked_df, report = run_masking(cleaned, output_dir=".")
    sys.stdout.buffer.write((report + "\n").encode("utf-8"))
    print("\n[Part 5] masked_sample.txt and customers_masked.csv written.")
