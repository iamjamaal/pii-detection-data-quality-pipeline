"""
part4_cleaning.py
-----------------
Data cleaning and normalisation for the customer dataset.

Performs:
  - Phone normalisation  -> XXX-XXX-XXXX
  - Date normalisation   -> YYYY-MM-DD
  - Name title-casing
  - Missing-value strategy (per-column fill rules)
  - Invalid-value remediation (duplicates, negatives, extremes, flags)
  - Re-validation after cleaning (before/after comparison)
  - Saves customers_cleaned.csv

Exposes run_cleaning(df) for import by the pipeline orchestrator,
and can also be executed standalone via __main__.
"""

import re
import os
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Tuple, Any, Optional





# Phone normalisation

def _strip_phone(val: Any) -> str:
    """Strip a phone value to digits only."""
    return re.sub(r"\D", "", str(val))


def normalise_phone(val: Any) -> Tuple[str, Optional[str]]:
    """
    Normalise a phone number to XXX-XXX-XXXX format.

    - Removes all non-digit characters.
    - Strips leading '1' if result is 11 digits (US country code).
    - Reformats exactly 10 digits as XXX-XXX-XXXX.
    - If result is not 10 digits, returns the original value and a flag note.

    Parameters
    ----------
    val : any

    Returns
    -------
    (normalised_value, flag_note)
        flag_note is None when normalisation succeeded.
    """
    if pd.isna(val) or str(val).strip() == "":
        return str(val), None

    original = str(val).strip()
    digits = _strip_phone(original)

    # Remove US country code prefix
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]

    if len(digits) == 10:
        formatted = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        changed = formatted != original
        return formatted, None
    else:
        return original, f"Could not normalise: {len(digits)} digits after stripping"



# Date normalisation

DATE_PARSE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]


def normalise_date(val: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Normalise a date value to YYYY-MM-DD.

    - Returns (normalised_str, None) on success.
    - Returns (None, note) when value is invalid or unparseable.

    Parameters
    ----------
    val : any

    Returns
    -------
    (normalised_value_or_None, flag_note_or_None)
    """
    if pd.isna(val) or str(val).strip() == "":
        return None, None  # already missing

    v = str(val).strip()

    if v.lower() == "invalid_date":
        return None, f"Literal 'invalid_date' — set to NaN"

    for fmt in DATE_PARSE_FORMATS:
        try:
            parsed = datetime.strptime(v, fmt).date()
            return parsed.strftime("%Y-%m-%d"), None
        except ValueError:
            pass

    return None, f"Unparseable date string '{v}' — set to NaN"




# Name title-casing

def normalise_name(val: Any) -> Tuple[str, Optional[str]]:
    """
    Apply title case to a name value if it is all-caps or all-lowercase.

    Parameters
    ----------
    val : any

    Returns
    -------
    (normalised_value, change_note_or_None)
    """
    if pd.isna(val) or str(val).strip() == "":
        return val, None
    v = str(val).strip()
    title = v.title()
    if v != title:
        return title, f"'{v}' -> '{title}'"
    return v, None




# Main cleaning pipeline

def run_cleaning(
    df: pd.DataFrame, output_dir: str = "."
) -> Tuple[pd.DataFrame, str]:
    """
    Apply all cleaning steps to the raw DataFrame, write outputs, and return
    the cleaned DataFrame plus a text cleaning log.

    Cleaning order:
    1. Normalise phone formats
    2. Normalise date formats (date_of_birth, created_date)
    3. Normalise name casing
    4. Fill missing values per strategy
    5. Handle invalid values (duplicates, negatives, out-of-range, flags)
    6. Re-validate (imports Part 3 validator)
    7. Save customers_cleaned.csv

    Parameters
    ----------
    df : pd.DataFrame
        Raw data loaded as strings.
    output_dir : str
        Directory for output files.

    Returns
    -------
    (cleaned_df, log_text)
    """
    from part3_validator import run_all_validators  # local import to avoid circularity

    # Work on a copy so the caller's DataFrame is unchanged
    cleaned = df.copy()

    log_lines: List[str] = []
    log_lines.append("DATA CLEANING LOG")
    log_lines.append("")
    log_lines.append("ACTIONS TAKEN:")
    log_lines.append("-" * 50)



    # 1. Phone normalisation
    phone_changes: List[str] = []
    phone_flags:   List[str] = []
    for idx, val in cleaned["phone"].items():
        norm, note = normalise_phone(val)
        if note:
            phone_flags.append(f"    Row {idx+2}: '{val}' -> flagged: {note}")
        elif str(norm) != str(val):
            phone_changes.append(
                f"    Row {idx+2}: '{val}' -> '{norm}'"
            )
            cleaned.at[idx, "phone"] = norm

    log_lines.append("\nNormalisation:")
    log_lines.append(
        f"  Phone format: Converted to XXX-XXX-XXXX "
        f"({len(phone_changes)} row(s) affected)"
    )
    for c in phone_changes:
        log_lines.append(c)
    if phone_flags:
        log_lines.append(f"  Phone flags ({len(phone_flags)} rows could not be normalised):")
        for f in phone_flags:
            log_lines.append(f)
            


    # 2. Date normalisation
    date_changes: List[str] = []
    date_nulled:  List[str] = []

    for col in ["date_of_birth", "created_date"]:
        for idx, val in cleaned[col].items():
            if pd.isna(val) or str(val).strip() == "":
                continue
            norm, note = normalise_date(val)
            if note:
                # Unparseable — set to NaN
                date_nulled.append(f"    Row {idx+2} [{col}]: '{val}' — {note}")
                cleaned.at[idx, col] = pd.NA
            elif norm and norm != str(val).strip():
                date_changes.append(
                    f"    Row {idx+2} [{col}]: '{val}' -> '{norm}'"
                )
                cleaned.at[idx, col] = norm

    log_lines.append(
        f"\n  Date format: Converted to YYYY-MM-DD "
        f"({len(date_changes)} row(s) reformatted)"
    )
    for c in date_changes:
        log_lines.append(c)
    if date_nulled:
        log_lines.append(
            f"  Invalid/unparseable dates set to NaN ({len(date_nulled)} occurrence(s)):"
        )
        for n in date_nulled:
            log_lines.append(n)



    # 3. Name title-casing
    name_changes: List[str] = []
    for col in ["first_name", "last_name"]:
        for idx, val in cleaned[col].items():
            norm, note = normalise_name(val)
            if note:
                name_changes.append(f"    Row {idx+2} [{col}]: {note}")
                cleaned.at[idx, col] = norm

    log_lines.append(
        f"\n  Name casing: Applied title case "
        f"({len(name_changes)} row(s) affected)"
    )
    for c in name_changes:
        log_lines.append(c)


    
    # 4. Missing value strategy
    log_lines.append("\nMissing Values:")

    fill_strategy: Dict[str, Any] = {
        "first_name":     "[UNKNOWN]",
        "last_name":      "[UNKNOWN]",
        "address":        "[UNKNOWN]",
        "income":         "0",
        "account_status": "unknown",
        # date_of_birth: leave as NaN
    }

    for col, fill_val in fill_strategy.items():
        missing_mask = cleaned[col].apply(
            lambda v: pd.isna(v) or str(v).strip() == ""
        )
        count = int(missing_mask.sum())
        if count > 0:
            cleaned.loc[missing_mask, col] = fill_val
            log_lines.append(
                f"  {col}: {count} row(s) missing -> filled with '{fill_val}'"
            )
        else:
            log_lines.append(f"  {col}: 0 rows missing — no action needed")

    # date_of_birth: leave NaN as-is
    dob_missing = cleaned["date_of_birth"].apply(
        lambda v: pd.isna(v) or str(v).strip() == ""
    ).sum()
    log_lines.append(
        f"  date_of_birth: {int(dob_missing)} row(s) missing -> left as NaN "
        f"(cannot be inferred)"
    )



    # 5. Invalid value remediation
    log_lines.append("\nInvalid Values:")

    # 5a. Duplicate customer_id — keep first, drop rest
    before_len = len(cleaned)
    dupe_mask = cleaned["customer_id"].astype(str).duplicated(keep="first")
    duped_rows = cleaned[dupe_mask]
    if not duped_rows.empty:
        dupe_info = [
            f"Row {idx+2} (ID={row['customer_id']})"
            for idx, row in duped_rows.iterrows()
        ]
        cleaned = cleaned[~dupe_mask].reset_index(drop=True)
        log_lines.append(
            f"  Duplicate customer_id: {len(dupe_info)} row(s) dropped — "
            + ", ".join(dupe_info)
        )
    else:
        log_lines.append("  Duplicate customer_id: none found")

    # 5b. Negative income — set to 0
    neg_income_mask = cleaned["income"].apply(
        lambda v: False if (pd.isna(v) or str(v).strip() == "")
        else _safe_float(str(v).strip(), default=0) < 0
    )
    neg_count = int(neg_income_mask.sum())
    if neg_count > 0:
        neg_rows = [
            f"Row {idx+2}: original income = {cleaned.at[idx, 'income']}"
            for idx in cleaned.index[neg_income_mask]
        ]
        cleaned.loc[neg_income_mask, "income"] = "0"
        log_lines.append(
            f"  Negative income: {neg_count} row(s) set to 0 — "
            + ", ".join(neg_rows)
        )
    else:
        log_lines.append("  Negative income: none found")

    # 5c. Income > $10M — flag but do NOT modify
    high_income_mask = cleaned["income"].apply(
        lambda v: False if (pd.isna(v) or str(v).strip() == "")
        else _safe_float(str(v).strip(), default=0) > 10_000_000
    )
    hi_count = int(high_income_mask.sum())
    if hi_count > 0:
        hi_rows = [
            f"Row {idx+2}: income = {cleaned.at[idx, 'income']}"
            for idx in cleaned.index[high_income_mask]
        ]
        log_lines.append(
            f"  Income > $10M: {hi_count} row(s) flagged for review (NOT modified) — "
            + ", ".join(hi_rows)
        )
    else:
        log_lines.append("  Income > $10M: none found")

    # 5d. Invalid account_status — flag but do NOT modify
    valid_statuses = {"active", "inactive", "suspended", "unknown", "[unknown]"}
    bad_status_mask = cleaned["account_status"].apply(
        lambda v: False if (pd.isna(v) or str(v).strip() == "")
        else str(v).strip().lower() not in {"active", "inactive", "suspended"}
    )
    bad_count = int(bad_status_mask.sum())
    if bad_count > 0:
        bad_rows = [
            f"Row {idx+2}: '{cleaned.at[idx, 'account_status']}'"
            for idx in cleaned.index[bad_status_mask]
        ]
        log_lines.append(
            f"  Invalid account_status: {bad_count} row(s) flagged for review — "
            + ", ".join(bad_rows)
        )
    else:
        log_lines.append("  Invalid account_status: none found")

    # 5e. Future created_date — flag but do NOT modify
    today = date.today()
    future_date_mask = cleaned["created_date"].apply(
        lambda v: _is_future_date(v, today)
    )
    fut_count = int(future_date_mask.sum())
    if fut_count > 0:
        fut_rows = [
            f"Row {idx+2}: '{cleaned.at[idx, 'created_date']}'"
            for idx in cleaned.index[future_date_mask]
        ]
        log_lines.append(
            f"  Future created_date: {fut_count} row(s) flagged for review (NOT modified) — "
            + ", ".join(fut_rows)
        )
    else:
        log_lines.append("  Future created_date: none found")

    # 5f. Age > 150 — set date_of_birth to NaN
    age_flag_rows: List[str] = []
    for idx, val in cleaned["date_of_birth"].items():
        if pd.isna(val) or str(val).strip() == "":
            continue
        v = str(val).strip()
        for fmt in ["%Y-%m-%d", "%m/%d/%Y"]:
            try:
                dob = datetime.strptime(v, fmt).date()
                age = (today - dob).days / 365.25
                if age > 150:
                    age_flag_rows.append(
                        f"Row {idx+2}: DOB '{val}' (~{age:.1f} years old)"
                    )
                    cleaned.at[idx, "date_of_birth"] = pd.NA
                break
            except ValueError:
                pass

    if age_flag_rows:
        log_lines.append(
            f"  Age > 150: {len(age_flag_rows)} row(s) — DOB set to NaN — "
            + ", ".join(age_flag_rows)
        )
    else:
        log_lines.append("  Age > 150: none found")
        
        


    # 6. Re-validate
    # Count failures on raw vs cleaned
    raw_failures_by_col = run_all_validators(df)
    raw_total = sum(len(v) for v in raw_failures_by_col.values())

    clean_failures_by_col = run_all_validators(cleaned)
    clean_total = sum(len(v) for v in clean_failures_by_col.values())

    improvement = raw_total - clean_total
    log_lines.append("\nValidation After Cleaning:")
    log_lines.append(f"  Before: {raw_total} validation failure(s)")
    log_lines.append(f"  After:  {clean_total} validation failure(s)")
    log_lines.append(
        f"  Improvement: {improvement} failure(s) resolved. "
        + (
            "Remaining failures are flags (e.g., income > $10M, future dates) "
            "kept intentionally for review."
            if clean_total > 0
            else "All failures resolved."
        )
    )



    
    # 7. Save cleaned CSV
    out_csv = os.path.join(output_dir, "customers_cleaned.csv")
    cleaned.to_csv(out_csv, index=False)
    log_lines.append(
        f"\nOutput: customers_cleaned.csv "
        f"({len(cleaned)} rows, {len(cleaned.columns)} columns)"
    )

    log_text = "\n".join(log_lines)

    # Write cleaning log
    out_log = os.path.join(output_dir, "cleaning_log.txt")
    with open(out_log, "w", encoding="utf-8") as f:
        f.write(log_text)

    return cleaned, log_text



# Utility helpers

def _safe_float(val: str, default: float = 0.0) -> float:
    """Parse a string to float, returning default on failure."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _is_future_date(val: Any, today: date) -> bool:
    """Return True if val is a valid date string in the future."""
    if pd.isna(val) or str(val).strip() == "":
        return False
    v = str(val).strip()
    for fmt in ["%Y-%m-%d", "%m/%d/%Y"]:
        try:
            d = datetime.strptime(v, fmt).date()
            return d > today
        except ValueError:
            pass
    return False




# Standalone entry point

if __name__ == "__main__":
    import sys

    csv_path = sys.argv[1] if len(sys.argv) > 1 else "customers_raw.csv"
    print(f"[Part 4] Loading '{csv_path}' ...")
    raw_df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    raw_df.replace("", pd.NA, inplace=True)
    print(f"[Part 4] Loaded {len(raw_df)} rows × {len(raw_df.columns)} columns.")

    cleaned_df, log = run_cleaning(raw_df, output_dir=".")
    sys.stdout.buffer.write((log + "\n").encode("utf-8"))
    print(f"\n[Part 4] cleaning_log.txt and customers_cleaned.csv written.")
    print(f"[Part 4] Cleaned dataset: {len(cleaned_df)} rows x {len(cleaned_df.columns)} columns.")
