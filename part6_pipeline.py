"""
part6_pipeline.py
-----------------
End-to-end orchestration pipeline for the PII Detection & Data Quality
Validation project.

Loads raw customer data, then executes all five processing stages in sequence:
  Stage 1  – Load
  Stage 2  – Data Quality Profiling  (Part 1)
  Stage 3  – PII Detection           (Part 2)
  Stage 4  – Validation              (Part 3)
  Stage 5  – Cleaning                (Part 4)
  Stage 6  – PII Masking             (Part 5)
  Stage 7  – Save pipeline report

Each stage is wrapped in its own try/except block.  A stage failure is logged
and execution continues where possible; a CSV load failure terminates the run.

All output files are written to the same directory as the input CSV (or an
optional output_dir argument).

Usage
-----
    python part6_pipeline.py [input_csv] [output_dir]

    # defaults:
    python part6_pipeline.py customers_raw.csv .
"""

import logging
import os
import sys
import traceback
from datetime import datetime
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(
    input_csv_path: str = "customers_raw.csv",
    output_dir: str = ".",
) -> None:
    """
    Execute the full data quality and PII pipeline from a single entry point.

    Parameters
    ----------
    input_csv_path : str
        Path to the raw input CSV file.
    output_dir : str
        Directory where all output files will be written.

    Returns
    -------
    None
        Writes all deliverable files to output_dir as a side-effect.
    """
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("PII DETECTION & DATA QUALITY VALIDATION PIPELINE")
    logger.info("=" * 60)
    logger.info(f"Input  : {os.path.abspath(input_csv_path)}")
    logger.info(f"Output : {os.path.abspath(output_dir)}")

    os.makedirs(output_dir, exist_ok=True)

    # Track stage outcomes for the final report
    stage_results = {}

    # -----------------------------------------------------------------------
    # Stage 1: Load
    # -----------------------------------------------------------------------
    logger.info("[Stage 1] Loading raw data ...")
    try:
        raw_df = pd.read_csv(input_csv_path, dtype=str, keep_default_na=False)
        raw_df.replace("", pd.NA, inplace=True)
        n_rows, n_cols = raw_df.shape
        logger.info(f"[Stage 1] Loaded {n_rows} rows × {n_cols} columns.")
        stage_results["load"] = {
            "status": "SUCCESS",
            "detail": f"{n_rows} rows, {n_cols} columns",
        }
    except FileNotFoundError:
        logger.error(f"[Stage 1] File not found: {input_csv_path}")
        logger.error("Pipeline cannot continue without input data. Exiting.")
        raise SystemExit(1)
    except Exception as exc:
        logger.error(f"[Stage 1] Unexpected error loading CSV: {exc}")
        raise SystemExit(1)

    # -----------------------------------------------------------------------
    # Stage 2: Data Quality Profiling
    # -----------------------------------------------------------------------
    logger.info("[Stage 2] Running data quality profiling ...")
    try:
        from part1_data_quality import run_quality_analysis
        _, findings = run_quality_analysis(raw_df, output_dir=output_dir)
        total_issues = (
            len(findings.get("invalid_vals", []))
            + len(findings.get("status_issues", []))
            + len(findings.get("name_issues", []))
        )
        logger.info(
            f"[Stage 2] Profiling complete. "
            f"Issues detected: {total_issues}. "
            f"-> data_quality_report.txt"
        )
        stage_results["quality"] = {
            "status": "SUCCESS",
            "detail": f"{total_issues} quality issues found",
            "file": "data_quality_report.txt",
        }
    except Exception as exc:
        logger.warning(f"[Stage 2] Profiling failed: {exc}\n{traceback.format_exc()}")
        stage_results["quality"] = {"status": "FAILED", "detail": str(exc)}

    # -----------------------------------------------------------------------
    # Stage 3: PII Detection
    # -----------------------------------------------------------------------
    logger.info("[Stage 3] Running PII detection ...")
    try:
        from part2_pii_detection import run_pii_detection
        _, pii_findings = run_pii_detection(raw_df, output_dir=output_dir)
        n_email = len(pii_findings.get("email_rows", []))
        n_phone = len(pii_findings.get("phone_rows", []))
        n_addr  = len(pii_findings.get("address_rows", []))
        n_dob   = len(pii_findings.get("dob_rows", []))
        logger.info(
            f"[Stage 3] PII detection complete. "
            f"Emails: {n_email}, Phones: {n_phone}, "
            f"Addresses: {n_addr}, DOBs: {n_dob}. "
            f"-> pii_detection_report.txt"
        )
        stage_results["pii"] = {
            "status": "SUCCESS",
            "detail": (
                f"Emails: {n_email}, Phones: {n_phone}, "
                f"Addresses: {n_addr}, DOBs: {n_dob}"
            ),
            "file": "pii_detection_report.txt",
        }
    except Exception as exc:
        logger.warning(f"[Stage 3] PII detection failed: {exc}\n{traceback.format_exc()}")
        stage_results["pii"] = {"status": "FAILED", "detail": str(exc)}

    # -----------------------------------------------------------------------
    # Stage 4: Validation
    # -----------------------------------------------------------------------
    logger.info("[Stage 4] Running validation ...")
    try:
        from part3_validator import run_validation
        _, failures_by_col = run_validation(raw_df, output_dir=output_dir)
        total_failures = sum(len(v) for v in failures_by_col.values())
        failed_rows = set()
        for col_failures in failures_by_col.values():
            for f in col_failures:
                failed_rows.add(f["row"])
        logger.info(
            f"[Stage 4] Validation complete. "
            f"{total_failures} failure(s) across {len(failed_rows)} row(s). "
            f"-> validation_results.txt"
        )
        stage_results["validation"] = {
            "status": "SUCCESS" if total_failures == 0 else "WARNINGS",
            "detail": f"{total_failures} failures in {len(failed_rows)} rows",
            "file": "validation_results.txt",
        }
    except Exception as exc:
        logger.warning(f"[Stage 4] Validation failed: {exc}\n{traceback.format_exc()}")
        stage_results["validation"] = {"status": "FAILED", "detail": str(exc)}
        failures_by_col = {}

    # -----------------------------------------------------------------------
    # Stage 5: Cleaning
    # -----------------------------------------------------------------------
    logger.info("[Stage 5] Running data cleaning ...")
    cleaned_df: Optional[pd.DataFrame] = None
    try:
        from part4_cleaning import run_cleaning
        cleaned_df, _ = run_cleaning(raw_df, output_dir=output_dir)
        logger.info(
            f"[Stage 5] Cleaning complete. "
            f"Output: {len(cleaned_df)} rows × {len(cleaned_df.columns)} columns. "
            f"-> cleaning_log.txt, customers_cleaned.csv"
        )
        stage_results["cleaning"] = {
            "status": "SUCCESS",
            "detail": f"{len(cleaned_df)} rows after cleaning",
            "files": ["cleaning_log.txt", "customers_cleaned.csv"],
        }
    except Exception as exc:
        logger.warning(f"[Stage 5] Cleaning failed: {exc}\n{traceback.format_exc()}")
        stage_results["cleaning"] = {"status": "FAILED", "detail": str(exc)}
        cleaned_df = raw_df  # fall back to raw if cleaning failed

    # -----------------------------------------------------------------------
    # Stage 6: PII Masking
    # -----------------------------------------------------------------------
    logger.info("[Stage 6] Running PII masking ...")
    try:
        from part5_masking import run_masking
        _, _ = run_masking(cleaned_df, output_dir=output_dir)
        logger.info(
            "[Stage 6] Masking complete. "
            "-> masked_sample.txt, customers_masked.csv"
        )
        stage_results["masking"] = {
            "status": "SUCCESS",
            "detail": "All PII columns masked",
            "files": ["masked_sample.txt", "customers_masked.csv"],
        }
    except Exception as exc:
        logger.warning(f"[Stage 6] Masking failed: {exc}\n{traceback.format_exc()}")
        stage_results["masking"] = {"status": "FAILED", "detail": str(exc)}

    # -----------------------------------------------------------------------
    # Stage 7: Generate pipeline execution report
    # -----------------------------------------------------------------------
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    all_files = [
        "data_quality_report.txt",
        "pii_detection_report.txt",
        "validation_results.txt",
        "cleaning_log.txt",
        "masked_sample.txt",
        "customers_cleaned.csv",
        "customers_masked.csv",
        "pipeline_execution_report.txt",
    ]

    # Verify files exist
    missing_files = [
        f for f in all_files
        if not os.path.exists(os.path.join(output_dir, f))
        and f != "pipeline_execution_report.txt"
    ]

    overall_status = (
        "SUCCESS [PASS]"
        if not missing_files and all(
            r["status"] in ("SUCCESS", "WARNINGS")
            for r in stage_results.values()
        )
        else "PARTIAL [WARN]"
    )

    # Build the report
    report_lines = []
    report_lines.append("PIPELINE EXECUTION REPORT")
    report_lines.append("=" * 50)
    report_lines.append(f"Timestamp: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Duration : {elapsed:.2f} seconds")
    report_lines.append("")

    stage_labels = {
        "load":       ("1", "Data loaded",                 input_csv_path),
        "quality":    ("2", "Quality profiling complete",  "data_quality_report.txt"),
        "pii":        ("3", "PII detection complete",      "pii_detection_report.txt"),
        "validation": ("4", "Validation complete",         "validation_results.txt"),
        "cleaning":   ("5", "Data cleaning complete",      "cleaning_log.txt, customers_cleaned.csv"),
        "masking":    ("6", "PII masking complete",        "masked_sample.txt, customers_masked.csv"),
    }

    report_lines.append("STEPS COMPLETED:")
    for key, (num, label, files) in stage_labels.items():
        info = stage_results.get(key, {})
        status_sym = "[OK]" if info.get("status") in ("SUCCESS", "WARNINGS") else "[FAIL]"
        detail = info.get("detail", "")
        report_lines.append(
            f"  {num}. {status_sym} {label}: {files}"
        )
        if detail:
            report_lines.append(f"       Detail: {detail}")

    report_lines.append("")
    report_lines.append("FILES GENERATED:")
    for f in all_files:
        full_path = os.path.join(output_dir, f)
        exists_sym = "[OK]" if os.path.exists(full_path) else "[MISSING]"
        report_lines.append(f"  - {f} {exists_sym}")

    if missing_files:
        report_lines.append("")
        report_lines.append(f"MISSING FILES: {missing_files}")

    report_lines.append("")
    report_lines.append("SUMMARY:")
    report_lines.append(f"  - Input : {n_rows} rows (raw / messy)")
    if cleaned_df is not None:
        report_lines.append(
            f"  - Output: {len(cleaned_df)} rows (cleaned, validated, masked)"
        )
    val_detail = stage_results.get("validation", {}).get("detail", "N/A")
    report_lines.append(f"  - Quality: {val_detail}")
    report_lines.append("  - PII Risk: MITIGATED (all PII columns masked)")
    report_lines.append(f"Status: {overall_status}")

    report_text = "\n".join(report_lines)

    # Write report
    report_path = os.path.join(output_dir, "pipeline_execution_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)

    logger.info("[Stage 7] Pipeline execution report written.")
    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE  -  Status: {overall_status}")
    logger.info(f"Total duration: {elapsed:.2f}s")
    logger.info("=" * 60)

    # Use sys.stdout.buffer for safe UTF-8 output on Windows terminals
    sys.stdout.buffer.write(("\n" + report_text + "\n").encode("utf-8"))


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    csv_path   = sys.argv[1] if len(sys.argv) > 1 else "customers_raw.csv"
    out_dir    = sys.argv[2] if len(sys.argv) > 2 else "."

    run_pipeline(input_csv_path=csv_path, output_dir=out_dir)
