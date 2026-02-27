# PII Detection & Data Quality Validation Pipeline

A production-style Python pipeline that profiles, validates, cleans, and masks a messy customer dataset. Built as a data engineering lab exercise covering real-world concerns: data quality, PII governance, and GDPR-compliant data sharing.

---

## Overview

Fintech companies regularly ingest customer data from multiple sources — and it arrives messy. This pipeline automates the full lifecycle:

```
Raw CSV  →  Profile  →  Detect PII  →  Validate  →  Clean  →  Mask  →  Report
```

Each stage is a standalone Python module that can run independently or be orchestrated end-to-end by the pipeline script.

---

## Dataset

`customers_raw.csv`  15 rows of intentionally messy customer data with:

| Issue | Example |
|---|---|
| Missing values | blank `first_name`, `address`, `income` |
| Invalid date strings | `"invalid_date"` in `date_of_birth` |
| Mixed date formats | `01/15/1975` instead of `1975-01-15` |
| Mixed phone formats | `(555) 987-6543`, `555.678.9012`, `5557890123`, `+1-555-789-0123` |
| Duplicate primary key | Two rows with `customer_id = 1` |
| Negative income | `-5000` |
| Income above threshold | `15000000` (> $10M) |
| Extreme age | DOB `1850-01-01` (~176 years old) |
| Invalid category | `account_status = "unknown"` |
| Name casing issues | `PATRICIA`, `jennifer` |
| Future created date | `2027-06-01` |

---

## Requirements

- Python 3.8+
- pandas >= 2.0.0

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## How to Run

Follow these steps in order. Each step generates the output files needed by the next.

---

### Step 0  Install dependencies

```bash
pip install -r requirements.txt
```

---

### Step 1  Profile the raw data

```bash
python part1_data_quality.py customers_raw.csv
```

**Generates:** `data_quality_report.txt`

---

### Step 2  Detect PII

```bash
python part2_pii_detection.py customers_raw.csv
```

**Generates:** `pii_detection_report.txt`

---

### Step 3  Validate the raw data

```bash
python part3_validator.py customers_raw.csv
```

**Generates:** `validation_results.txt`

---

### Step 4  Clean the data

```bash
python part4_cleaning.py customers_raw.csv
```

**Generates:** `cleaning_log.txt`, `customers_cleaned.csv`

---

### Step 5  Mask PII

```bash
python part5_masking.py customers_cleaned.csv
```

**Requires:** `customers_cleaned.csv` (from Step 4)
**Generates:** `customers_masked.csv`, `masked_sample.txt`

---

### Step 6  Run the full pipeline (all steps at once)

```bash
python part6_pipeline.py customers_raw.csv .
```

**Generates:** all of the above in one run, plus `pipeline_execution_report.txt`

---

## Project Structure

```
pii-detection-data-quality-pipeline/
│
├── customers_raw.csv               # Raw messy input dataset (15 rows)
│
├── part1_data_quality.py           # Completeness, types, format, uniqueness profiling
├── part2_pii_detection.py          # Regex-based PII detection and risk assessment
├── part3_validator.py              # Custom per-column validation framework
├── part4_cleaning.py               # Normalisation, missing-value strategy, remediation
├── part5_masking.py                # Column-specific PII masking functions
├── part6_pipeline.py               # End-to-end orchestrator with logging
│
│   (output files are generated when you run the scripts — see How to Run)
│
├── reflection.md                   # Governance, trade-offs, and lessons learned  → [read reflection](reflection.md)
├── requirements.txt
└── README.md
```

---

## Pipeline Stages

### Stage 1  Data Quality Profiling (`part1_data_quality.py`)
- Completeness % per column
- Detected vs expected data types
- Phone and date format diversity
- `customer_id` uniqueness check
- Invalid value detection (literal strings, out-of-range, future dates)
- Severity classification: Critical / High / Medium

### Stage 2  PII Detection (`part2_pii_detection.py`)
- Column-level PII classification (HIGH / MEDIUM risk)
- Regex scanning for email and phone patterns
- Per-row PII inventory
- Breach exposure risk narrative

### Stage 3  Validation (`part3_validator.py`)
- Custom rule engine — no external validation libraries required
- Rules per column: uniqueness, regex, date parsing, range checks, enum membership
- Outputs every failure with row number, value, and violated rule

### Stage 4  Cleaning (`part4_cleaning.py`)
- Phone normalisation → `XXX-XXX-XXXX`
- Date normalisation → `YYYY-MM-DD`
- Name title-casing
- Per-column missing-value strategy (fill / flag / leave)
- Duplicate removal, negative income correction, extreme-age nulling
- Re-validation before/after comparison

### Stage 5  PII Masking (`part5_masking.py`)

| Column | Before | After |
|---|---|---|
| `first_name` | `John` | `J***` |
| `last_name` | `Doe` | `D***` |
| `email` | `john.doe@gmail.com` | `j***@gmail.com` |
| `phone` | `555-123-4567` | `***-***-4567` |
| `address` | `123 Main St New York NY 10001` | `[MASKED ADDRESS]` |
| `date_of_birth` | `1985-03-15` | `1985-**-**` |

Columns **not** masked: `customer_id`, `income`, `account_status`, `created_date`

### Stage 6  Pipeline Orchestration (`part6_pipeline.py`)
- Imports and calls all five modules in sequence
- Per-stage try/except with structured logging
- Generates `pipeline_execution_report.txt`
- Accepts any input CSV path — reusable for other datasets

---

## Sample Results

```
Input:  15 rows (raw)
Output: 14 rows (cleaned, validated, masked)

Quality issues found: 19  (3 Critical, 3 High, 13 Medium)
Validation failures before cleaning: 14 across 10 rows
Validation failures after cleaning:   6 (intentional flags — not errors)

PII coverage:  100% of rows contain at least one PII field
After masking: 0 PII fields exposed in customers_masked.csv
```

---

## Reflection & Governance

See **[reflection.md](reflection.md)** for a full written analysis covering:

- Top 5 data quality issues found and how each was fixed
- PII risk assessment and breach impact analysis
- Masking trade-offs,  when to mask vs. when not to
- Validation strategy evaluation and identified gaps
- Production operations: scheduling, failure handling, monitoring, and rollback

---

## Key Concepts

- **Data profiling**  understanding what's broken before touching it
- **PII detection**  regex-based scanning + column classification
- **Validation**  rule-based quality gates with full failure context
- **ETL cleaning**  documented, auditable transformations
- **PII masking**  structure-preserving anonymisation for safe sharing
- **Pipeline design**  modular, importable, error-tolerant orchestration
- **Governance**  severity tiers, missing-value strategies, review flags
