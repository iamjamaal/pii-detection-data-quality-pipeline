# Reflection & Governance
## PII Detection & Data Quality Validation Pipeline

---

## 1. Top 5 Data Quality Issues Found

### Issue 1 Multiple Date Formats (Critical)
**What it was:** The `date_of_birth` and `created_date` columns contained at least three different formats: `YYYY-MM-DD` (standard), `MM/DD/YYYY` (US locale), and the literal string `"invalid_date"`. Pandas could not parse these columns as datetime automatically, leaving them as `object` (string) dtype.

**How it was fixed:** A multi-format parser (`_parse_date`) attempted each known format in sequence. Successfully parsed values were re-serialised as `YYYY-MM-DD`. Values matching `"invalid_date"` or otherwise unparseable were set to `NaN` and logged.

**Impact:** Unresolved, this would break any downstream date-based analytics (age calculations, cohort analysis, subscription duration queries). Standardising to ISO 8601 (`YYYY-MM-DD`) makes the column unambiguous for every database, BI tool, and analytics library.

---

### Issue 2  Inconsistent Phone Formats (High)
**What it was:** Five distinct phone formats were present in one column: `XXX-XXX-XXXX`, `(XXX) XXX-XXXX`, `XXX.XXX.XXXX`, `XXXXXXXXXX` (no separators), and `+1-XXX-XXX-XXXX` (E.164-style with country code). Any regex-based lookup or JOIN on phone numbers would silently fail to match equivalent numbers stored in different formats.

**How it was fixed:** All non-digit characters were stripped; a leading `1` was removed for 11-digit US numbers; the remaining 10 digits were reformatted as `XXX-XXX-XXXX`. Numbers that could not be normalised to exactly 10 digits were flagged and left unchanged.

**Impact:** Normalisation enables reliable de-duplication, customer lookups, and SMS/dialler integrations. Without it, a customer with phone `5551234567` in one system and `(555) 123-4567` in another would appear as two different people.

---

### Issue 3  Missing Critical Fields (High)
**What it was:** Six columns had at least one null or empty value: `first_name`, `last_name`, `address`, `income`, `account_status`, and `date_of_birth`. Some of these are operationally critical, you cannot send a letter without an address, and you cannot apply risk-based pricing without income.

**How it was fixed:** A deliberate per-column strategy was applied: string fields (`first_name`, `last_name`, `address`) were filled with the placeholder `[UNKNOWN]` to preserve row count and flag records needing manual review. `income` was set to `0` (conservative, flagged for review). `account_status` was filled with `unknown` (a sentinel value outside valid states, easily filterable). `date_of_birth` was left as `NaN` because inferring a birth date is not feasible.

**Impact:** Row count is preserved (no silent data loss), downstream aggregations do not crash on nulls, and the `[UNKNOWN]` / `unknown` sentinels make it trivial to identify and prioritise remediation.

---

### Issue 4 — Duplicate customer_id (Critical)
**What it was:** Two rows shared `customer_id = 1`. A primary-key constraint violation of this kind invalidates any JOIN, aggregation, or foreign-key reference that assumes uniqueness.

**How it was fixed:** The first occurrence was kept; subsequent duplicates were dropped and their row indices logged. The strategy is documented so the data steward can investigate the root cause (e.g., a merge script that failed to generate a new ID).

**Impact:** Unresolved duplicates would cause double-counting in revenue reports, inflate customer counts in dashboards, and produce incorrect results in any machine-learning model that uses `customer_id` as a join key.

---

### Issue 5  Out-of-Range Values: Negative Income and Age > 150 (High)
**What it was:** One row had `income = -5000` and one row had `date_of_birth = 1850-01-01` (implying an age of ~175 years). Both represent data-entry errors or ETL bugs that produce logically impossible values.

**How it was fixed:** Negative income was corrected to `0` and flagged for review. The implausible birth date was set to `NaN` (the actual date cannot be inferred). A separate flag was raised for the record with `income = 15,000,000`, which is technically possible but worth human review before analysis.

**Impact:** Negative income would corrupt income-based statistics (averages, percentiles). An extreme birth date would skew any age-demographic analysis and could cause integer overflows in age calculation queries.

---

## 2. PII Risk Assessment

### PII Detected
| Column | Type | Why It Is Sensitive |
|---|---|---|
| `first_name` + `last_name` | Direct identifier | Identifies the individual by name |
| `email` | Direct identifier | Enables contact, login credential, phishing vector |
| `phone` | Direct identifier | Enables contact, SIM-swap attacks, social engineering |
| `address` | Direct identifier | Physical location enables stalking, mail fraud |
| `date_of_birth` | Quasi-identifier | Combined with name and address, sufficient for identity theft |
| `income` | Sensitive attribute | Reveals economic status; protected under many privacy laws |

### Damage from a Breach
A breach of this unmasked dataset would give attackers:
- **Phishing campaigns** full email list with names for targeted spear phishing.
- **Identity theft**  name + DOB + address satisfies most "knowledge-based authentication" questions used by banks and utilities.
- **SIM-swap fraud** phone number combined with personal details is sufficient to fool many carriers into transferring a phone number, enabling MFA bypass.
- **Financial fraud**  income data enables targeted credit-card or loan fraud and reveals which customers are high-value targets.
- **Regulatory liability** a breach of this dataset without adequate protection could trigger fines under GDPR (up to 4% of global annual turnover), CCPA, or sector-specific regulations (PCI-DSS, GLBA).

---

## 3. Masking Trade-offs

### What Utility Was Lost
| Masked Column | Lost Capability |
|---|---|
| `email` | Cannot contact customers directly; cannot send OTPs or newsletters |
| `phone` | Cannot run outbound calling campaigns; cannot verify 2FA |
| `address` | Cannot compute geographic segments; cannot mail physical correspondence |
| `first_name`/`last_name` | Cannot personalise outbound communications |
| `date_of_birth` | Cannot compute exact age; only year is preserved for cohort analysis |

### When Masking Is Worth the Trade-off
Masking is the right choice whenever data leaves a controlled environment:
- **Analytics and data science**  business metrics (churn rate, average income by status) can be computed on masked data. The year of birth is sufficient for generation-level demographic analysis.
- **External reporting or vendor sharing**  any third party (marketing agency, BI contractor) should receive masked data only.
- **Testing and staging environments** developers should never work with production PII.
- **GDPR/CCPA compliance**  masked data can be shared across jurisdictions without triggering cross-border data transfer restrictions.

### When You Would NOT Mask
- **Internal customer service** agents need real names, emails, and phones to assist customers.
- **Fraud investigation**  investigators need full PII to verify identity and trace transactions.
- **Regulatory audits** regulators may require access to the unmasked record.
- **Marketing execution**  the team *sending* emails needs unmasked emails, but the analytics team reviewing open rates does not.

The principle is **minimum necessary access**: mask by default, unmask only when there is a documented, audited business need.

---

## 4. Validation Strategy Evaluation

### What the Validators Caught
The custom framework successfully identified:
- Null/empty values in every column
- Non-numeric income, negative income, income above threshold
- Invalid date strings and unparseable dates
- Future `created_date` values
- Duplicate `customer_id`
- Name fields containing non-alphabetic characters
- `account_status` values outside the allowed set
- Phone numbers with digit counts outside 10–15

### What They Missed
- **Cross-column validation**  e.g., a customer aged 15 with income of $110,000 might be valid individually but implausible in combination.
- **Email-domain validation**  the regex checks format but not whether the domain actually exists (DNS MX lookup).
- **Duplicate records by non-ID fields**  two rows could have the same email and different IDs (soft duplicates).
- **Statistical outliers**  income of $93,000 passes all rules but could still be an anomaly for a given customer segment.
- **Referential integrity**  if this table links to a transactions table, orphaned foreign keys would not be detected.

### How to Improve
1. **Cross-column rules** add composite validators (e.g., minimum plausible income given reported age).
2. **Regex + DNS MX lookup** for email validation in environments where external calls are allowed.
3. **Fuzzy deduplication**  use edit-distance on name + address to catch soft duplicates that slipped through ID generation.
4. **Statistical bounds** fit income to a distribution per `account_status` tier and flag Z-score outliers (e.g., |Z| > 3).
5. **Schema versioning** store the validation ruleset alongside the data so auditors can see exactly which rules were applied to each batch.

---

## 5. Production Operations

### Scheduling
For a fintech ingestion pipeline processing customer sign-ups, a reasonable cadence is:
- **Real-time / micro-batch**  validate each new record immediately at point of entry using the validation module as a library call inside the onboarding API.
- **Daily batch**  run the full pipeline on the previous day's file drop at 02:00 UTC before business analytics need the data.
- **On-demand re-scan**  triggered when a new validation rule is added, to retroactively find violations in historical data.

### Failure Handling
| Scenario | Response |
|---|---|
| CSV cannot be loaded | Hard stop; alert data engineering on-call via PagerDuty |
| Validation failure count exceeds threshold (e.g., > 20%) | Halt pipeline, quarantine file, notify data steward |
| Individual field failures | Log, flag rows, continue processing (soft failure) |
| PII masking exception | Hard stop before any output is written; do not produce a partially masked file |

### Monitoring & Alerting
- Emit metrics (failure counts per column, % of rows affected) to a metrics store (Prometheus, Datadog) after each run.
- Set alert thresholds: warn if failure rate rises by > 5 percentage points vs. the 7-day rolling average (drift detection).
- Dashboard: track data completeness and validation pass rate over time to detect upstream data quality degradation.

### Data Versioning
- Store each raw input file with a timestamp suffix (`customers_raw_2024-01-20.csv`).
- Maintain a run log (run_id, timestamp, row count, failure count, output files).
- Use an audit table to record every transformation applied to each row.

### Rollback
- The cleaned and masked CSVs are derived from the raw file; re-running the pipeline with the same input re-produces them deterministically.
- If a bug in the cleaning logic is discovered post-deployment, re-run against the archived raw files with the corrected code.

---

## 6. Lessons Learned

### What Was Surprising
The sheer variety of phone formats in a single small dataset was striking. In production, even more exotic formats appear (extensions, international numbers with varying prefix conventions, toll free numbers). A naive regex cannot cover all cases; a dedicated phone number library (e.g., `libphonenumber`) is worth the dependency in any real system.

The "age > 150" check also highlighted how easy it is for a database timestamp mismatch or a two-digit year ambiguity to produce obviously impossible values that still pass type checks.

### What Was Harder Than Expected
Designing a per-column missing-value strategy that is both defensible and clearly documented proved more nuanced than simply calling `fillna()`. Each decision has a downstream impact: filling `account_status` with `"unknown"` means downstream queries must exclude that value from active customer counts, or they will overcount.

### What Would Be Different Next Time
1. **Agree on a data contract with the upstream source system before ingestion** define expected formats, valid values, and null policies in a shared schema document. This eliminates many surprises at ingestion time.
2. **Build the validator as a library, not a script**  so individual microservices can call `validate_record(row)` at write time rather than discovering problems hours later in a batch job.
3. **Separate "flag for review" from "fix" actions**  a cleaning step that silently sets negative income to `0` could mask a systematic billing error upstream. In production, corrections should require human approval via a review queue.
4. **Include data-lineage tracking from day one** every transformation should record which rule was applied, by whom, and when, so the audit trail is complete if the data ever becomes evidence in a legal or regulatory proceeding.
