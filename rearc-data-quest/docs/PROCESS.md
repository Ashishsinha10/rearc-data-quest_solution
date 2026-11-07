# Rearc Data Quest — Databricks Edition

## 1. Project Overview

This project implements an end-to-end data engineering solution using BLS productivity data and DataUSA population data.

The solution includes programmatic ingestion, data validation, medallion architecture, Lakeflow Spark Declarative Pipelines, and analytical Gold datasets.

## 2. Technology Stack

- Databricks Free Edition
- Python
- PySpark
- Spark SQL
- Lakeflow Spark Declarative Pipelines
- Unity Catalog
- Databricks Volumes
- Git and GitHub
- BLS public data
- DataUSA API

## 3. Architecture

The solution follows a separated ingestion and transformation architecture.

### Data ingestion

BLS website and DataUSA API are the external sources.

Separate Python notebooks perform:

- Dynamic BLS source-file discovery.
- BLS file downloads.
- DataUSA population API extraction.
- SHA256 checksum generation.
- Manifest maintenance.
- Idempotent ingestion and file-change detection.
- Storage in Unity Catalog Volumes.

### Data transformation

The Lakeflow pipeline processes the landed files using the medallion architecture.

**Bronze**

- Ingest raw BLS productivity data.
- Ingest BLS metadata files.
- Ingest DataUSA population JSON.
- Preserve source values for downstream processing.

**Silver**

- Clean and standardize BLS records.
- Convert productivity values into appropriate numeric types.
- Prepare BLS series metadata and readable labels.
- Standardize population records.
- Apply data-quality expectations to critical fields.

**Gold**

- Q1: US population statistics.
- Q2: Productivity analysis by series and year.
- Q3: Productivity annual values joined with US population.

Additional quality views validate uniqueness of critical BLS business keys.

## 4. Source Data and Ingestion

### BLS

Source: https://download.bls.gov/pub/time.series/pr/

The ingestion process dynamically discovers source files rather than relying on a fixed list.

It maintains a manifest containing file metadata, file size, and SHA256 checksums.

Idempotency was tested:

- Initial run downloaded 12 files.
- Second run downloaded 0 files and identified 12 unchanged files.
- All 12 files matched their manifest checksums.

### DataUSA

Population data was downloaded using the DataUSA API.

The JSON response was stored in the Unity Catalog Volume.

The dataset contains 11 US population records across the available years from 2013 to 2024. The 2020 observation is absent from this source response and was not artificially generated.

## 5. Analytical Questions

### Q1 — Population statistics

Calculate the mean and standard deviation of US population from 2013 through 2018, inclusive.

**Validated result:**

- Number of years: 6
- Mean population: 322,069,808
- Sample standard deviation (STDDEV_SAMP): approximately 4,158,441.04

### Q2 — Productivity analysis

For each BLS series, calculate the year with the highest sum of quarterly values.

**Implementation considerations:**

- Exclude Q05 annual-average records from quarterly aggregation.
- Aggregate Q01 through Q04.
- Include readable series labels.
- Use deterministic tie-breaking by selecting the earlier year when totals are equal.

**Validation:**

- 237 output records.
- 237 distinct series.
- No duplicate series.
- Independent calculation confirmed the winning years and totals.

### Q3 — Productivity and population join

Analyze BLS series PRS30006032 for period Q01 and join annual productivity values with US population.

The join preserves productivity records when population is unavailable.

**Validated result:**

- 39 output records.
- Six records for 2013–2018 have matching population values.
- Annual productivity values and population values were successfully joined for those six years.

## 6. SQL and PySpark

All three analytical questions (Q1, Q2 and Q3) have been implemented in both Spark SQL and PySpark.

- SQL implementations are maintained in `sql/gold_analytics_sql`.
- PySpark implementations are maintained in `src/gold/gold_analytics`.

The selected Gold outputs are materialized through the Lakeflow Spark Declarative Pipeline. Both implementations were executed and validated, providing alternative approaches to the same analytical requirements.

## 7. Design Decisions and Trade-offs

- Ingestion is separated from declarative transformations.
- File downloads and API calls are not embedded inside pipeline dataset definitions.
- SHA256 checksums support source-file change detection.
- The manifest supports idempotent ingestion.
- BLS identifiers are preserved as strings to avoid losing leading zeros.
- Q05 is excluded from quarterly productivity aggregation.
- Deterministic ranking makes Q2 results reproducible.
- A left join is used for Q3 to preserve productivity records.
- Data is stored in Unity Catalog Volumes and managed tables.
- Data-quality expectations enforce critical schema and non-null requirements.
- Separate quality views check uniqueness of BLS data and series business keys.

## 8. Testing and Validation

The following validations were performed:

**Ingestion**
- BLS ingestion and manifest validation.
- BLS idempotency test.
- SHA256 checksum verification.
- DataUSA record validation.

**Pipeline**
- Bronze, Silver and Gold pipeline execution.
- Required-column schema validation.
- Non-null key validation for critical datasets.
- Lakeflow data-quality expectations evaluated successfully.
- Uniqueness checks for BLS data and BLS series business keys.
- Successful Lakeflow pipeline run with 19 datasets.

**Analytical outputs**
- Q1 population statistics validation.
- Q2 row-count and uniqueness validation.
- Independent Q2 winning-year and quarterly-total calculation.
- Q3 output and six-year population-match validation.

The `tests/gold_output_validation` notebook contains executable PySpark assertions for the Gold outputs. All three Gold validation sections completed successfully.

## 9. AI Usage

AI assistance was used for:

- Reviewing architecture and design choices.
- Debugging Python and SQL errors.
- Improving PySpark transformations.
- Validating analytical query logic.
- Reviewing documentation and implementation decisions.

All generated or suggested code was reviewed, executed, and validated against the project data.

## 10. Retrospective

### What went well

- Dynamic BLS ingestion was implemented.
- Idempotent file processing was verified.
- Bronze, Silver and Gold transformations were implemented.
- Lakeflow expectations were registered and evaluated.
- Uniqueness quality checks were added.
- Analytical results were independently validated.

### Future improvements

- Automate ingestion and pipeline orchestration through Databricks Jobs.
- Extend data-quality expectations to additional datasets and Gold outputs.
- Add CI/CD deployment.
- Add dashboards and Genie-based analytics.
- Improve operational alerting and observability.

## 11. Repository Structure

The project is organized into the following areas:

- `ingestion/` — source ingestion notebooks and utilities.
- `pipelines/` — Lakeflow pipeline source code and notebook.
- `src/bronze/` — Bronze development and inspection.
- `src/silver/` — Silver transformations.
- `src/gold/` — Gold analytics.
- `sql/` — SQL implementations for Q1, Q2 and Q3.
- `tests/` — `gold_output_validation` notebook for Gold output validation.
- `docs/` — project documentation.
- `docs/screenshots/` — screenshots of pipeline execution, Bronze, Silver, Gold and analytical results.

---

**Project:** Rearc Data Quest — Databricks Edition  
**Architecture:** Ingestion → Bronze → Silver → Gold  
**Platform:** Databricks Free Edition