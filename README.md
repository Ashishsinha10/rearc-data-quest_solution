# Rearc Data Quest – Databricks Edition

An end-to-end data engineering solution built using **Databricks, PySpark, Spark SQL, Python, and Lakeflow Spark Declarative Pipelines**.

## Project Overview

This project ingests BLS productivity data and DataUSA population data, processes them through a Medallion Architecture, and produces analytical datasets.

## Architecture

**Ingestion → Bronze → Silver → Gold**

- **Ingestion:** Dynamic BLS file discovery and DataUSA API ingestion.
- **Bronze:** Raw source data ingestion.
- **Silver:** Data cleansing, standardization, and metadata enrichment.
- **Gold:** Business-level analytical datasets.

## Technology Stack

- Databricks Free Edition
- Python and PySpark
- Spark SQL
- Delta Lake
- Lakeflow Spark Declarative Pipelines
- Git and GitHub

## Analytical Questions

1. Calculate mean and standard deviation of US population from 2013–2018.
2. Identify the year with the highest quarterly productivity total for each BLS series.
3. Join annual productivity values for series `PRS30006032` with US population data.

Solutions are implemented using both SQL and PySpark.

## Project Structure

- `rearc-data-quest/docs/` – Documentation and screenshots.
- `rearc-data-quest/ingestion/` – Source ingestion notebooks.
- `rearc-data-quest/pipelines/` – Lakeflow pipeline implementation.
- `rearc-data-quest/sql/` – SQL analytical solutions.
- `rearc-data-quest/src/` – Bronze, Silver, and Gold transformations.
- `rearc-data-quest/tests/` – Gold output validation notebook.

## Documentation

See [`docs/PROCESS.md`](rearc-data-quest/docs/PROCESS.md) for architecture, implementation decisions, validation, and project retrospective.

## Author

Ashish Sinha
