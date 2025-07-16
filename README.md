# Task_04_Descriptive_Stats 

This repository contains Python scripts for performing descriptive statistical summaries on CSV datasets. The scripts are designed to handle datasets with nested JSON-like columns, automatically unpacking them before summarization. The focus is on ease of use, reliability, and providing meaningful insights from raw CSV data.

---

## Features

- **Pure Python Implementation**
  - Reads CSV files using built-in `csv` module.
  - Detects and unpacks columns containing nested dictionary-like strings.
  - Computes summary statistics for each column:
    - Count, unique values, mean, min, max, standard deviation, and most frequent value.
  - Supports optional grouping by one or more columns.
  - Processes only the first 10 rows for quick analysis on large datasets.
  - Saves summary output as CSV.

- **Polars Implementation (Optional)**
  - Uses the [Polars](https://pola-rs.github.io/polars/) library for fast DataFrame operations.
  - Detects unpackable columns and expands them similarly.
  - Produces the same summary statistics as the pure Python version.
  - Designed for larger datasets where speed is important.

---

## Usage

### Pure Python Script

Run the Python script `pure_python_descriptive.py`:

```bash
python pure_python_descriptive.py
