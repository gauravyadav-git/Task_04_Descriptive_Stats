# Descriptive Statistics Scripts for CSV Data

This repository contains three Python scripts to perform descriptive statistical summaries on CSV datasets. Each script handles nested JSON-like columns by unpacking them automatically before summarizing, supports optional grouping, and outputs results as CSV files.

---

## Included Scripts

### 1. Pure Python Version (`pure_python_descriptive.py`)

- Uses built-in `csv` module only (no third-party dependencies).
- Reads CSV files with robust UTF-8 encoding and error replacement.
- Detects and unpacks nested dictionary-like columns.
- Computes count, unique values, mean, min, max, standard deviation, and most frequent values for each column.
- Supports grouping by one or more columns.
- Processes the first 10 rows by default for quick analysis.
- Outputs summaries to `python_summaries/`.

### 2. Polars Version (`polars_descriptive.py`)

- Uses [Polars](https://pola-rs.github.io/polars/) for fast, memory-efficient data processing.
- Detects and unpacks nested dictionary-like columns.
- Produces the same summary statistics as the pure Python version.
- Supports grouping by columns.
- Suitable for larger datasets where speed matters.
- Outputs summaries to `polars_summaries/`.

### 3. Pandas Version (`pandas_descriptive.py`)

- Uses [Pandas](https://pandas.pydata.org/) and `matplotlib` for data manipulation and optional plotting.
- Detects and unpacks nested dictionary-like columns.
- Computes descriptive stats for all columns and grouped subsets.
- Offers optional histogram and bar chart generation.
- Outputs summaries to `pandas_summaries/`.

---

## Usage

Run any script using Python 3.7+:

```bash
python pure_python_descriptive.py
python polars_descriptive.py
python pandas_descriptive.py
