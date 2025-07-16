import csv
import os
import time
import ast
from collections import defaultdict, Counter
from statistics import mean, stdev

# ------------------- Helpers -------------------

def is_number(s):
    try:
        float(str(s).replace(',', '').strip())
        return True
    except:
        return False

def to_number(s):
    return float(str(s).replace(',', '').strip())

def load_csv(filepath):
    with open(filepath, mode='r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        data = [row for row in reader if any(row.values())]
    return data

def write_summary_csv(summary, output_file):
    if not summary:
        print("No summary data to write.")
        return

    keys = summary[0].keys()
    os.makedirs("python_summaries", exist_ok=True)

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(summary)

    print(f"✅ Summary written to {output_file}")

# ------------------- Unpacking -------------------

def detect_unpackable_columns(data, sample_size=5):
    if not data:
        return []
    columns = data[0].keys()
    unpackable = []
    for col in columns:
        samples = [row[col] for row in data[:sample_size] if col in row and row[col].strip() != '']
        for val in samples:
            try:
                parsed = ast.literal_eval(val)
                # Detect if dict of dicts
                if isinstance(parsed, dict) and all(isinstance(v, dict) for v in parsed.values()):
                    unpackable.append(col)
                    break
            except:
                continue
    return unpackable

def unpack_column(data, column, prefix):
    unpacked_data = []
    for row in data:
        raw = row.get(column, '')
        try:
            nested = ast.literal_eval(raw)
        except:
            # If can't parse, just keep original row without unpacking for this one
            unpacked_data.append(row)
            continue
        if not isinstance(nested, dict):
            unpacked_data.append(row)
            continue
        # For each key/subdict, create a new row
        for key, subdict in nested.items():
            if not isinstance(subdict, dict):
                continue
            new_row = row.copy()
            new_row[f"{prefix}_key"] = key
            for k, v in subdict.items():
                new_row[f"{prefix}_{k}"] = v
            # Remove original packed column value to avoid confusion
            if column in new_row:
                del new_row[column]
            unpacked_data.append(new_row)
    return unpacked_data

# ------------------- Summarization -------------------

def summarize_data(data, group_label="full_dataset"):
    if not data:
        print("❌ No data loaded.")
        return []

    summary = []
    columns = data[0].keys()

    for col in columns:
        # Safely convert to string before strip
        col_values = [row[col] for row in data if str(row.get(col, '')).strip() != '']
        count = len(col_values)
        unique = len(set(col_values))

        numeric_values = []
        for v in col_values:
            if is_number(v):
                numeric_values.append(to_number(v))

        if numeric_values:
            mean_val = round(mean(numeric_values), 4)
            min_val = min(numeric_values)
            max_val = max(numeric_values)
            std_val = round(stdev(numeric_values), 4) if len(numeric_values) > 1 else "NA"
            most_freq = "NA"
        else:
            mean_val = min_val = max_val = std_val = "NA"
            freq = Counter(str(v) for v in col_values)
            most_freq = f"{freq.most_common(1)[0][0]} ({freq.most_common(1)[0][1]})" if freq else "NA"

        summary.append({
            "group": group_label,
            "column": col,
            "count": count,
            "unique": unique,
            "mean": mean_val,
            "min": min_val,
            "max": max_val,
            "std_dev": std_val,
            "most_freq": most_freq
        })

    return summary

def summarize_grouped_data(data, group_cols):
    if not data or not group_cols:
        return []

    summary = []
    all_columns = list(data[0].keys())
    value_columns = [c for c in all_columns if c not in group_cols]

    groups = defaultdict(list)
    for row in data:
        key = tuple(row[col] for col in group_cols)
        groups[key].append(row)

    for group_key, rows in groups.items():
        label = ", ".join(f"{col}={val}" for col, val in zip(group_cols, group_key))
        group_summary = summarize_data(rows, group_label=label)
        summary.extend(group_summary)

    return summary

# ------------------- Main -------------------

if _name_ == "_main_":
    print("📊 PURE PYTHON DESCRIPTIVE STATISTICS")

    filepath = input("Enter CSV file path (e.g., ./data/2024_fb_ads_president_scored_anon.csv): ").strip()
    dataset_name = os.path.splitext(os.path.basename(filepath))[0]
    group_input = input("Enter group keys (comma-separated or leave blank): ").strip()
    group_keys = [g.strip() for g in group_input.split(",") if g.strip()]

    start = time.perf_counter()

    print("📥 Loading data...")
    data = load_csv(filepath)

    # Limit data to first 10 rows for performance
    data = data[:10]

    print("🔍 Detecting unpackable columns...")
    unpack_cols = detect_unpackable_columns(data)
    for col in unpack_cols:
        prefix = col.split("_")[0]
        print(f"🔄 Unpacking column: {col}")
        data = unpack_column(data, col, prefix)

    print("🔎 Summarizing full dataset...")
    summaries = summarize_data(data)

    if group_keys:
        print(f"🔎 Summarizing grouped by {group_keys} ...")
        summaries.extend(summarize_grouped_data(data, group_keys))

    out_file = f"python_summaries/summary_{dataset_name}.csv"
    write_summary_csv(summaries, out_file)

    elapsed = time.perf_counter() - start
    print(f"⏱ Done in {elapsed:.2f} seconds.")