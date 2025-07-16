import pandas as pd
import matplotlib.pyplot as plt
import chardet
import os
import time
import ast

# ------------------ Helpers ------------------

def detect_encoding(filepath):
    with open(filepath, 'rb') as f:
        raw = f.read(10000)
    result = chardet.detect(raw)
    return result['encoding'] or 'utf-8'

def load_csv(filepath):
    try:
        encoding = detect_encoding(filepath)
        print(f"🔍 Detected encoding: {encoding}")
        return pd.read_csv(filepath, encoding=encoding).dropna(how='all')
    except UnicodeDecodeError:
        print("⚠ Encoding issue detected. Retrying with UTF-8 and fallback open().")
        with open(filepath, mode='r', encoding='utf-8', errors='replace') as f:
            return pd.read_csv(f).dropna(how='all')

def detect_unpackable_columns(df, sample_size=5):
    unpackable = []
    for col in df.columns:
        samples = df[col].dropna().astype(str).head(sample_size)
        for val in samples:
            try:
                parsed = ast.literal_eval(val.strip())
                if isinstance(parsed, dict) and all(isinstance(v, dict) for v in parsed.values()):
                    unpackable.append(col)
                    break
            except:
                continue
    return unpackable

def unpack_column(df, col, prefix):
    rows = []
    for _, row in df.iterrows():
        raw = row[col]
        try:
            nested = ast.literal_eval(str(raw).strip())
        except:
            continue
        for subkey, subvals in nested.items():
            if not isinstance(subvals, dict):
                continue
            new_row = row.to_dict()
            new_row[f"{prefix}_key"] = subkey
            for k, v in subvals.items():
                new_row[f"{prefix}_{k}"] = v
            rows.append(new_row)
    return pd.DataFrame(rows)

# ------------------ Summary ------------------

def summarize_dataframe(df, group_label):
    summary = []
    for col in df.columns:
        s = df[col].dropna()
        col_stats = {
            "column": col,
            "count": s.count(),
            "unique": s.nunique(),
            "mean": round(s.mean(), 4) if pd.api.types.is_numeric_dtype(s) else "NA",
            "min": s.min() if pd.api.types.is_numeric_dtype(s) else "NA",
            "max": s.max() if pd.api.types.is_numeric_dtype(s) else "NA",
            "std_dev": round(s.std(), 4) if pd.api.types.is_numeric_dtype(s) else "NA",
            "most_freq": f"{s.mode().iloc[0]} ({s.value_counts().iloc[0]})" if not pd.api.types.is_numeric_dtype(s) and not s.empty else "NA",
            "group": group_label
        }
        summary.append(col_stats)
    return summary

# ------------------ Main ------------------

if __name__ == "__main__":
    print("📊 PANDAS DESCRIPTIVE STATISTICS")
    filepath = input("Enter CSV path (e.g., ./data/2024_fb_ads_president_scored_anon.csv): ").strip()
    dataset_name = os.path.splitext(os.path.basename(filepath))[0]
    group_input = input("Enter group keys (comma-separated or leave blank): ").strip()
    group_keys = [g.strip() for g in group_input.split(",") if g.strip()]

    print("📥 Loading data...")
    df = load_csv(filepath)

    print("🔍 Unpacking nested columns (if any)...")
    unpack_cols = detect_unpackable_columns(df)
    for col in unpack_cols:
        df = unpack_column(df, col, col.split("_")[0])
        df.drop(columns=[col], inplace=True)

    print("📊 Summarizing...")
    start = time.perf_counter()
    summaries = summarize_dataframe(df, group_label="full_dataset")

    if group_keys:
        grouped = df.groupby(group_keys)
        for group_val, group_df in grouped:
            label = ", ".join(f"{k}={v}" for k, v in zip(group_keys, group_val)) if isinstance(group_val, tuple) else f"{group_keys[0]}={group_val}"
            summaries.extend(summarize_dataframe(group_df, group_label=label))

    os.makedirs("pandas_summaries", exist_ok=True)
    out_file = f"pandas_summaries/pandas_summary_{dataset_name}.csv"
    pd.DataFrame(summaries).to_csv(out_file, index=False)
    print(f"✅ Summary saved to {out_file}")

    print(f"⏱ Done in {time.perf_counter() - start:.2f} seconds")