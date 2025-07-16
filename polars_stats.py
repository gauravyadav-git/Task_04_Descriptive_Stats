import polars as pl
import os
import time
import ast
import chardet
import matplotlib.pyplot as plt

# ------------------ Helpers ------------------

def detect_encoding(filepath):
    with open(filepath, 'rb') as f:
        raw = f.read(10000)
    result = chardet.detect(raw)
    return result['encoding'] or 'utf-8'

def load_csv(filepath):
    encoding = detect_encoding(filepath)
    return pl.read_csv(filepath, encoding=encoding)

def is_numeric_dtype(dtype):
    return dtype in {
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64
    }

def detect_unpackable_columns(df, sample_size=5):
    unpackable = []
    for col in df.columns:
        samples = df[col].limit(sample_size).to_list()
        for val in samples:
            try:
                if isinstance(val, str):
                    parsed = ast.literal_eval(val)
                    if isinstance(parsed, dict) and all(isinstance(v, dict) for v in parsed.values()):
                        unpackable.append(col)
                        break
            except:
                continue
    return unpackable

def unpack_column(df, column, prefix):
    unpacked = []
    for row in df.iter_rows(named=True):
        raw = row[column]
        try:
            nested = ast.literal_eval(str(raw))
        except:
            continue
        for key, subvals in nested.items():
            new_row = row.copy()
            new_row[f"{prefix}_key"] = key
            if isinstance(subvals, dict):
                for k, v in subvals.items():
                    new_row[f"{prefix}_{k}"] = v
            unpacked.append(new_row)
    return pl.DataFrame(unpacked)

# ------------------ Summary ------------------

def summarize_df(df, label):
    summary = []
    for col in df.columns:
        s = df[col]
        dtype = s.dtype
        count = s.len() - s.null_count()
        unique = s.n_unique()
        mean_val = min_val = max_val = std_val = most_freq = "NA"

        if is_numeric_dtype(dtype):
            mean_val = round(s.mean(), 4) if s.mean() is not None else "NA"
            min_val = s.min()
            max_val = s.max()
            std_raw = s.std()
            std_val = round(std_raw, 4) if std_raw is not None else "NA"
        else:            
            try:
                vc_df = (df.select(pl.col(col).value_counts().alias("vc"))
                         .unnest("vc")
                         .sort("count", descending=True))
                if vc_df.height > 0:
                    top_val = vc_df[col][0]
                    top_count = vc_df["count"][0]
                    most_freq = f"{top_val} ({top_count})"
            except Exception:
                most_freq = "NA"



        summary.append({
            "column": col,
            "count": count,
            "unique": unique,
            "mean": mean_val,
            "min": min_val,
            "max": max_val,
            "std_dev": std_val,
            "most_freq": most_freq,
            "group": label
        })
    return summary


# ------------------ Main ------------------

if __name__ == "__main__":
    print("📊 POLARS DESCRIPTIVE STATISTICS")
    filepath = input("Enter CSV path (e.g., ./data/2024_fb_ads_president_scored_anon.csv): ").strip()
    dataset_name = os.path.splitext(os.path.basename(filepath))[0]
    group_input = input("Enter group keys (comma-separated or leave blank): ").strip()
    group_keys = [g.strip() for g in group_input.split(",") if g.strip()]

    print("📥 Loading data...")
    df = load_csv(filepath).drop_nulls()

    print("🔍 Detecting unpackable columns...")
    unpack_cols = detect_unpackable_columns(df)
    for col in unpack_cols:
        df = unpack_column(df, col, col.split("_")[0])
        df = df.drop(col)

    print("📊 Summarizing...")
    start = time.perf_counter()
    all_summary = summarize_df(df, "full_dataset")

    if group_keys:
        try:
            grouped = df.groupby(group_keys)
            for group_vals, group_df in grouped:
                label = ", ".join(f"{k}={v}" for k, v in zip(group_keys, group_vals)) if isinstance(group_vals, tuple) else f"{group_keys[0]}={group_vals}"
                all_summary.extend(summarize_df(group_df, label))
        except Exception as e:
            print(f"⚠ Grouping failed: {e}")

    os.makedirs("polars_summaries", exist_ok=True)
    output_file = f"polars_summaries/polars_summary_{dataset_name}.csv"
    pl.DataFrame(all_summary).write_csv(output_file)
    print(f"✅ Summary saved to {output_file}")

    print(f"⏱ Done in {time.perf_counter() - start:.2f} seconds")