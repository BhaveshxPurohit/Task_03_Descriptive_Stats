import polars as pl
from collections import Counter

# === Configuration ===
csv_files = {
    "2024_fb_posts_president_scored_anon.csv": r"C:\Users\puroh\OneDrive\Documents\Syracuse\RA\Task_03_Descriptive_Stats\Data\2024_fb_posts_president_scored_anon.csv"
}
ROW_LIMIT = 500

# === Helpers ===
def is_numeric_dtype(dtype):
    return dtype in (
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64
    )

def print_numeric_stats(df):
    if df.is_empty() or not df.columns:
        print("  ⚠️ No numeric columns found.")
        return
    summary = df.describe()
    for col in df.columns:
        if col not in summary.columns:
            continue
        col_summary = summary.select(col).to_series().to_list()
        print(f"  📊 {col} -> count: {col_summary[0]}, mean: {col_summary[1]}, min: {col_summary[2]}, max: {col_summary[3]}, std: {col_summary[4]}")

def print_non_numeric_stats(df):
    if df.is_empty() or not df.columns:
        print("  ⚠️ No non-numeric columns found.")
        return
    for col in df.columns:
        series = df[col].drop_nulls()
        if series.is_empty():
            continue
        value_counts = series.value_counts().sort("count", descending=True)
        top_val = value_counts[0, col]
        freq = value_counts[0, "count"]
        print(f"  🔠 {col} -> count: {series.len()}, unique: {series.n_unique()}, top: {top_val}, freq: {freq}")

def print_overall_numeric(values, label=""):
    if not values:
        print(f"⚠️ No numeric data to summarize for {label}.")
        return
    s = pl.Series("values", values, strict=False)
    print(f"📊 Overall Numeric Stats ({label}):")
    print(f"  Count: {s.len()}")
    print(f"  Nulls: {s.null_count()}")
    print(f"  Mean: {s.mean():.4f}")
    print(f"  Std:  {s.std():.4f}")
    print(f"  Min:  {s.min()}")
    print(f"  Max:  {s.max()}")

def print_overall_non_numeric(values, label=""):
    if not values:
        print(f"⚠️ No non-numeric data to summarize for {label}.")
        return
    s = pl.Series("non_numeric", values).drop_nulls()
    if s.is_empty():
        print(f"⚠️ No non-numeric data to summarize for {label}.")
        return
    vc = s.value_counts().sort("count", descending=True)
    top_val = vc[0, "non_numeric"]
    freq = vc[0, "count"]
    print(f"🔠 Overall Non-Numeric Stats ({label}):")
    print(f"  Total: {s.len()}, Unique: {s.n_unique()}, Top: {top_val}, Freq: {freq}")

# === Part Processor ===
def process_file_part(file_name, path, part):
    print(f"\n==== 📂 File: {file_name} | Part {part} ====")
    try:
        df = pl.read_csv(path).head(ROW_LIMIT)
    except Exception as e:
        print(f"  ⚠️ Failed to load file: {e}")
        return [], []

    numeric_cols = [col for col in df.columns if is_numeric_dtype(df[col].dtype)]
    non_numeric_cols = [col for col in df.columns if not is_numeric_dtype(df[col].dtype)]

    if part == 1:
        print("\n-- Numeric Stats Per Column --")
        print_numeric_stats(df.select(numeric_cols))

        print("\n-- Non-Numeric Stats Per Column --")
        print_non_numeric_stats(df.select(non_numeric_cols))

        numeric_vals = [
            val for col in numeric_cols if col not in ("Facebook_Id", "post_id")
            for val in df[col].drop_nulls().to_list()
        ]
        non_numeric_vals = [
            val for col in non_numeric_cols if col not in ("Facebook_Id", "post_id")
            for val in df[col].drop_nulls().to_list()
        ]
        return numeric_vals, non_numeric_vals

    # === Grouped by Facebook_Id or (Facebook_Id, post_id)
    group_cols = []
    if part == 2:
        group_cols = ["Facebook_Id"]
    elif part == 3:
        group_cols = ["Facebook_Id", "post_id"]

    if not all(col in df.columns for col in group_cols):
        print(f"  ⚠️ Missing required grouping columns: {group_cols}")
        return [], []

    grouped = df.group_by(group_cols)
    numeric_grouped = grouped.mean().select([
        col for col in numeric_cols if col not in group_cols
    ]) if numeric_cols else pl.DataFrame()

    non_numeric_grouped = []
    if non_numeric_cols:
        for _, sub_df in grouped:
            non_numeric_grouped.append(sub_df.select([
                col for col in non_numeric_cols if col not in group_cols
            ]))
    non_numeric_combined = pl.concat(non_numeric_grouped) if non_numeric_grouped else pl.DataFrame()

    print("\n-- Numeric Stats Per Column (Grouped) --")
    print_numeric_stats(numeric_grouped)

    print("\n-- Non-Numeric Stats Per Column (Grouped) --")
    print_non_numeric_stats(non_numeric_combined)

    numeric_vals = [
        val for col in numeric_grouped.columns
        for val in numeric_grouped[col].drop_nulls().to_list()
    ]
    non_numeric_vals = [
        val for col in non_numeric_combined.columns
        for val in non_numeric_combined[col].drop_nulls().to_list()
    ]
    return numeric_vals, non_numeric_vals

# === Master Runner ===
def run_analysis(part, label):
    print(f"\n====================== 📊 PART {part} ANALYSIS ({label}) ======================\n")
    all_numeric_vals = []
    all_non_numeric_vals = []

    for file_name, path in csv_files.items():
        numeric_vals, non_numeric_vals = process_file_part(file_name, path, part)
        all_numeric_vals.extend(numeric_vals)
        all_non_numeric_vals.extend(non_numeric_vals)

    print()
    print_overall_numeric(all_numeric_vals, label)
    print()
    print_overall_non_numeric(all_non_numeric_vals, label)
    print()

# === Run All Parts ===
run_analysis(1, "No Aggregation")
run_analysis(2, "Grouped by Facebook_Id")
run_analysis(3, "Grouped by Facebook_Id & post_id")
