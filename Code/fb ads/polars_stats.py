import polars as pl

# === Configuration ===
csv_files = {
    "main_ads_cleaned.csv": r"C:\Users\puroh\OneDrive\Documents\Syracuse\RA\Task_03_Descriptive_Stats\Unpacked Data\fb ads\main_ads_cleaned.csv",
    "unpacked_demographics.csv": r"C:\Users\puroh\OneDrive\Documents\Syracuse\RA\Task_03_Descriptive_Stats\Unpacked Data\fb ads\unpacked_demographics.csv",
    "unpacked_platforms.csv": r"C:\Users\puroh\OneDrive\Documents\Syracuse\RA\Task_03_Descriptive_Stats\Unpacked Data\fb ads\unpacked_platforms.csv",
    "unpacked_mentions.csv": r"C:\Users\puroh\OneDrive\Documents\Syracuse\RA\Task_03_Descriptive_Stats\Unpacked Data\fb ads\unpacked_mentions.csv",
    "unpacked_delivery_by_region.csv": r"C:\Users\puroh\OneDrive\Documents\Syracuse\RA\Task_03_Descriptive_Stats\Unpacked Data\fb ads\unpacked_delivery_by_region.csv",
}
ROW_LIMIT = 500

# === Helpers ===
def is_numeric_dtype(dtype):
    return dtype.is_numeric()

def print_numeric_stats(df):
    if df.is_empty() or not df.columns:
        print("  ⚠️ No numeric columns found.")
        return
    for col in df.columns:
        s = df[col].drop_nulls()
        print(f"  📊 {col} -> count: {s.len()}, mean: {s.mean()}, min: {s.min()}, max: {s.max()}, std: {s.std()}")

def print_non_numeric_stats(df):
    if df.is_empty() or not df.columns:
        print("  ⚠️ No non-numeric columns found.")
        return
    for col in df.columns:
        s = df[col].drop_nulls()
        if s.is_empty():
            continue
        vc = s.value_counts().sort("count", descending=True)
        print(f"  🔠 {col} -> count: {s.len()}, unique: {s.n_unique()}, top: {vc[0, col]}, freq: {vc[0, 'count']}")

def print_overall_stats(values, is_numeric):
    s = pl.Series("vals", values).drop_nulls()
    if s.is_empty():
        print("  ⚠️ No data for global statistics.")
        return
    if is_numeric:
        print(f"  📉 Global ➡️ Overall Numeric Stats:\n  count: {s.len()}\n  mean: {s.mean()}\n  min: {s.min()}\n  max: {s.max()}\n  std: {s.std()}")
    else:
        vc = s.value_counts().sort("count", descending=True)
        print(f"  📝 Global ➡️ Overall Non-Numeric Stats:\n  total entries: {s.len()}\n  unique values: {s.n_unique()}\n  top: {vc[0, 'vals']}\n  freq: {vc[0, 'count']}")

# === Part Processor ===
def process_file_part(file_name, path, part):
    print(f"\n==== 📂 File: {file_name} | Part {part} ====")
    try:
        df = pl.read_csv(path).head(ROW_LIMIT)
    except Exception as e:
        print(f"  ⚠️ Failed to load file: {e}")
        return pl.DataFrame(), pl.DataFrame()

    numeric_cols = [col for col in df.columns if is_numeric_dtype(df[col].dtype)]
    non_numeric_cols = [col for col in df.columns if not is_numeric_dtype(df[col].dtype)]

    if part == 1:
        print("\n-- Numeric Stats Per Column --")
        print_numeric_stats(df.select(numeric_cols))

        print("\n-- Non-Numeric Stats Per Column --")
        print_non_numeric_stats(df.select(non_numeric_cols))

        return df.select(numeric_cols), df.select(non_numeric_cols)

    # Grouping logic
    group_cols = []
    if part == 2:
        group_cols = ["page_id"]
    elif part == 3:
        group_cols = ["page_id", "ad_id"]

    if not all(col in df.columns for col in group_cols):
        print(f"  ⚠️ Missing required grouping columns: {group_cols}")
        return pl.DataFrame(), pl.DataFrame()

    grouped = df.group_by(group_cols)
    numeric_agg = grouped.mean().select([col for col in numeric_cols if col not in group_cols]) if numeric_cols else pl.DataFrame()

    non_numeric_flat = []
    if non_numeric_cols:
        for _, group_df in grouped:
            non_numeric_flat.append(group_df.select(non_numeric_cols))
    non_numeric_df = pl.concat(non_numeric_flat) if non_numeric_flat else pl.DataFrame()

    print("\n-- Numeric Stats Per Column (Grouped) --")
    print_numeric_stats(numeric_agg)

    print("\n-- Non-Numeric Stats Per Column (Grouped) --")
    print_non_numeric_stats(non_numeric_df)

    return numeric_agg, non_numeric_df

# === Master Runner ===
def run_analysis(part):
    print(f"\n====================== 📊 PART {part} ANALYSIS ======================\n")
    all_numeric_vals = []
    all_non_numeric_vals = []

    for file_name, path in csv_files.items():
        numeric_df, non_numeric_df = process_file_part(file_name, path, part)

        for col in numeric_df.columns:
            if col not in ("page_id", "ad_id"):
                all_numeric_vals.extend(numeric_df[col].drop_nulls().to_list())

        for col in non_numeric_df.columns:
            if col not in ("page_id", "ad_id"):
                all_non_numeric_vals.extend(non_numeric_df[col].drop_nulls().to_list())

    print(f"\n====================== 🌍 Overall Global Stats (All Files Combined) ======================")
    print_overall_stats(all_numeric_vals, is_numeric=True)
    print_overall_stats(all_non_numeric_vals, is_numeric=False)

# === Run All Parts ===
for part in [1, 2, 3]:
    run_analysis(part)
