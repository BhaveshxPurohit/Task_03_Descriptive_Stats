import pandas as pd
from collections import Counter

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
def print_numeric_stats(df):
    if df.empty or df.shape[1] == 0:
        print("  ⚠️ No numeric columns found.")
        return
    desc = df.describe().T
    for col, row in desc.iterrows():
        print(f"  📊 {col} -> count: {int(row['count'])}, mean: {row['mean']}, min: {row['min']}, max: {row['max']}, std: {row['std']}")

def print_non_numeric_stats(df):
    if df.empty or df.shape[1] == 0:
        print("  ⚠️ No non-numeric columns found.")
        return
    for col in df.columns:
        series = df[col].dropna()
        if series.empty:
            continue
        counter = series.value_counts()
        top_val = counter.idxmax()
        freq = counter.max()
        print(f"  🔠 {col} -> count: {series.count()}, unique: {series.nunique()}, top: {top_val}, freq: {freq}")

def print_overall_stats(series, is_numeric):
    series = pd.Series(series).dropna()
    if series.empty:
        print("  ⚠️ No data for global statistics.")
        return
    if is_numeric:
        stats = series.describe()
        print(f"  📉 Global ➡️ Overall Numeric Stats:\n  count: {int(stats['count'])}\n  mean: {stats['mean']}\n  min: {stats['min']}\n  max: {stats['max']}\n  std: {stats['std']}")
    else:
        counter = series.value_counts()
        print(f"  📝 Global ➡️ Overall Non-Numeric Stats:\n  total entries: {series.count()}\n  unique values: {series.nunique()}\n  top: {counter.idxmax()}\n  freq: {counter.max()}")

# === Part Processor ===
def process_file_part(file_name, path, part):
    print(f"\n==== 📂 File: {file_name} | Part {part} ====")
    try:
        df = pd.read_csv(path, nrows=ROW_LIMIT)
    except Exception as e:
        print(f"  ⚠️ Failed to load file: {e}")
        return pd.DataFrame(), pd.DataFrame()

    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    non_numeric_cols = df.select_dtypes(exclude='number').columns.tolist()

    if part == 1:
        print("\n-- Numeric Stats Per Column --")
        print_numeric_stats(df[numeric_cols])

        print("\n-- Non-Numeric Stats Per Column --")
        print_non_numeric_stats(df[non_numeric_cols])

        return df[numeric_cols], df[non_numeric_cols]

    elif part == 2:
        if 'page_id' not in df.columns:
            print("  ⚠️ Missing 'page_id' column.")
            return pd.DataFrame(), pd.DataFrame()
        grouped = df.groupby('page_id')

    elif part == 3:
        if 'page_id' not in df.columns or 'ad_id' not in df.columns:
            print("  ⚠️ Missing 'page_id' or 'ad_id' column.")
            return pd.DataFrame(), pd.DataFrame()
        grouped = df.groupby(['page_id', 'ad_id'])

    numeric_agg = grouped[numeric_cols].mean() if numeric_cols else pd.DataFrame()
    non_numeric_flat = pd.concat([group[non_numeric_cols] for _, group in grouped]) if non_numeric_cols else pd.DataFrame()

    print("\n-- Numeric Stats Per Column (Grouped) --")
    print_numeric_stats(numeric_agg)

    print("\n-- Non-Numeric Stats Per Column (Grouped) --")
    print_non_numeric_stats(non_numeric_flat)

    return numeric_agg, non_numeric_flat

# === Master Runner ===
def run_analysis(part):
    print(f"\n====================== 📊 PART {part} ANALYSIS ======================\n")
    all_numeric_vals = []
    all_non_numeric_vals = []

    for file_name, path in csv_files.items():
        numeric_df, non_numeric_df = process_file_part(file_name, path, part)

        for col in numeric_df.columns:
            if col not in ("page_id", "ad_id"):
                all_numeric_vals.extend(numeric_df[col].dropna().tolist())

        for col in non_numeric_df.columns:
            if col not in ("page_id", "ad_id"):
                all_non_numeric_vals.extend(non_numeric_df[col].dropna().tolist())

    print(f"\n====================== 🌍 Overall Global Stats (All Files Combined) ======================")
    print_overall_stats(all_numeric_vals, is_numeric=True)
    print_overall_stats(all_non_numeric_vals, is_numeric=False)

# === Run All Parts ===
for part in [1, 2, 3]:
    run_analysis(part)
