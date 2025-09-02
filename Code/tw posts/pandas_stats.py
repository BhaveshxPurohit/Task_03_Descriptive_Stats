import pandas as pd
from collections import Counter

# === Configuration ===
file_path = r"C:\Users\puroh\OneDrive\Documents\Syracuse\RA\Task_03_Descriptive_Stats\Data\2024_tw_posts_president_scored_anon.csv"
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

# === PART 1 Runner ===
def run_part_1():
    print(f"\n====================== 📊 PART 1 ANALYSIS (Twitter Posts) ======================\n")
    try:
        df = pd.read_csv(file_path, nrows=ROW_LIMIT)
    except Exception as e:
        print(f"  ⚠️ Failed to load file: {e}")
        return

    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    non_numeric_cols = df.select_dtypes(exclude='number').columns.tolist()

    print("\n-- Numeric Stats Per Column --")
    print_numeric_stats(df[numeric_cols])

    print("\n-- Non-Numeric Stats Per Column --")
    print_non_numeric_stats(df[non_numeric_cols])

    all_numeric_vals = []
    all_non_numeric_vals = []

    for col in numeric_cols:
        if col not in ("Facebook_Id", "post_id"):
            all_numeric_vals.extend(df[col].dropna().tolist())

    for col in non_numeric_cols:
        if col not in ("Facebook_Id", "post_id"):
            all_non_numeric_vals.extend(df[col].dropna().tolist())

    print(f"\n====================== 🌍 Overall Global Stats ======================")
    print_overall_stats(all_numeric_vals, is_numeric=True)
    print_overall_stats(all_non_numeric_vals, is_numeric=False)

# === Run Part 1 Only ===
run_part_1()
