import pandas as pd
from collections import Counter

# === Configuration ===
csv_path = r"C:\Users\puroh\OneDrive\Documents\Syracuse\RA\Task_03_Descriptive_Stats\Data\2024_tw_posts_president_scored_anon.csv"
ROW_LIMIT = 500

# === Load Data ===
try:
    df = pd.read_csv(csv_path, nrows=ROW_LIMIT)
except Exception as e:
    print(f"⚠️ Failed to load CSV: {e}")
    df = pd.DataFrame()

if df.empty:
    print("⚠️ No data loaded.")
else:
    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    non_numeric_cols = df.select_dtypes(exclude='number').columns.tolist()

    print("\n-- Numeric Stats Per Column --")
    if not numeric_cols:
        print("  ⚠️ No numeric columns found.")
    else:
        desc = df[numeric_cols].describe().T
        for col in numeric_cols:
            row = desc.loc[col]
            print(f"  📊 {col} -> count: {int(row['count'])}, mean: {row['mean']:.4f}, min: {row['min']}, max: {row['max']}, std: {row['std']:.4f}")

    print("\n-- Non-Numeric Stats Per Column --")
    for col in non_numeric_cols:
        series = df[col].dropna().astype(str)
        if series.empty:
            continue
        counts = series.value_counts()
        top_val = counts.index[0]
        freq = counts.iloc[0]
        print(f"  🔠 {col} -> count: {len(series)}, unique: {series.nunique()}, top: {top_val}, freq: {freq}")

    # === Overall stats (excluding Facebook_Id and post_id) ===
    print("\n====================== 🌍 Overall Global Stats ======================")
    numeric_vals = []
    for col in numeric_cols:
        if col not in ("Facebook_Id", "post_id"):
            numeric_vals.extend(df[col].dropna().tolist())

    non_numeric_vals = []
    for col in non_numeric_cols:
        if col not in ("Facebook_Id", "post_id"):
            non_numeric_vals.extend(df[col].dropna().astype(str).tolist())

    if numeric_vals:
        s = pd.Series(numeric_vals)
        print("\n📉 Overall Numeric Stats (excluding IDs):")
        print(f"  Count: {s.count()}")
        print(f"  Mean: {s.mean():.4f}")
        print(f"  Std:  {s.std():.4f}")
        print(f"  Min:  {s.min()}")
        print(f"  Max:  {s.max()}")
    else:
        print("\n⚠️ No overall numeric values to summarize.")

    if non_numeric_vals:
        vc = pd.Series(non_numeric_vals).value_counts()
        print("\n📝 Overall Non-Numeric Stats (excluding IDs):")
        print(f"  Total: {vc.sum()}, Unique: {vc.count()}, Top: {vc.index[0]}, Freq: {vc.iloc[0]}")
    else:
        print("\n⚠️ No overall non-numeric values to summarize.")
