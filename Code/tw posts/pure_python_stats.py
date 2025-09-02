import csv
from collections import defaultdict, Counter
from itertools import islice

# === Configuration ===
csv_file = r"C:\Users\puroh\OneDrive\Documents\Syracuse\RA\Task_03_Descriptive_Stats\Data\2024_tw_posts_president_scored_anon.csv"
ROW_LIMIT = 500

# === Helpers ===
def try_parse_float(val):
    try:
        return float(val)
    except:
        return None

def compute_basic_stats(values):
    if not values:
        return {'count': 0, 'mean': None, 'min': None, 'max': None, 'std': None}
    count = len(values)
    mean = sum(values) / count
    std = (sum((x - mean) ** 2 for x in values) / count) ** 0.5 if count > 1 else 0
    return {'count': count, 'mean': mean, 'min': min(values), 'max': max(values), 'std': std}

def detect_column_types(path, sample_size=100):
    numeric_cols, non_numeric_cols = set(), set()
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        col_samples = defaultdict(list)
        for i, row in enumerate(reader):
            for col, val in row.items():
                val = val.strip()
                if val:
                    col_samples[col].append(val)
            if i >= sample_size:
                break
    for col, samples in col_samples.items():
        numeric_count = sum(1 for val in samples if try_parse_float(val) is not None)
        if numeric_count >= 0.8 * len(samples):
            numeric_cols.add(col)
        else:
            non_numeric_cols.add(col)
    return list(numeric_cols), list(non_numeric_cols)

def print_column_stats(stats_dict, is_numeric):
    for col, values in stats_dict.items():
        if is_numeric:
            stats = compute_basic_stats(values)
            print(f"  📊 {col} -> count: {stats['count']}, mean: {stats['mean']}, min: {stats['min']}, max: {stats['max']}, std: {stats['std']}")
        else:
            if not values:
                continue
            counter = Counter(values)
            top_val, freq = counter.most_common(1)[0]
            print(f"  🔠 {col} -> count: {len(values)}, unique: {len(counter)}, top: {top_val}, freq: {freq}")

def print_overall_stats(all_values, is_numeric):
    if is_numeric:
        stats = compute_basic_stats(all_values)
        print(f"  📉 Global ➡️ Overall Numeric Stats:\n  count: {stats['count']}\n  mean: {stats['mean']}\n  min: {stats['min']}\n  max: {stats['max']}\n  std: {stats['std']}")
    else:
        if not all_values:
            return
        counter = Counter(all_values)
        top_val, freq = counter.most_common(1)[0]
        print(f"  📝 Global ➡️ Overall Non-Numeric Stats:\n  total entries: {len(all_values)}\n  unique values: {len(counter)}\n  top: {top_val}\n  freq: {freq}")

# === Main Execution (Part 1 only) ===
print(f"\n==== 📂 File: 2024_tw_posts_president_scored_anon.csv | Part 1 ====")
numeric_cols, non_numeric_cols = detect_column_types(csv_file)
numeric_data = defaultdict(list)
non_numeric_data = defaultdict(list)

with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in islice(reader, ROW_LIMIT):
        for col in numeric_cols:
            val = try_parse_float(row.get(col))
            if val is not None:
                numeric_data[col].append(val)
        for col in non_numeric_cols:
            val = row.get(col, "").strip()
            if val:
                non_numeric_data[col].append(val)

print("\n-- Numeric Stats Per Column --")
print_column_stats(numeric_data, is_numeric=True)

print("\n-- Non-Numeric Stats Per Column --")
print_column_stats(non_numeric_data, is_numeric=False)

print(f"\n====================== 🌍 Overall Global Stats ======================")
all_numeric_vals = []
for col, vals in numeric_data.items():
    if col not in ("Facebook_Id", "post_id"):
        all_numeric_vals.extend(vals)

all_non_numeric_vals = []
for col, vals in non_numeric_data.items():
    if col not in ("Facebook_Id", "post_id"):
        all_non_numeric_vals.extend(vals)

print_overall_stats(all_numeric_vals, is_numeric=True)
print_overall_stats(all_non_numeric_vals, is_numeric=False)
