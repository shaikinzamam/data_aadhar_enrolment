import pandas as pd
import glob

# Path where your 3 CSV files are stored
RAW_PATH = "raw_data/*.csv"

# Final single output file
OUTPUT_FILE = "district_month_demand.csv"

# -------------------------
# 1. Load and merge all CSV files
# -------------------------
files = glob.glob(RAW_PATH)

if len(files) == 0:
    print("❌ No CSV files found in raw_data folder.")
    exit()

df_list = [pd.read_csv(file) for file in files]
df = pd.concat(df_list, ignore_index=True)

print("✅ Files merged:", len(files))
print("✅ Total rows:", len(df))

# -------------------------
# 2. Create total_enrolments
# -------------------------
df["total_enrolments"] = (
    df["age_0_5"] + df["age_5_17"] + df["age_18_greater"]
)

# -------------------------
# 3. Convert date → YYYY-MM
# -------------------------
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.dropna(subset=["date"])
df["month"] = df["date"].dt.to_period("M").astype(str)

# -------------------------
# 4. Aggregate to state + district + month
# -------------------------
monthly = df.groupby(
    ["state", "district", "month"], as_index=False
).agg(
    monthly_enrolments=("total_enrolments", "sum")
)

# -------------------------
# 5. Monthly load (same as demand load)
# -------------------------
monthly["monthly_load"] = monthly["monthly_enrolments"]

# -------------------------
# 6. Estimated update requests (35%)
# -------------------------
monthly["estimated_update_requests"] = (
    monthly["monthly_enrolments"] * 0.35
).round().astype(int)

# -------------------------
# 7. Save single final CSV file
# -------------------------
monthly.to_csv(OUTPUT_FILE, index=False)

print("🎯 FINAL FILE GENERATED:", OUTPUT_FILE)
