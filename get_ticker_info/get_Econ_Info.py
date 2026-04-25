"""
Kalshi Economics Series Lister — Print Only
===========================================
Fetches every series on Kalshi, prints categories, and prints full info
for every Economics series. No files saved.
"""

import requests
import pandas as pd

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# ============================================================
# 1. PULL ALL SERIES
# ============================================================
resp = requests.get(f"{BASE_URL}/series")
series_list = resp.json().get("series", [])

print(f"Total series on Kalshi: {len(series_list)}")

# ============================================================
# 2. BUILD DATAFRAME
# ============================================================
df_all = pd.DataFrame([
    {
        "ticker":    s.get("ticker"),
        "title":     s.get("title"),
        "category":  s.get("category"),
        "frequency": s.get("frequency"),
        "tags":      ", ".join(s.get("tags") or []),
    }
    for s in series_list
])

# ============================================================
# 3. CATEGORY COUNTS
# ============================================================
print("\n=== Categories available ===")
print(df_all["category"].value_counts().to_string())

# ============================================================
# 4. ECONOMICS — FULL LISTING
# ============================================================
df_econ = df_all[df_all["category"].str.contains("Economic", case=False, na=False)].reset_index(drop=True)

pd.set_option("display.max_rows", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", None)

print(f"\n=== Economics series ({len(df_econ)} total) ===")
print(df_econ.to_string(index=False))
