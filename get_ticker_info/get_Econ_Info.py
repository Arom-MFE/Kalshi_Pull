"""
Kalshi Economics Series Lister — Print Only
===========================================
Fetches every series on Kalshi, prints categories, and prints full info
for every Economics series. No files saved. Keyless.

For targeted searches (by category, tag, keyword, status) use
get_ticker_info/find_events.py.
"""

import sys
from pathlib import Path

# Ensure kalshi_io is importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from kalshi_io import discovery
from kalshi_io.runlog import configure_logging


def main() -> None:
    configure_logging()

    # ============================================================
    # 1. PULL ALL SERIES (rate limited, retried, fails loudly)
    # ============================================================
    series_list = discovery.list_series()

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
            "tags":      ", ".join(s["tags"]),
            # True for a dead pre-KX spelling (CPIYOY) whose KX series exists
            "legacy":    s["legacy_twin"],
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


if __name__ == "__main__":
    main()
