"""Every historical layer over ALL cataloged tickers: metadata, daily, hourly,
trades and minute candles, NOT just the focus universe.

This is pull_historical/backfill.py run without arguments: it prints an
estimate first (about 245,000 requests and 14 to 16 hours on the 2026-09-17
catalog), walks the catalog in priority order, journals what is final, writes
failure lists, and resumes with the same command. For a bounded run use
backfill.py with --tickers or --layers, or the individual pullers.
"""

import sys

from pull_historical.backfill import main

if __name__ == "__main__":
    sys.exit(main([]))
