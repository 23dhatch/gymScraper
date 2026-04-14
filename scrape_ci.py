"""
CI scraper — runs in GitHub Actions, appends results to data/occupancy.csv.
Does not require Flask, SQLite, or a display.
"""

import asyncio
import os
import sys
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import scraper

CSV_PATH = os.path.join(os.path.dirname(__file__), "data", "occupancy.csv")
COLS = ["facility", "area", "count", "capacity", "updated_at", "scraped_at"]


def append_to_csv(records: list[dict]) -> int:
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = [
        {
            "facility": r["facility"],
            "area": r["area"],
            "count": r["count"],
            "capacity": r["capacity"],
            "updated_at": r["updated_at"],
            "scraped_at": now,
        }
        for r in records
    ]

    if not rows:
        print("No records scraped.")
        return 0

    df_new = pd.DataFrame(rows, columns=COLS)

    if os.path.exists(CSV_PATH) and os.path.getsize(CSV_PATH) > len(",".join(COLS)):
        df_existing = pd.read_csv(CSV_PATH)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True).drop_duplicates(
            subset=["facility", "area", "updated_at"]
        )
    else:
        df_combined = df_new

    df_combined.to_csv(CSV_PATH, index=False)
    return len(rows)


if __name__ == "__main__":
    print(f"Scraping at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} …")
    results = asyncio.run(scraper.scrape_all())
    saved = append_to_csv(results)
    print(f"Done. {saved} record(s) written to {CSV_PATH}")
    total = sum(1 for _ in open(CSV_PATH)) - 1  # subtract header
    print(f"Total rows in CSV: {total}")
