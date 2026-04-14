import sqlite3
import os
import pandas as pd
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "occupancy.db")


def _connect():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db():
    with _connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS readings (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                facility     TEXT NOT NULL,
                area         TEXT NOT NULL,
                count        INTEGER,
                capacity     INTEGER,
                updated_at   TEXT,
                scraped_at   TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_reading
                ON readings(facility, area, updated_at);
        """)


def insert_reading(facility: str, area: str, count: int, capacity: int, updated_at: str) -> bool:
    """Insert a reading. Returns True if inserted, False if it was a duplicate."""
    with _connect() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO readings (facility, area, count, capacity, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (facility, area, count, capacity, updated_at),
        )
        return cur.rowcount > 0


def get_readings_df() -> pd.DataFrame:
    """Return all readings as a DataFrame with parsed datetime columns."""
    with _connect() as conn:
        df = pd.read_sql_query("SELECT * FROM readings ORDER BY scraped_at", conn)
    if df.empty:
        return df
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    df["hour"] = df["scraped_at"].dt.hour
    df["day_of_week"] = df["scraped_at"].dt.dayofweek  # 0=Mon … 6=Sun
    df["day_name"] = df["scraped_at"].dt.day_name()
    df["pct_full"] = (df["count"] / df["capacity"] * 100).round(1)
    return df


def get_all_readings_json() -> list:
    with _connect() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT facility, area, count, capacity, updated_at, scraped_at "
            "FROM readings ORDER BY scraped_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_latest_per_facility() -> list:
    with _connect() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT facility, area, count, capacity, updated_at, scraped_at
            FROM readings
            WHERE id IN (
                SELECT MAX(id) FROM readings GROUP BY facility, area
            )
            ORDER BY facility, area
        """).fetchall()
    return [dict(r) for r in rows]


def get_status() -> dict:
    with _connect() as conn:
        row = conn.execute(
            "SELECT MAX(scraped_at) as last_scrape, COUNT(*) as total FROM readings"
        ).fetchone()
    return {
        "last_scrape": row[0],
        "reading_count": row[1],
    }
