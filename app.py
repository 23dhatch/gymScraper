"""
OSU Gym Occupancy Tracker
=========================
Runs as a background app with a Windows system tray icon.
- Double-click launch.bat to start (no terminal window)
- Tray icon lets you open the dashboard or quit

Setup (one-time):
    pip install -r requirements.txt
    playwright install chromium
"""

import asyncio
import io
import os
import sys
import threading
import webbrowser
import schedule
import time
from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template
from PIL import Image, ImageDraw
import pystray

import db
import scraper

app = Flask(__name__)

SCRAPE_INTERVAL_MINUTES = 30
PORT = 5000

# ---------------------------------------------------------------------------
# Scheduler state
# ---------------------------------------------------------------------------
_state_lock = threading.Lock()
_state = {
    "last_scrape": None,
    "next_scrape": None,
    "new_readings": 0,
}


def _run_scrape():
    try:
        results = asyncio.run(scraper.scrape_all())
        inserted = 0
        for r in results:
            if db.insert_reading(
                facility=r["facility"],
                area=r["area"],
                count=r["count"],
                capacity=r["capacity"],
                updated_at=r["updated_at"],
            ):
                inserted += 1
        now = datetime.now()
        with _state_lock:
            _state["last_scrape"] = now.strftime("%Y-%m-%d %H:%M:%S")
            _state["next_scrape"] = (
                now + timedelta(minutes=SCRAPE_INTERVAL_MINUTES)
            ).strftime("%Y-%m-%d %H:%M:%S")
            _state["new_readings"] = inserted
    except Exception:
        pass


def _scheduler_thread():
    schedule.every(SCRAPE_INTERVAL_MINUTES).minutes.do(_run_scrape)
    while True:
        schedule.run_pending()
        time.sleep(30)


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/latest")
def api_latest():
    return jsonify(db.get_latest_per_facility())


@app.route("/api/data")
def api_data():
    return jsonify(db.get_all_readings_json())


@app.route("/api/chart/by_hour")
def api_chart_by_hour():
    df = db.get_readings_df()
    if df.empty:
        return jsonify({})
    result = {}
    for (fac, area), grp in df.groupby(["facility", "area"]):
        key = f"{fac} {area}"
        hourly = grp.groupby("hour")["pct_full"].mean().reindex(range(24), fill_value=None)
        result[key] = [round(v, 1) if v is not None else None for v in hourly.tolist()]
    return jsonify(result)


@app.route("/api/chart/heatmap")
def api_chart_heatmap():
    df = db.get_readings_df()
    if df.empty:
        return jsonify({})
    result = {}
    for (fac, area), grp in df.groupby(["facility", "area"]):
        key = f"{fac} {area}"
        pivot = (
            grp.groupby(["day_of_week", "hour"])["pct_full"]
            .mean()
            .unstack(fill_value=None)
        )
        matrix = []
        for day in range(7):
            row = []
            for hour in range(24):
                val = pivot.at[day, hour] if (day in pivot.index and hour in pivot.columns) else None
                row.append(round(val, 1) if val is not None else None)
            matrix.append(row)
        result[key] = matrix
    return jsonify(result)


@app.route("/api/status")
def api_status():
    status = db.get_status()
    with _state_lock:
        status["next_scrape"] = _state["next_scrape"]
        status["new_readings"] = _state["new_readings"]
    return jsonify(status)


# ---------------------------------------------------------------------------
# System tray icon
# ---------------------------------------------------------------------------

def _make_icon_image() -> Image.Image:
    """Draw a simple scarlet circle with 'G' for the tray icon."""
    size = 64
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    # OSU scarlet background circle
    draw.ellipse([2, 2, size - 2, size - 2], fill="#BB0000")
    # White 'G' letter centered
    draw.text((18, 14), "G", fill="white")
    return img


def _open_dashboard(icon, item):
    webbrowser.open(f"http://localhost:{PORT}")


def _quit_app(icon, item):
    icon.stop()
    os._exit(0)


def _run_tray():
    icon_image = _make_icon_image()
    menu = pystray.Menu(
        pystray.MenuItem("Open Dashboard", _open_dashboard, default=True),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("Quit", _quit_app),
    )
    icon = pystray.Icon("GymTracker", icon_image, "OSU Gym Tracker", menu)
    icon.run()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    db.init_db()

    # Initial scrape in background so the app starts fast
    threading.Thread(target=_run_scrape, daemon=True).start()

    # Recurring scheduler
    threading.Thread(target=_scheduler_thread, daemon=True).start()

    # Flask in its own daemon thread
    flask_thread = threading.Thread(
        target=lambda: app.run(host="localhost", port=PORT, debug=False, use_reloader=False),
        daemon=True,
    )
    flask_thread.start()

    # Open browser once Flask is up
    def _open_browser():
        time.sleep(1.5)
        webbrowser.open(f"http://localhost:{PORT}")

    threading.Thread(target=_open_browser, daemon=True).start()

    # System tray runs on the main thread (required on Windows)
    _run_tray()
