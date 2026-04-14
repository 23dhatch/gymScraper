"""
Scraper for OSU RecSports gym occupancy.

Strategy A (preferred): Intercept the XHR/fetch responses that the page's
JavaScript makes when loading occupancy data. This gives us clean JSON.

Strategy B (fallback): Parse the rendered DOM for occupancy widget elements
once JS has finished running.

Both strategies target the weight floor specifically, but will store any area
found if no weight-floor-labeled area is detected.
"""

import asyncio
import json
import os
import re
from datetime import datetime

from playwright.async_api import async_playwright, Page, Response

FACILITIES = [
    ("rpac", "RPAC"),
    ("jon", "JON"),
]

BASE_URL = "https://recsports.osu.edu/fms/facilities/{}"

# Also try the aggregated Innosoft Fusion occupancy page
FUSION_URL = "https://ohiostate.innosoftfusion.com/FacilityOccupancy"

WEIGHT_FLOOR_PATTERN = re.compile(r"weight\s*floor", re.IGNORECASE)


def _parse_time_str(raw: str) -> str:
    """Normalise whatever timestamp string the page uses into a consistent str."""
    if not raw:
        return ""
    raw = raw.strip()
    # Try to enrich with today's date if it looks like a bare time ("4:14 PM")
    if re.match(r"^\d{1,2}:\d{2}\s*(AM|PM)$", raw, re.IGNORECASE):
        today = datetime.now().strftime("%Y-%m-%d")
        try:
            dt = datetime.strptime(f"{today} {raw}", "%Y-%m-%d %I:%M %p")
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            pass
    return raw


# ---------------------------------------------------------------------------
# Strategy A – network interception
# ---------------------------------------------------------------------------

def _extract_from_json(payload, facility_name: str) -> list[dict]:
    """
    Try to pull occupancy records out of an arbitrary JSON payload.
    Innosoft Fusion typically returns a list of objects with fields like
    FacilityName / AreaName / CurrentCount / Capacity / LastUpdated.
    We try both camelCase and PascalCase variants.
    """
    results = []
    items = []

    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        # look for a list value
        for v in payload.values():
            if isinstance(v, list) and v:
                items = v
                break
        if not items:
            items = [payload]

    for item in items:
        if not isinstance(item, dict):
            continue
        area = (
            item.get("AreaName")
            or item.get("areaName")
            or item.get("area")
            or item.get("name")
            or ""
        )
        count = (
            item.get("CurrentCount")
            or item.get("currentCount")
            or item.get("count")
            or item.get("occupancy")
        )
        capacity = (
            item.get("Capacity")
            or item.get("capacity")
            or item.get("maxCapacity")
            or item.get("MaxCapacity")
        )
        updated_at = (
            item.get("LastUpdated")
            or item.get("lastUpdated")
            or item.get("UpdatedAt")
            or item.get("updatedAt")
            or item.get("timestamp")
            or ""
        )
        # Facility from payload or caller
        fac = (
            item.get("FacilityName")
            or item.get("facilityName")
            or item.get("facility")
            or facility_name
        )
        if count is None or capacity is None:
            continue
        results.append(
            {
                "facility": str(fac).upper() if fac == facility_name else str(fac),
                "area": str(area),
                "count": int(count),
                "capacity": int(capacity),
                "updated_at": _parse_time_str(str(updated_at)),
            }
        )
    return results


async def _scrape_via_interception(page: Page, url: str, facility_name: str) -> list[dict]:
    captured: list[dict] = []

    async def on_response(response: Response):
        resp_url = response.url.lower()
        if not any(kw in resp_url for kw in ("occupancy", "capacity", "facility", "count")):
            return
        ct = response.headers.get("content-type", "")
        if "json" not in ct:
            return
        try:
            body = await response.json()
            records = _extract_from_json(body, facility_name)
            captured.extend(records)
        except Exception:
            pass

    page.on("response", on_response)
    try:
        await page.goto(url, wait_until="networkidle", timeout=30_000)
    except Exception:
        pass
    finally:
        page.remove_listener("response", on_response)

    return captured


# ---------------------------------------------------------------------------
# Strategy B – DOM scraping
# ---------------------------------------------------------------------------

def _extract_time_from_status(status_text: str) -> str:
    """
    Extract a time string from status text like:
      "Open / Last updated: 1:56 p.m."
      "Open / Last updated: 11:05 a.m."
    Returns a canonical string like "1:56 PM" or empty string.
    """
    m = re.search(r"(\d{1,2}:\d{2})\s*(a\.m\.|p\.m\.|am|pm)", status_text, re.IGNORECASE)
    if not m:
        return ""
    time_part = m.group(1)
    ampm = m.group(2).replace(".", "").upper()
    return f"{time_part} {ampm}"


async def _scrape_via_dom(page: Page, facility_name: str) -> list[dict]:
    """
    Parse the rendered DOM using the known .c-meter widget structure:
      <div class="c-meter">
        <label class="c-meter__label">
          <span class="c-meter__title">Weight Floor</span>
          <span class="c-meter__status">Open / Last updated: 1:56 p.m.</span>
        </label>
        <meter class="c-meter__meter" value="65" min="0" max="125">…</meter>
      </div>
    """
    results = []
    try:
        meters = await page.locator("div.c-meter").all()
        for meter_div in meters:
            try:
                title_el = meter_div.locator("span.c-meter__title")
                area = (await title_el.inner_text()).strip()

                status_el = meter_div.locator("span.c-meter__status")
                status_text = (await status_el.inner_text()).strip()

                meter_el = meter_div.locator("meter.c-meter__meter")
                count = await meter_el.get_attribute("value")
                capacity = await meter_el.get_attribute("max")

                if count is None or capacity is None:
                    continue

                updated_at = _parse_time_str(_extract_time_from_status(status_text))

                results.append(
                    {
                        "facility": facility_name,
                        "area": area,
                        "count": int(float(count)),
                        "capacity": int(float(capacity)),
                        "updated_at": updated_at,
                    }
                )
            except Exception:
                continue
    except Exception:
        pass
    return results


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def _filter_weight_floor(records: list[dict]) -> list[dict]:
    """Prefer weight-floor entries; if none found return all records."""
    wf = [r for r in records if WEIGHT_FLOOR_PATTERN.search(r.get("area", ""))]
    return wf if wf else records


async def scrape_all() -> list[dict]:
    """
    Scrape RPAC and JON occupancy. Returns a list of dicts ready for db.insert_reading().
    """
    all_results: list[dict] = []

    async with async_playwright() as pw:
        # GitHub Actions (Linux) requires --no-sandbox
        is_ci = os.environ.get("CI", "").lower() == "true"
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"] if is_ci else [],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )

        for fac_id, fac_name in FACILITIES:
            page = await context.new_page()
            url = BASE_URL.format(fac_id)
            print(f"  Scraping {fac_name} ({url}) …")

            try:
                # Strategy A
                records = await _scrape_via_interception(page, url, fac_name)

                # Strategy B fallback
                if not records:
                    print(f"  No JSON intercepted for {fac_name}, falling back to DOM …")
                    records = await _scrape_via_dom(page, fac_name)

                records = _filter_weight_floor(records)

                if records:
                    print(f"  {fac_name}: found {len(records)} record(s)")
                    all_results.extend(records)
                else:
                    print(f"  {fac_name}: no occupancy data found this scrape")

            except Exception as exc:
                print(f"  Error scraping {fac_name}: {exc}")
            finally:
                await page.close()

        await context.close()
        await browser.close()

    return all_results


if __name__ == "__main__":
    results = asyncio.run(scrape_all())
    print("\nResults:")
    for r in results:
        print(r)
