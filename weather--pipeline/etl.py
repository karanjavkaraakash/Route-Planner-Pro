"""
RoutePlannerPro Weather Pipeline — ETL Script
=============================================
Fetches GFS (wind + MSLP) and WaveWatch III (waves + swell)
from NOAA NOMADS, parses GRIB2, upserts to Supabase weather_grid.

Sources:
  - GFS 0.5°: https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p50.pl
  - WaveWatch III: https://nomads.ncep.noaa.gov/cgi-bin/filter_gfswave.pl

Usage:
  python etl.py --mode short   # 0-5 day (hours 0-120, 6-hourly)
  python etl.py --mode long    # 5-16 day (hours 120-384, 6-hourly)
  python etl.py --mode all     # full 16-day run
"""

import os
import sys
import time
import argparse
import logging
import requests
import tempfile
import math
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client

# ── Optional: eccodes for GRIB2 parsing ──────────────────────────────────────
try:
    import eccodes
    HAS_ECCODES = True
except ImportError:
    HAS_ECCODES = False

# ── Optional: cfgrib / xarray fallback ───────────────────────────────────────
try:
    import xarray as xr
    import cfgrib
    HAS_XARRAY = True
except ImportError:
    HAS_XARRAY = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
RESOLUTION           = 0.5          # degrees (0.5° global)
BATCH_SIZE           = 500          # rows per upsert batch
REQUEST_TIMEOUT      = 120          # seconds per GRIB download
MAX_RETRIES          = 3

# Forecast hour sets
HOURS_SHORT = list(range(0, 121, 6))          # 0–120  in 6h steps  (21 steps)
HOURS_LONG  = list(range(126, 385, 6))        # 126–384 in 6h steps (44 steps)
# Note: NOMADS GFS uses 3h steps to 120, then 6h. We use 6h throughout for consistency.

# GFS variables we want from NOMADS filter API
GFS_VARS = {
    "UGRD":  "var_UGRD",    # U-wind component 10m
    "VGRD":  "var_VGRD",    # V-wind component 10m
    "PRMSL": "var_PRMSL",   # Mean sea level pressure
}
GFS_LEVEL = "lev_10_m_above_ground"

# WaveWatch III variables
WW3_VARS = {
    "HTSGW":  "var_HTSGW",   # Significant wave height
    "PERPW":  "var_PERPW",   # Peak wave period
    "DIRPW":  "var_DIRPW",   # Peak wave direction
    "SWELL":  "var_SWELL",   # Swell height
    "SWDIR":  "var_SWDIR",   # Swell direction
}


# ── Supabase client ───────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ── NOMADS run-time discovery ─────────────────────────────────────────────────
def latest_gfs_run() -> tuple[str, str]:
    """
    Returns (yyyymmdd, hh) for the most recent completed GFS run.
    GFS runs at 00, 06, 12, 18 UTC; allow 5h for processing.
    """
    now = datetime.now(timezone.utc)
    for lag in [5, 11, 17, 23]:
        candidate = now - timedelta(hours=lag)
        run_hour = (candidate.hour // 6) * 6
        return candidate.strftime("%Y%m%d"), f"{run_hour:02d}"
    # fallback
    return now.strftime("%Y%m%d"), "00"


# ── NOMADS URL builders ───────────────────────────────────────────────────────
def gfs_url(date: str, run: str, fhour: int) -> str:
    """
    NOMADS GFS 0.5° GRIB filter URL.
    Requests only the variables + level we need to minimise download size.
    """
    fstr = f"{fhour:03d}"
    params = (
        f"file=gfs.t{run}z.pgrb2full.0p50.f{fstr}"
        f"&{GFS_LEVEL}=on"
        f"&{GFS_VARS['UGRD']}=on"
        f"&{GFS_VARS['VGRD']}=on"
        f"&{GFS_VARS['PRMSL']}=on"
        f"&leftlon=0&rightlon=360&toplat=90&bottomlat=-90"
        f"&dir=%2Fgfs.{date}%2F{run}%2Fatmos"
    )
    return f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p50.pl?{params}"


def ww3_url(date: str, run: str, fhour: int) -> str:
    """
    NOMADS WaveWatch III global GRIB filter URL.
    """
    fstr = f"{fhour:03d}"
    var_str = "&".join(f"{v}=on" for v in WW3_VARS.values())
    params = (
        f"file=gfswave.t{run}z.global.0p25.f{fstr}.grib2"
        f"&{var_str}"
        f"&leftlon=0&rightlon=360&toplat=90&bottomlat=-90"
        f"&dir=%2Fgfs.{date}%2F{run}%2Fwave%2Fglobal"
    )
    return f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfswave.pl?{params}"


# ── GRIB download with retry ──────────────────────────────────────────────────
def download_grib(url: str, label: str) -> bytes | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"  Downloading {label} (attempt {attempt})")
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200 and len(r.content) > 1000:
                return r.content
            log.warning(f"  Bad response {r.status_code} for {label}")
        except requests.RequestException as e:
            log.warning(f"  Request error: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(10 * attempt)
    log.error(f"  Failed to download {label} after {MAX_RETRIES} attempts")
    return None


# ── GRIB2 parsing via eccodes ─────────────────────────────────────────────────
def parse_grib_eccodes(data: bytes) -> dict:
    """
    Parse raw GRIB2 bytes using eccodes.
    Returns dict keyed by shortName → {lats, lons, values} arrays.
    """
    result = {}
    with tempfile.NamedTemporaryFile(suffix=".grib2", delete=False) as f:
        f.write(data)
        fname = f.name

    try:
        with open(fname, "rb") as f:
            while True:
                msg = eccodes.codes_grib_new_from_file(f)
                if msg is None:
                    break
                try:
                    short = eccodes.codes_get(msg, "shortName")
                    lats  = eccodes.codes_get_array(msg, "latitudes")
                    lons  = eccodes.codes_get_array(msg, "longitudes")
                    vals  = eccodes.codes_get_array(msg, "values")
                    result[short] = {"lats": lats, "lons": lons, "vals": vals}
                finally:
                    eccodes.codes_release(msg)
    finally:
        os.unlink(fname)

    return result


def parse_grib_xarray(data: bytes, filter_by_keys: dict = None) -> dict:
    """
    Fallback: parse GRIB2 via cfgrib/xarray.
    """
    result = {}
    with tempfile.NamedTemporaryFile(suffix=".grib2", delete=False) as f:
        f.write(data)
        fname = f.name
    try:
        kwargs = {"indexpath": ""}
        if filter_by_keys:
            kwargs["filter_by_keys"] = filter_by_keys
        ds = xr.open_dataset(fname, engine="cfgrib", **kwargs)
        lats = ds.latitude.values.flatten()
        lons = ds.longitude.values.flatten()
        for var in ds.data_vars:
            vals = ds[var].values.flatten()
            result[var] = {"lats": lats, "lons": lons, "vals": vals}
    finally:
        os.unlink(fname)
    return result


def parse_grib(data: bytes) -> dict:
    if HAS_ECCODES:
        return parse_grib_eccodes(data)
    elif HAS_XARRAY:
        return parse_grib_xarray(data)
    else:
        raise RuntimeError("Neither eccodes nor cfgrib/xarray is available. Install one.")


# ── Wind math helpers ─────────────────────────────────────────────────────────
def uv_to_speed_dir(u: float, v: float) -> tuple[float, float]:
    speed = math.sqrt(u**2 + v**2)
    direction = (270 - math.degrees(math.atan2(v, u))) % 360
    return round(speed, 2), round(direction, 1)


# ── Thinning: subsample to RESOLUTION ────────────────────────────────────────
def thin_to_resolution(lats, lons, vals, res: float = 0.5):
    """
    Reduce grid to target resolution by selecting every Nth point.
    NOMADS GFS 0.5° is already at 0.5° so this is mostly a no-op,
    but handles WW3 0.25° → 0.5° thinning.
    """
    points = {}
    for lat, lon, val in zip(lats, lons, vals):
        rlat = round(round(lat / res) * res, 3)
        rlon = round(round(lon / res) * res, 3)
        # normalise lon to -180..180
        if rlon > 180:
            rlon = rlon - 360
        key = (rlat, rlon)
        if key not in points:
            points[key] = val
    return points


# ── Row builder ───────────────────────────────────────────────────────────────
def build_rows_gfs(parsed: dict, valid_time: datetime, fhour: int, run_time: datetime) -> list[dict]:
    """Merge u/v/mslp into per-gridpoint rows."""
    rows = {}

    def merge(varname: str, field: str):
        if varname not in parsed:
            return
        d = parsed[varname]
        pts = thin_to_resolution(d["lats"], d["lons"], d["vals"], RESOLUTION)
        for (lat, lon), val in pts.items():
            key = (lat, lon)
            if key not in rows:
                rows[key] = {
                    "lat": lat, "lon": lon,
                    "valid_time": valid_time.isoformat(),
                    "forecast_hour": fhour,
                    "resolution_deg": RESOLUTION,
                    "run_time": run_time.isoformat(),
                }
            rows[key][field] = None if val > 9e19 else round(float(val), 4)

    merge("10u", "wind_u")
    merge("10v", "wind_v")
    merge("prmsl", "mslp")

    # Derive speed + direction
    for key, row in rows.items():
        u = row.get("wind_u")
        v = row.get("wind_v")
        if u is not None and v is not None:
            row["wind_speed"], row["wind_dir"] = uv_to_speed_dir(u, v)

    return list(rows.values())


def build_rows_ww3(parsed: dict, valid_time: datetime, fhour: int, run_time: datetime) -> list[dict]:
    """Merge wave variables into per-gridpoint rows."""
    rows = {}

    mapping = {
        "swh":   "wave_hs",
        "perpw": "wave_tp",
        "dirpw": "wave_dir",
        "swell": "swell_hs",
        "swdir": "swell_dir",
    }

    for src, dst in mapping.items():
        if src not in parsed:
            continue
        d = parsed[src]
        pts = thin_to_resolution(d["lats"], d["lons"], d["vals"], RESOLUTION)
        for (lat, lon), val in pts.items():
            key = (lat, lon)
            if key not in rows:
                rows[key] = {
                    "lat": lat, "lon": lon,
                    "valid_time": valid_time.isoformat(),
                    "forecast_hour": fhour,
                    "resolution_deg": RESOLUTION,
                    "run_time": run_time.isoformat(),
                }
            rows[key][dst] = None if val > 9e19 else round(float(val), 4)

    return list(rows.values())


# ── Supabase upsert ───────────────────────────────────────────────────────────
def upsert_rows(sb: Client, rows: list[dict], source: str) -> int:
    if not rows:
        return 0
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        sb.table("weather_grid").upsert(
            batch,
            on_conflict="lat,lon,valid_time,run_time"
        ).execute()
        total += len(batch)
        log.info(f"    Upserted {total}/{len(rows)} {source} rows")
    return total


# ── Log pipeline run ──────────────────────────────────────────────────────────
def log_run(sb: Client, source: str, forecast_days: int,
            records: int, duration: float, status: str, error: str = None):
    sb.table("pipeline_run_log").insert({
        "source": source,
        "forecast_days": forecast_days,
        "records_upserted": records,
        "duration_secs": round(duration, 1),
        "status": status,
        "error_msg": error,
    }).execute()


# ── Main pipeline ─────────────────────────────────────────────────────────────
def run_pipeline(mode: str):
    t0 = time.time()
    date, run = latest_gfs_run()
    run_time = datetime.strptime(f"{date}{run}", "%Y%m%d%H").replace(tzinfo=timezone.utc)
    log.info(f"Pipeline start | mode={mode} | GFS run={date}/{run}Z")

    if mode == "short":
        hours = HOURS_SHORT
    elif mode == "long":
        hours = HOURS_LONG
    else:
        hours = HOURS_SHORT + HOURS_LONG

    sb = get_supabase()
    total_gfs = 0
    total_ww3 = 0

    for fhour in hours:
        valid_time = run_time + timedelta(hours=fhour)
        log.info(f"Processing fhour={fhour:03d} → valid={valid_time.strftime('%Y-%m-%d %HZ')}")

        # ── GFS ──────────────────────────────────────────────────────────────
        gfs_data = download_grib(gfs_url(date, run, fhour), f"GFS f{fhour:03d}")
        if gfs_data:
            try:
                parsed = parse_grib(gfs_data)
                rows = build_rows_gfs(parsed, valid_time, fhour, run_time)
                n = upsert_rows(sb, rows, "GFS")
                total_gfs += n
            except Exception as e:
                log.error(f"  GFS parse/upsert error at f{fhour:03d}: {e}")
        else:
            log.warning(f"  Skipping GFS f{fhour:03d} — download failed")

        # ── WaveWatch III ─────────────────────────────────────────────────────
        # WW3 only available to f120 in some runs; gracefully skip if missing
        if fhour <= 120:
            ww3_data = download_grib(ww3_url(date, run, fhour), f"WW3 f{fhour:03d}")
            if ww3_data:
                try:
                    parsed = parse_grib(ww3_data)
                    rows = build_rows_ww3(parsed, valid_time, fhour, run_time)
                    n = upsert_rows(sb, rows, "WW3")
                    total_ww3 += n
                except Exception as e:
                    log.error(f"  WW3 parse/upsert error at f{fhour:03d}: {e}")
            else:
                log.info(f"  WW3 f{fhour:03d} not available — skipping (normal beyond f120)")

        # small pause to be a good NOMADS citizen
        time.sleep(2)

    duration = time.time() - t0
    log.info(f"Pipeline complete | GFS rows={total_gfs} | WW3 rows={total_ww3} | {duration:.0f}s")

    log_run(sb, "GFS", len(hours) // 4, total_gfs, duration, "success")
    if total_ww3 > 0:
        log_run(sb, "WW3", len(hours) // 4, total_ww3, duration, "success")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["short", "long", "all"],
        default="short",
        help="short=0-5d, long=5-16d, all=full 16d"
    )
    args = parser.parse_args()
    run_pipeline(args.mode)
