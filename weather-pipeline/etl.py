"""
RoutePlannerPro Weather Pipeline v2 — Optimised ETL
====================================================
Key optimisations vs v1:
  - Ocean-only grid (147k points vs 259k) via pre-baked CSV mask
  - SMALLINT storage with fixed scaling (2 bytes vs 4 per variable)
  - No PostGIS geom column — btree lat/lon index only
  - Drops wind_u/wind_v — only stores derived speed/dir
  - Deletes previous run before inserting (storage stays flat)
  - WW3 gracefully skipped beyond f120 (NOMADS limitation)

Scaling conventions (divide on read):
  wind_speed : stored ×10  (e.g. 15.3 kts → 153)
  wind_dir   : stored as-is (0–360 integer)
  wave_hs    : stored ×100 (e.g. 2.45m → 245)
  wave_tp    : stored ×10  (e.g. 8.5s → 85)
  wave_dir   : stored as-is
  swell_hs   : stored ×100
  swell_dir  : stored as-is
  mslp       : stored as (value_Pa/100 - 950) → range 0..100 for 950..1050 hPa

Usage:
  python etl_v2.py --mode short   # hours 0-120
  python etl_v2.py --mode long    # hours 120-384
  python etl_v2.py --mode all     # full 16-day
"""

import os, sys, time, argparse, logging, requests, tempfile, math, csv
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client

try:
    import eccodes
    HAS_ECCODES = True
except ImportError:
    HAS_ECCODES = False

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

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
BATCH_SIZE           = 1000
REQUEST_TIMEOUT      = 120
MAX_RETRIES          = 3
RESOLUTION           = 0.5

HOURS_SHORT = list(range(0, 121, 6))       # 21 steps
HOURS_LONG  = list(range(126, 385, 6))     # 44 steps

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OCEAN_MASK_PATH = os.path.join(SCRIPT_DIR, "ocean_points_0p5.csv")

# ── Ocean mask ────────────────────────────────────────────────────────────────
def load_ocean_mask() -> set:
    """Load pre-baked ocean point set from CSV. O(1) lookup via set."""
    mask = set()
    with open(OCEAN_MASK_PATH, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            mask.add((round(float(row['lat']), 2), round(float(row['lon']), 2)))
    log.info(f"Ocean mask loaded: {len(mask):,} points")
    return mask

# ── NOMADS helpers ────────────────────────────────────────────────────────────
def latest_gfs_run() -> tuple:
    now = datetime.now(timezone.utc)
    run_hour = (now.hour // 6) * 6
    # Step back one run to ensure it's complete (5h processing lag)
    candidate = now - timedelta(hours=5)
    run_hour = (candidate.hour // 6) * 6
    return candidate.strftime("%Y%m%d"), f"{run_hour:02d}"

def gfs_url(date, run, fhour):
    fstr = f"{fhour:03d}"
    return (
        f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p50.pl?"
        f"file=gfs.t{run}z.pgrb2full.0p50.f{fstr}"
        f"&lev_10_m_above_ground=on&var_UGRD=on&var_VGRD=on&var_PRMSL=on"
        f"&leftlon=0&rightlon=360&toplat=90&bottomlat=-90"
        f"&dir=%2Fgfs.{date}%2F{run}%2Fatmos"
    )

def ww3_url(date, run, fhour):
    fstr = f"{fhour:03d}"
    return (
        f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfswave.pl?"
        f"file=gfswave.t{run}z.global.0p25.f{fstr}.grib2"
        f"&var_HTSGW=on&var_PERPW=on&var_DIRPW=on&var_SWELL=on&var_SWDIR=on"
        f"&leftlon=0&rightlon=360&toplat=90&bottomlat=-90"
        f"&dir=%2Fgfs.{date}%2F{run}%2Fwave%2Fglobal"
    )

def download_grib(url, label):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"  Downloading {label} (attempt {attempt})")
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200 and len(r.content) > 1000:
                return r.content
            if r.status_code == 404:
                log.info(f"  {label} not available (404) — skipping")
                return None
            log.warning(f"  HTTP {r.status_code} for {label}")
        except requests.RequestException as e:
            log.warning(f"  Request error: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(10 * attempt)
    return None

# ── GRIB parsing ──────────────────────────────────────────────────────────────
def parse_grib(data: bytes) -> dict:
    with tempfile.NamedTemporaryFile(suffix=".grib2", delete=False) as f:
        f.write(data)
        fname = f.name
    result = {}
    try:
        if HAS_ECCODES:
            with open(fname, "rb") as f:
                while True:
                    msg = eccodes.codes_grib_new_from_file(f)
                    if msg is None: break
                    try:
                        short = eccodes.codes_get(msg, "shortName")
                        lats  = eccodes.codes_get_array(msg, "latitudes")
                        lons  = eccodes.codes_get_array(msg, "longitudes")
                        vals  = eccodes.codes_get_array(msg, "values")
                        result[short] = (lats, lons, vals)
                    finally:
                        eccodes.codes_release(msg)
        elif HAS_XARRAY:
            ds = xr.open_dataset(fname, engine="cfgrib", indexpath="")
            flat_lats = ds.latitude.values.flatten()
            flat_lons = ds.longitude.values.flatten()
            for var in ds.data_vars:
                result[var] = (flat_lats, flat_lons, ds[var].values.flatten())
        else:
            raise RuntimeError("Install eccodes or cfgrib+xarray")
    finally:
        os.unlink(fname)
    return result

# ── Scaling helpers ───────────────────────────────────────────────────────────
def scale_wind_speed(ms: float) -> int:
    """m/s → knots, store ×10"""
    kts = ms * 1.94384
    return max(0, min(32767, round(kts * 10)))

def scale_wave_hs(m: float) -> int:
    """metres, store ×100"""
    return max(0, min(32767, round(m * 100)))

def scale_wave_tp(s: float) -> int:
    """seconds, store ×10"""
    return max(0, min(32767, round(s * 10)))

def scale_dir(deg: float) -> int:
    """0–360 direction"""
    return max(0, min(360, round(deg % 360)))

def scale_mslp(pa: float) -> int:
    """Pa → store as (hPa - 950), range 0..100"""
    hpa = pa / 100.0
    return max(0, min(32767, round(hpa - 950)))

def uv_to_kts_dir(u, v):
    speed_ms = math.sqrt(u**2 + v**2)
    direction = (270 - math.degrees(math.atan2(v, u))) % 360
    return speed_ms, direction

# ── Row builders ──────────────────────────────────────────────────────────────
def build_gfs_rows(parsed, valid_time, fhour, run_time, ocean_mask):
    rows = {}

    def extract(varname):
        if varname not in parsed:
            return {}
        lats, lons, vals = parsed[varname]
        pts = {}
        for lat, lon, val in zip(lats, lons, vals):
            rlat = round(round(float(lat) / RESOLUTION) * RESOLUTION, 2)
            rlon = round(round(float(lon) / RESOLUTION) * RESOLUTION, 2)
            if rlon > 180: rlon = round(rlon - 360, 2)
            key = (rlat, rlon)
            if key in ocean_mask and key not in pts:
                pts[key] = float(val)
        return pts

    u_pts   = extract("10u")
    v_pts   = extract("10v")
    msl_pts = extract("prmsl")

    all_keys = set(u_pts) | set(v_pts) | set(msl_pts)

    for key in all_keys:
        lat, lon = key
        row = {
            "lat": lat,
            "lon": lon,
            "valid_time": valid_time.isoformat(),
            "forecast_hour": fhour,
            "run_time": run_time.isoformat(),
        }
        u = u_pts.get(key)
        v = v_pts.get(key)
        if u is not None and v is not None and u < 9e19 and v < 9e19:
            speed_ms, direction = uv_to_kts_dir(u, v)
            row["wind_speed"] = scale_wind_speed(speed_ms)
            row["wind_dir"]   = scale_dir(direction)
        mslp = msl_pts.get(key)
        if mslp is not None and mslp < 9e19:
            row["mslp"] = scale_mslp(mslp)
        rows[key] = row

    return list(rows.values())

def build_ww3_rows(parsed, valid_time, fhour, run_time, ocean_mask):
    rows = {}
    mapping = {
        "swh":   ("wave_hs",  scale_wave_hs),
        "perpw": ("wave_tp",  scale_wave_tp),
        "dirpw": ("wave_dir", scale_dir),
        "swell": ("swell_hs", scale_wave_hs),
        "swdir": ("swell_dir",scale_dir),
    }

    for src, (dst, scaler) in mapping.items():
        if src not in parsed:
            continue
        lats, lons, vals = parsed[src]
        for lat, lon, val in zip(lats, lons, vals):
            rlat = round(round(float(lat) / RESOLUTION) * RESOLUTION, 2)
            rlon = round(round(float(lon) / RESOLUTION) * RESOLUTION, 2)
            if rlon > 180: rlon = round(rlon - 360, 2)
            key = (rlat, rlon)
            if key not in ocean_mask: continue
            if float(val) > 9e19: continue
            if key not in rows:
                rows[key] = {
                    "lat": rlat, "lon": rlon,
                    "valid_time": valid_time.isoformat(),
                    "forecast_hour": fhour,
                    "run_time": run_time.isoformat(),
                }
            rows[key][dst] = scaler(float(val))

    return list(rows.values())

# ── Supabase upsert ───────────────────────────────────────────────────────────
def upsert_rows(sb, rows, label):
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        sb.table("weather_grid").upsert(
            batch,
            on_conflict="lat,lon,valid_time,run_time"
        ).execute()
        total += len(batch)
    log.info(f"    Upserted {total} {label} rows")
    return total

def delete_old_runs(sb, current_run_time):
    """Delete all rows not from current run to keep storage flat."""
    sb.table("weather_grid")\
      .delete()\
      .neq("run_time", current_run_time.isoformat())\
      .execute()
    log.info("Deleted previous run data")

def log_run(sb, source, records, duration, status, error=None):
    sb.table("pipeline_run_log").insert({
        "source": source,
        "forecast_days": 16,
        "records_upserted": records,
        "duration_secs": round(duration, 1),
        "status": status,
        "error_msg": error,
    }).execute()

# ── Main ──────────────────────────────────────────────────────────────────────
def run_pipeline(mode):
    t0 = time.time()
    date, run = latest_gfs_run()
    run_time = datetime.strptime(f"{date}{run}", "%Y%m%d%H").replace(tzinfo=timezone.utc)
    log.info(f"Pipeline v2 | mode={mode} | GFS run={date}/{run}Z")

    ocean_mask = load_ocean_mask()
    sb = get_supabase()

    hours = HOURS_SHORT if mode == "short" else HOURS_LONG if mode == "long" else HOURS_SHORT + HOURS_LONG

    # Delete previous run first (keeps storage flat)
    delete_old_runs(sb, run_time)

    total_gfs = total_ww3 = 0

    for fhour in hours:
        valid_time = run_time + timedelta(hours=fhour)
        log.info(f"fhour={fhour:03d} → {valid_time.strftime('%Y-%m-%d %HZ')}")

        # GFS
        data = download_grib(gfs_url(date, run, fhour), f"GFS f{fhour:03d}")
        if data:
            try:
                parsed = parse_grib(data)
                rows = build_gfs_rows(parsed, valid_time, fhour, run_time, ocean_mask)
                total_gfs += upsert_rows(sb, rows, "GFS")
            except Exception as e:
                log.error(f"GFS error at f{fhour:03d}: {e}")

        # WW3 (only available to f120)
        if fhour <= 120:
            data = download_grib(ww3_url(date, run, fhour), f"WW3 f{fhour:03d}")
            if data:
                try:
                    parsed = parse_grib(data)
                    rows = build_ww3_rows(parsed, valid_time, fhour, run_time, ocean_mask)
                    total_ww3 += upsert_rows(sb, rows, "WW3")
                except Exception as e:
                    log.error(f"WW3 error at f{fhour:03d}: {e}")

        time.sleep(1)  # be a good NOMADS citizen

    duration = time.time() - t0
    log.info(f"Done | GFS={total_gfs} WW3={total_ww3} rows | {duration:.0f}s")
    log_run(sb, f"GFS+WW3/{mode}", total_gfs + total_ww3, duration, "success")

def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["short","long","all"], default="short")
    args = parser.parse_args()
    run_pipeline(args.mode)
