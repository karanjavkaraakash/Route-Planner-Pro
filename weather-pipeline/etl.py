"""
RoutePlannerPro Weather Pipeline v2.3
======================================
GFS wind + MSLP only. WW3 removed — unreliable on NOMADS (intermittent 404s).
Wave data is fetched per-route from Open-Meteo marine in the routing tool.

Sources:
  - GFS 0.5 wind (U/V 10m)  -> wind_speed, wind_dir
  - GFS 0.5 MSLP             -> mslp
  - Wave columns kept in schema for future CMEMS Phase 3 fill

Storage strategy:
  - TRUNCATE via RPC before each run (no index bloat)
  - ~160k ocean points x 21 steps = ~3.3M rows per short run
  - Target: ~200MB well within Supabase 500MB free tier

Usage:
  python etl_v2.py --mode short   # hours 0-120
  python etl_v2.py --mode long    # hours 126-384
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
REQUEST_TIMEOUT      = 180
MAX_RETRIES          = 3
RESOLUTION           = 0.5

HOURS_SHORT = list(range(0, 121, 6))    # 21 steps
HOURS_LONG  = list(range(126, 385, 6))  # 44 steps

SCRIPT_DIR      = os.path.dirname(os.path.abspath(__file__))
OCEAN_MASK_PATH = os.path.join(SCRIPT_DIR, "ocean_points_0p5.csv")

# ── Supabase client ───────────────────────────────────────────────────────────
def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# ── Ocean mask ────────────────────────────────────────────────────────────────
def load_ocean_mask():
    mask = set()
    with open(OCEAN_MASK_PATH, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            mask.add((round(float(row['lat']), 1), round(float(row['lon']), 1)))
    log.info(f"Ocean mask loaded: {len(mask):,} points")
    return mask

# ── GFS run discovery ─────────────────────────────────────────────────────────
def latest_gfs_run():
    candidate = datetime.now(timezone.utc) - timedelta(hours=5)
    run_hour  = (candidate.hour // 6) * 6
    return candidate.strftime("%Y%m%d"), f"{run_hour:02d}"

# ── NOMADS URL builders ───────────────────────────────────────────────────────
def gfs_wind_url(date, run, fhour):
    fstr = f"{fhour:03d}"
    return (
        f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p50.pl?"
        f"file=gfs.t{run}z.pgrb2full.0p50.f{fstr}"
        f"&lev_10_m_above_ground=on&var_UGRD=on&var_VGRD=on"
        f"&leftlon=0&rightlon=360&toplat=90&bottomlat=-90"
        f"&dir=%2Fgfs.{date}%2F{run}%2Fatmos"
    )

def gfs_mslp_url(date, run, fhour):
    fstr = f"{fhour:03d}"
    return (
        f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p50.pl?"
        f"file=gfs.t{run}z.pgrb2full.0p50.f{fstr}"
        f"&lev_mean_sea_level=on&var_PRMSL=on"
        f"&leftlon=0&rightlon=360&toplat=90&bottomlat=-90"
        f"&dir=%2Fgfs.{date}%2F{run}%2Fatmos"
    )

# ── GRIB download ─────────────────────────────────────────────────────────────
def download_grib(url, label):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"  Downloading {label} (attempt {attempt})")
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 404:
                log.info(f"  {label} -> 404, skipping")
                return None
            if r.status_code == 200 and len(r.content) > 500:
                log.info(f"  {label} -> {len(r.content)/1024:.0f} KB")
                return r.content
            log.warning(f"  {label} -> HTTP {r.status_code} size={len(r.content)}")
        except requests.RequestException as e:
            log.warning(f"  Request error: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(10 * attempt)
    return None

# ── GRIB parsing ──────────────────────────────────────────────────────────────
def parse_grib(data, label):
    with tempfile.NamedTemporaryFile(suffix=".grib2", delete=False) as f:
        f.write(data)
        fname = f.name
    result = {}
    try:
        if HAS_ECCODES:
            with open(fname, "rb") as fh:
                while True:
                    msg = eccodes.codes_grib_new_from_file(fh)
                    if msg is None:
                        break
                    try:
                        short = eccodes.codes_get(msg, "shortName")
                        lats  = eccodes.codes_get_array(msg, "latitudes")
                        lons  = eccodes.codes_get_array(msg, "longitudes")
                        vals  = eccodes.codes_get_array(msg, "values")
                        result[short] = (lats, lons, vals)
                        log.info(f"    {label}: var={short} pts={len(vals):,}")
                    finally:
                        eccodes.codes_release(msg)
        elif HAS_XARRAY:
            ds = xr.open_dataset(fname, engine="cfgrib", indexpath="")
            flat_lats = ds.latitude.values.flatten()
            flat_lons = ds.longitude.values.flatten()
            for var in ds.data_vars:
                vals = ds[var].values.flatten()
                result[var] = (flat_lats, flat_lons, vals)
                log.info(f"    {label}: var={var} pts={len(vals):,}")
        else:
            log.error("No GRIB parser available. Install eccodes or cfgrib.")
    except Exception as e:
        log.error(f"  Parse error {label}: {e}")
    finally:
        os.unlink(fname)
    return result

# ── Coordinate normalisation ──────────────────────────────────────────────────
def make_key(lat, lon):
    """Normalise lon to -180..180 BEFORE snapping to 0.5 grid."""
    flat_lon = float(lon)
    if flat_lon > 180:
        flat_lon -= 360
    rlat = round(round(float(lat) / RESOLUTION) * RESOLUTION, 1)
    rlon = round(round(flat_lon   / RESOLUTION) * RESOLUTION, 1)
    return (rlat, rlon)

# ── Scaling ───────────────────────────────────────────────────────────────────
def uv_to_speed_dir(u, v):
    speed = math.sqrt(u**2 + v**2)
    dirn  = (270 - math.degrees(math.atan2(v, u))) % 360
    return speed, dirn

def scale_wind_speed(ms):
    return max(0, min(32767, round(ms * 1.94384 * 10)))

def scale_dir(deg):
    return max(0, min(360, round(float(deg) % 360)))

def scale_mslp(pa):
    return max(0, min(32767, round(pa / 100.0 - 950)))

# ── Extract variable ──────────────────────────────────────────────────────────
def extract_var(parsed, varname, ocean_mask):
    if varname not in parsed:
        return {}
    lats, lons, vals = parsed[varname]
    pts = {}
    for lat, lon, val in zip(lats, lons, vals):
        key = make_key(lat, lon)
        if key in ocean_mask and key not in pts and float(val) < 9e10:
            pts[key] = float(val)
    log.info(f"    '{varname}' -> {len(pts):,} ocean points")
    return pts

# ── Build GFS rows ────────────────────────────────────────────────────────────
def build_gfs_rows(wind_parsed, mslp_parsed, valid_time, fhour, run_time, ocean_mask):
    u_pts   = extract_var(wind_parsed, "10u",   ocean_mask)
    v_pts   = extract_var(wind_parsed, "10v",   ocean_mask)
    msl_pts = extract_var(mslp_parsed, "prmsl", ocean_mask) if mslp_parsed else {}

    rows = {}
    for key in set(u_pts) | set(v_pts) | set(msl_pts):
        lat, lon = key
        row = {
            "lat":           lat,
            "lon":           lon,
            "valid_time":    valid_time.isoformat(),
            "forecast_hour": fhour,
            "run_time":      run_time.isoformat(),
        }
        u = u_pts.get(key)
        v = v_pts.get(key)
        if u is not None and v is not None:
            spd, drn = uv_to_speed_dir(u, v)
            row["wind_speed"] = scale_wind_speed(spd)
            row["wind_dir"]   = scale_dir(drn)
        mslp = msl_pts.get(key)
        if mslp is not None:
            row["mslp"] = scale_mslp(mslp)
        rows[key] = row

    log.info(f"  Built {len(rows):,} GFS rows for fhour={fhour}")
    return list(rows.values())

# ── Upsert ────────────────────────────────────────────────────────────────────
def upsert_rows(sb, rows, label):
    if not rows:
        log.warning(f"  No rows to upsert for {label}")
        return 0
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        sb.table("weather_grid").upsert(
            batch, on_conflict="lat,lon,valid_time,run_time"
        ).execute()
        total += len(batch)
    log.info(f"  Upserted {total:,} {label} rows")
    return total

# ── Truncate via RPC ──────────────────────────────────────────────────────────
def truncate_table(sb):
    """
    TRUNCATE weather_grid via RPC — instant space reclaim, no index bloat.
    Requires this function in Supabase SQL Editor:

      CREATE OR REPLACE FUNCTION truncate_weather_grid()
      RETURNS void LANGUAGE plpgsql SECURITY DEFINER
      AS $$ BEGIN TRUNCATE TABLE weather_grid; END; $$;
    """
    for attempt in range(1, 4):
        try:
            sb.rpc("truncate_weather_grid", {}).execute()
            log.info("weather_grid TRUNCATED via RPC")
            return
        except Exception as e:
            if attempt < 3:
                log.warning(f"TRUNCATE RPC attempt {attempt} failed: {e} — retrying")
                time.sleep(3)
            else:
                log.error(f"TRUNCATE RPC failed: {e}")
                raise

# ── Pipeline log ──────────────────────────────────────────────────────────────
def log_run(sb, source, records, duration, status, error=None):
    try:
        sb.table("pipeline_run_log").insert({
            "source":           source,
            "forecast_days":    16,
            "records_upserted": records,
            "duration_secs":    round(duration, 1),
            "status":           status,
            "error_msg":        error,
        }).execute()
    except Exception as e:
        log.error(f"Pipeline log write failed: {e}")

# ── Main ──────────────────────────────────────────────────────────────────────
def run_pipeline(mode):
    t0 = time.time()
    log.info(f"GRIB parsers: eccodes={HAS_ECCODES}, xarray={HAS_XARRAY}")
    if not HAS_ECCODES and not HAS_XARRAY:
        log.error("FATAL: no GRIB parser. Install eccodes or cfgrib.")
        sys.exit(1)

    date, run = latest_gfs_run()
    run_time  = datetime.strptime(f"{date}{run}", "%Y%m%d%H").replace(tzinfo=timezone.utc)
    log.info(f"Pipeline v2.3 | mode={mode} | GFS run={date}/{run}Z")

    ocean_mask = load_ocean_mask()
    sb         = get_supabase()
    hours      = (HOURS_SHORT            if mode == "short"
                  else HOURS_LONG        if mode == "long"
                  else HOURS_SHORT + HOURS_LONG)

    truncate_table(sb)

    total_gfs = 0

    for fhour in hours:
        valid_time = run_time + timedelta(hours=fhour)
        log.info(f"── fhour={fhour:03d} -> {valid_time.strftime('%Y-%m-%d %HZ')} ──")

        wind_data   = download_grib(gfs_wind_url(date, run, fhour), f"GFS-wind f{fhour:03d}")
        wind_parsed = parse_grib(wind_data, "wind") if wind_data else {}

        mslp_data   = download_grib(gfs_mslp_url(date, run, fhour), f"GFS-mslp f{fhour:03d}")
        mslp_parsed = parse_grib(mslp_data, "mslp") if mslp_data else {}

        rows = build_gfs_rows(wind_parsed, mslp_parsed, valid_time, fhour, run_time, ocean_mask)
        total_gfs += upsert_rows(sb, rows, "GFS")

        time.sleep(1)

    duration = time.time() - t0
    log.info(f"Done | GFS={total_gfs:,} rows | {duration:.0f}s")
    log_run(sb, f"GFS/{mode}", total_gfs, duration, "success")

    # Note: VACUUM cannot run inside Supabase RPC transaction blocks.
    # Autovacuum handles cleanup automatically in the background.


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["short", "long", "all"], default="short")
    args = parser.parse_args()
    run_pipeline(args.mode)
