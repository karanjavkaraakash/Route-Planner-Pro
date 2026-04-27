"""
RoutePlannerPro Weather Pipeline — DB2 (Long Range)
====================================================
Stores GFS wind for days 5-16 at 1° resolution in a separate
Supabase project (DB2), keeping DB1 under the 500MB free tier limit.

  DB1: Days 0-5  | 0.5° | wind + MSLP | ~454MB
  DB2: Days 5-16 | 1.0° | wind only   | ~134MB

Sources:
  - GFS 1° wind (U/V 10m) → wind_speed, wind_dir
  - Hours 126-384 (days 5-16), 6-hourly = 44 steps

Refresh: every 12h (GFS long-range accuracy stable at that cadence)

Usage:
  python etl_long.py
"""

import os, sys, time, logging, requests, tempfile, math, csv
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
SUPABASE_URL         = os.environ["SUPABASE_URL_LONG"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY_LONG"]
BATCH_SIZE           = 1000
REQUEST_TIMEOUT      = 180
MAX_RETRIES          = 3
RESOLUTION           = 1.0   # 1° grid for DB2

# Hours 126-384 in 6h steps = 44 forecast steps (days 5-16)
HOURS_LONG = list(range(126, 385, 6))

SCRIPT_DIR      = os.path.dirname(os.path.abspath(__file__))
OCEAN_MASK_PATH = os.path.join(SCRIPT_DIR, "ocean_points_1deg.csv")

# ── Supabase client ───────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# ── Ocean mask ────────────────────────────────────────────────────────────────
def load_ocean_mask() -> set:
    mask = set()
    with open(OCEAN_MASK_PATH, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            mask.add((round(float(row['lat']), 1), round(float(row['lon']), 1)))
    log.info(f"Ocean mask loaded: {len(mask):,} points at 1°")
    return mask

# ── GFS run discovery ─────────────────────────────────────────────────────────
def latest_gfs_run() -> tuple:
    candidate = datetime.now(timezone.utc) - timedelta(hours=5)
    run_hour  = (candidate.hour // 6) * 6
    return candidate.strftime("%Y%m%d"), f"{run_hour:02d}"

# ── NOMADS URL builders ───────────────────────────────────────────────────────
def gfs_wind_url(date: str, run: str, fhour: int) -> str:
    """GFS 0.5° wind — thinned to 1° after download."""
    fstr = f"{fhour:03d}"
    return (
        f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p50.pl?"
        f"file=gfs.t{run}z.pgrb2full.0p50.f{fstr}"
        f"&lev_10_m_above_ground=on&var_UGRD=on&var_VGRD=on"
        f"&leftlon=0&rightlon=360&toplat=90&bottomlat=-90"
        f"&dir=%2Fgfs.{date}%2F{run}%2Fatmos"
    )

def gfs_wave_url(date: str, run: str, fhour: int) -> str:
    """GFS significant wave height (HTSGW) at surface level."""
    fstr = f"{fhour:03d}"
    return (
        f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p50.pl?"
        f"file=gfs.t{run}z.pgrb2full.0p50.f{fstr}"
        f"&lev_surface=on&var_HTSGW=on"
        f"&leftlon=0&rightlon=360&toplat=90&bottomlat=-90"
        f"&dir=%2Fgfs.{date}%2F{run}%2Fatmos"
    )

# ── Download ──────────────────────────────────────────────────────────────────
def download_grib(url: str, label: str) -> bytes | None:
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
            log.warning(f"  {label} -> HTTP {r.status_code}")
        except requests.RequestException as e:
            log.warning(f"  Request error: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(10 * attempt)
    return None

# ── GRIB parsing ──────────────────────────────────────────────────────────────
def parse_grib(data: bytes, label: str) -> dict:
    with tempfile.NamedTemporaryFile(suffix=".grib2", delete=False) as f:
        f.write(data)
        fname = f.name
    result = {}
    try:
        if HAS_ECCODES:
            with open(fname, "rb") as fh:
                while True:
                    msg = eccodes.codes_grib_new_from_file(fh)
                    if msg is None: break
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
            log.error("No GRIB parser available.")
    except Exception as e:
        log.error(f"  Parse error {label}: {e}")
    finally:
        os.unlink(fname)
    return result

# ── Coordinate normalisation ──────────────────────────────────────────────────
def make_key(lat: float, lon: float) -> tuple:
    """Normalise lon to -180..180 BEFORE snapping to 1° grid."""
    flat_lon = float(lon)
    if flat_lon > 180:
        flat_lon -= 360
    rlat = round(round(float(lat) / RESOLUTION) * RESOLUTION, 1)
    rlon = round(round(flat_lon   / RESOLUTION) * RESOLUTION, 1)
    return (rlat, rlon)

# ── Scaling ───────────────────────────────────────────────────────────────────
def uv_to_speed_dir(u: float, v: float) -> tuple:
    speed = math.sqrt(u**2 + v**2)
    dirn  = (270 - math.degrees(math.atan2(v, u))) % 360
    return speed, dirn

def scale_wind_speed(ms: float) -> int:
    return max(0, min(32767, round(ms * 1.94384 * 10)))

def scale_dir(deg: float) -> int:
    return max(0, min(360, round(float(deg) % 360)))

def scale_wave_height(metres: float) -> int:
    """Store wave height as integer centimetres (×100). Range: 0–327m."""
    return max(0, min(32767, round(float(metres) * 100)))

# ── Extract variable ──────────────────────────────────────────────────────────
def extract_var(parsed: dict, varname: str, ocean_mask: set) -> dict:
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

# ── Build rows ────────────────────────────────────────────────────────────────
def build_rows(wind_parsed: dict, wave_parsed: dict, valid_time: datetime,
               fhour: int, run_time: datetime, ocean_mask: set) -> list:
    u_pts    = extract_var(wind_parsed, "10u",  ocean_mask)
    v_pts    = extract_var(wind_parsed, "10v",  ocean_mask)
    wave_pts = extract_var(wave_parsed, "shww", ocean_mask) if wave_parsed else {}
    if not wave_pts:
        wave_pts = extract_var(wave_parsed, "swh",   ocean_mask) if wave_parsed else {}
    if not wave_pts:
        wave_pts = extract_var(wave_parsed, "htsgw", ocean_mask) if wave_parsed else {}

    rows = {}
    for key in set(u_pts) | set(v_pts):
        lat, lon = key
        u = u_pts.get(key)
        v = v_pts.get(key)
        if u is None or v is None:
            continue
        spd, drn = uv_to_speed_dir(u, v)
        row = {
            "lat":           lat,
            "lon":           lon,
            "valid_time":    valid_time.isoformat(),
            "forecast_hour": fhour,
            "run_time":      run_time.isoformat(),
            "wind_speed":    scale_wind_speed(spd),
            "wind_dir":      scale_dir(drn),
        }
        wh = wave_pts.get(key)
        if wh is not None:
            row["wave_height"] = scale_wave_height(wh)
        rows[key] = row

    log.info(f"  Built {len(rows):,} rows for fhour={fhour} "
             f"(wave_height: {len(wave_pts):,} pts)")
    return list(rows.values())

# ── Upsert ────────────────────────────────────────────────────────────────────
def upsert_rows(sb: Client, rows: list) -> int:
    if not rows:
        return 0
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        sb.table("weather_grid_long").upsert(
            batch, on_conflict="lat,lon,valid_time,run_time"
        ).execute()
        total += len(batch)
    log.info(f"  Upserted {total:,} rows")
    return total

# ── Truncate via RPC ──────────────────────────────────────────────────────────
def truncate_table(sb: Client):
    for attempt in range(1, 4):
        try:
            sb.rpc("truncate_weather_grid_long", {}).execute()
            log.info("weather_grid_long TRUNCATED ✓")
            return
        except Exception as e:
            if attempt < 3:
                log.warning(f"TRUNCATE RPC attempt {attempt} failed: {e}")
                time.sleep(3)
            else:
                log.error(f"TRUNCATE RPC failed: {e}")
                raise

# ── Pipeline log ──────────────────────────────────────────────────────────────
def log_run(sb: Client, records: int, duration: float, status: str, error: str = None):
    try:
        sb.table("pipeline_run_log").insert({
            "source":           "GFS-long/1deg",
            "forecast_days":    16,
            "records_upserted": records,
            "duration_secs":    round(duration, 1),
            "status":           status,
            "error_msg":        error,
        }).execute()
    except Exception as e:
        log.error(f"Log write failed: {e}")

# ── Main ──────────────────────────────────────────────────────────────────────
def run_pipeline():
    t0 = time.time()
    log.info(f"GRIB parsers: eccodes={HAS_ECCODES}, xarray={HAS_XARRAY}")
    if not HAS_ECCODES and not HAS_XARRAY:
        log.error("FATAL: no GRIB parser.")
        sys.exit(1)

    date, run = latest_gfs_run()
    run_time  = datetime.strptime(f"{date}{run}", "%Y%m%d%H").replace(tzinfo=timezone.utc)
    log.info(f"DB2 Pipeline | GFS run={date}/{run}Z | {len(HOURS_LONG)} steps")

    ocean_mask = load_ocean_mask()
    sb         = get_supabase()

    truncate_table(sb)

    total = 0
    for fhour in HOURS_LONG:
        valid_time = run_time + timedelta(hours=fhour)
        log.info(f"── fhour={fhour:03d} -> {valid_time.strftime('%Y-%m-%d %HZ')} ──")

        wind_data   = download_grib(gfs_wind_url(date, run, fhour), f"GFS f{fhour:03d}")
        wind_parsed = parse_grib(wind_data, "wind") if wind_data else {}

        wave_data   = download_grib(gfs_wave_url(date, run, fhour), f"GFS-wave f{fhour:03d}")
        wave_parsed = parse_grib(wave_data, "wave") if wave_data else {}

        rows = build_rows(wind_parsed, wave_parsed, valid_time, fhour, run_time, ocean_mask)
        total += upsert_rows(sb, rows)

        time.sleep(1)

    duration = time.time() - t0
    log.info(f"DB2 Done | {total:,} rows | {duration:.0f}s")
    log_run(sb, total, duration, "success")

    # Note: VACUUM cannot run inside Supabase RPC transaction blocks.
    # Autovacuum handles cleanup automatically in the background.


if __name__ == "__main__":
    run_pipeline()
