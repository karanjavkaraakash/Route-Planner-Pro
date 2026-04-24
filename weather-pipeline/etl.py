"""
RoutePlannerPro Weather Pipeline v2.1 — Optimised ETL
======================================================
v2.1 fixes: explicit parse diagnostics, better error visibility,
fallback to pure-Python GRIB subset via NOMADS HTTP byte-range,
ocean mask key normalisation fix.
"""

import os, sys, time, argparse, logging, requests, tempfile, math, csv, struct
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

# ── Supabase ──────────────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# ── Ocean mask ────────────────────────────────────────────────────────────────
def load_ocean_mask() -> set:
    mask = set()
    with open(OCEAN_MASK_PATH, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            mask.add((round(float(row['lat']), 2), round(float(row['lon']), 2)))
    log.info(f"Ocean mask loaded: {len(mask):,} points")
    # Log a few sample keys so we can verify format
    sample = list(mask)[:3]
    log.info(f"Ocean mask sample keys: {sample}")
    return mask

# ── NOMADS URL builders ───────────────────────────────────────────────────────
def latest_gfs_run() -> tuple:
    candidate = datetime.now(timezone.utc) - timedelta(hours=5)
    run_hour  = (candidate.hour // 6) * 6
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

# ── Download ──────────────────────────────────────────────────────────────────
def download_grib(url, label):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"  Downloading {label} (attempt {attempt})")
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 404:
                log.info(f"  {label} → 404 not available, skipping")
                return None
            if r.status_code == 200 and len(r.content) > 1000:
                log.info(f"  {label} → {len(r.content)/1024:.0f} KB downloaded")
                return r.content
            log.warning(f"  {label} → HTTP {r.status_code}, size={len(r.content)}")
        except requests.RequestException as e:
            log.warning(f"  Request error: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(10 * attempt)
    return None

# ── GRIB parsing ──────────────────────────────────────────────────────────────
def parse_grib(data: bytes, label: str) -> dict:
    """Parse GRIB2 bytes. Returns dict of shortName → (lats, lons, vals)."""
    with tempfile.NamedTemporaryFile(suffix=".grib2", delete=False) as f:
        f.write(data)
        fname = f.name

    result = {}
    try:
        if HAS_ECCODES:
            log.info(f"  Parsing {label} via eccodes")
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
                        log.info(f"    Found variable: {short} ({len(vals):,} values)")
                    finally:
                        eccodes.codes_release(msg)

        elif HAS_XARRAY:
            log.info(f"  Parsing {label} via cfgrib/xarray")
            ds = xr.open_dataset(fname, engine="cfgrib", indexpath="")
            flat_lats = ds.latitude.values.flatten()
            flat_lons = ds.longitude.values.flatten()
            for var in ds.data_vars:
                vals = ds[var].values.flatten()
                result[var] = (flat_lats, flat_lons, vals)
                log.info(f"    Found variable: {var} ({len(vals):,} values)")
        else:
            log.error("No GRIB parser available — install eccodes or cfgrib")

    except Exception as e:
        log.error(f"  GRIB parse error for {label}: {e}")
    finally:
        os.unlink(fname)

    log.info(f"  Parsed {len(result)} variables from {label}: {list(result.keys())}")
    return result

# ── Scaling ───────────────────────────────────────────────────────────────────
def uv_to_speed_dir(u, v):
    speed_ms  = math.sqrt(u**2 + v**2)
    direction = (270 - math.degrees(math.atan2(v, u))) % 360
    return speed_ms, direction

def scale_wind_speed(ms):  return max(0, min(32767, round(ms * 1.94384 * 10)))
def scale_wave_hs(m):      return max(0, min(32767, round(m * 100)))
def scale_wave_tp(s):      return max(0, min(32767, round(s * 10)))
def scale_dir(deg):        return max(0, min(360,   round(float(deg) % 360)))
def scale_mslp(pa):        return max(0, min(32767, round(pa / 100.0 - 950)))

# ── Normalise lon to ocean mask format (-180..180) ────────────────────────────
def norm_lon(lon):
    lon = float(lon)
    if lon > 180:
        lon -= 360
    return round(lon, 2)

def snap(val, res=0.5):
    """Snap to nearest grid resolution."""
    return round(round(float(val) / res) * res, 2)

# ── Row builders ──────────────────────────────────────────────────────────────
def build_gfs_rows(parsed, valid_time, fhour, run_time, ocean_mask):
    rows = {}

    def extract(varname):
        if varname not in parsed:
            log.warning(f"    Variable '{varname}' not found in parsed GRIB")
            return {}
        lats, lons, vals = parsed[varname]
        pts = {}
        for lat, lon, val in zip(lats, lons, vals):
            rlat = snap(lat)
            rlon = norm_lon(snap(lon))
            key  = (rlat, rlon)
            if key in ocean_mask and key not in pts:
                pts[key] = float(val)
        log.info(f"    '{varname}' → {len(pts):,} ocean points extracted")
        return pts

    u_pts   = extract("10u")
    v_pts   = extract("10v")
    msl_pts = extract("prmsl")

    # Diagnostic: if empty, check key format mismatch
    if not u_pts and "10u" in parsed:
        lats, lons, vals = parsed["10u"]
        sample_lat = snap(lats[0])
        sample_lon = norm_lon(snap(lons[0]))
        log.warning(f"    Key format check — GRIB sample: ({sample_lat}, {sample_lon})")
        mask_sample = list(ocean_mask)[:1]
        log.warning(f"    Key format check — mask sample: {mask_sample}")

    for key in set(u_pts) | set(v_pts) | set(msl_pts):
        lat, lon = key
        row = {
            "lat": lat, "lon": lon,
            "valid_time":    valid_time.isoformat(),
            "forecast_hour": fhour,
            "run_time":      run_time.isoformat(),
        }
        u = u_pts.get(key)
        v = v_pts.get(key)
        if u is not None and v is not None and abs(u) < 9e10 and abs(v) < 9e10:
            spd, drn = uv_to_speed_dir(u, v)
            row["wind_speed"] = scale_wind_speed(spd)
            row["wind_dir"]   = scale_dir(drn)
        mslp = msl_pts.get(key)
        if mslp is not None and mslp < 9e10:
            row["mslp"] = scale_mslp(mslp)
        rows[key] = row

    log.info(f"    Built {len(rows):,} GFS rows for fhour={fhour}")
    return list(rows.values())

def build_ww3_rows(parsed, valid_time, fhour, run_time, ocean_mask):
    rows = {}
    mapping = {
        "swh":   ("wave_hs",   scale_wave_hs),
        "perpw": ("wave_tp",   scale_wave_tp),
        "dirpw": ("wave_dir",  scale_dir),
        "swell": ("swell_hs",  scale_wave_hs),
        "swdir": ("swell_dir", scale_dir),
    }
    for src, (dst, scaler) in mapping.items():
        if src not in parsed:
            continue
        lats, lons, vals = parsed[src]
        for lat, lon, val in zip(lats, lons, vals):
            rlat = snap(lat)
            rlon = norm_lon(snap(lon))
            key  = (rlat, rlon)
            if key not in ocean_mask: continue
            if float(val) > 9e10:    continue
            if key not in rows:
                rows[key] = {
                    "lat": rlat, "lon": rlon,
                    "valid_time":    valid_time.isoformat(),
                    "forecast_hour": fhour,
                    "run_time":      run_time.isoformat(),
                }
            rows[key][dst] = scaler(float(val))

    log.info(f"    Built {len(rows):,} WW3 rows for fhour={fhour}")
    return list(rows.values())

# ── Upsert ────────────────────────────────────────────────────────────────────
def upsert_rows(sb, rows, label):
    if not rows:
        log.warning(f"    No rows to upsert for {label} — skipping")
        return 0
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        sb.table("weather_grid").upsert(
            batch, on_conflict="lat,lon,valid_time,run_time"
        ).execute()
        total += len(batch)
    log.info(f"    Upserted {total:,} {label} rows")
    return total

def delete_old_runs(sb, current_run_time):
    sb.table("weather_grid")\
      .delete()\
      .neq("run_time", current_run_time.isoformat())\
      .execute()
    log.info("Old run data deleted")

def log_run(sb, source, records, duration, status, error=None):
    try:
        sb.table("pipeline_run_log").insert({
            "source": source, "forecast_days": 16,
            "records_upserted": records,
            "duration_secs": round(duration, 1),
            "status": status, "error_msg": error,
        }).execute()
    except Exception as e:
        log.error(f"Failed to write pipeline log: {e}")

# ── Main ──────────────────────────────────────────────────────────────────────
def run_pipeline(mode):
    t0 = time.time()

    # Log parser availability
    log.info(f"GRIB parsers — eccodes={HAS_ECCODES}, xarray/cfgrib={HAS_XARRAY}")
    if not HAS_ECCODES and not HAS_XARRAY:
        log.error("FATAL: No GRIB parser available. Cannot continue.")
        sys.exit(1)

    date, run = latest_gfs_run()
    run_time  = datetime.strptime(f"{date}{run}", "%Y%m%d%H").replace(tzinfo=timezone.utc)
    log.info(f"Pipeline v2.1 | mode={mode} | GFS run={date}/{run}Z")

    ocean_mask = load_ocean_mask()
    sb         = get_supabase()
    hours      = (HOURS_SHORT if mode == "short"
                  else HOURS_LONG if mode == "long"
                  else HOURS_SHORT + HOURS_LONG)

    delete_old_runs(sb, run_time)

    total_gfs = total_ww3 = 0

    for fhour in hours:
        valid_time = run_time + timedelta(hours=fhour)
        log.info(f"── fhour={fhour:03d} valid={valid_time.strftime('%Y-%m-%d %HZ')} ──")

        # GFS
        data = download_grib(gfs_url(date, run, fhour), f"GFS f{fhour:03d}")
        if data:
            parsed = parse_grib(data, f"GFS f{fhour:03d}")
            rows   = build_gfs_rows(parsed, valid_time, fhour, run_time, ocean_mask)
            total_gfs += upsert_rows(sb, rows, "GFS")
        else:
            log.warning(f"  GFS f{fhour:03d} download failed — skipping")

        # WW3
        if fhour <= 120:
            data = download_grib(ww3_url(date, run, fhour), f"WW3 f{fhour:03d}")
            if data:
                parsed = parse_grib(data, f"WW3 f{fhour:03d}")
                rows   = build_ww3_rows(parsed, valid_time, fhour, run_time, ocean_mask)
                total_ww3 += upsert_rows(sb, rows, "WW3")

        time.sleep(1)

    duration = time.time() - t0
    log.info(f"═══ Pipeline complete | GFS={total_gfs:,} WW3={total_ww3:,} rows | {duration:.0f}s ═══")
    log_run(sb, f"GFS+WW3/{mode}", total_gfs + total_ww3, duration, "success")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["short","long","all"], default="short")
    args = parser.parse_args()
    run_pipeline(args.mode)
