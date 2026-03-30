#!/usr/bin/env python3
"""
RoutePlannerPro — Maritime Routing + GFS Weather Server
Deploy to Render.com
pip install flask searoute gunicorn requests numpy
"""
import math, os, struct, io, json, time, threading, zlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from flask import Flask, jsonify, request, send_from_directory, Response
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder=".")
ENGINE = None
GRAPH  = None

# ── GFS WEATHER CACHE ─────────────────────────────────────────────────────────
# Cache structure: {cache_key: {data: [...], fetched_at: timestamp}}
_wx_cache = {}
_wx_lock  = threading.Lock()
WX_CACHE_TTL = 3600  # 1 hour

def haversine_km(lon1, lat1, lon2, lat2):
    R=6371.0; r=math.pi/180
    dlat=(lat2-lat1)*r; dlon=(lon2-lon1)*r
    a=math.sin(dlat/2)**2+math.cos(lat1*r)*math.cos(lat2*r)*math.sin(dlon/2)**2
    return R*2*math.asin(math.sqrt(max(0,a)))

def load_engine():
    global ENGINE, GRAPH
    try:
        from scgraph.geographs.marnet import marnet_geograph
        GRAPH=marnet_geograph; ENGINE='scgraph'
        log.info(f"Engine: scgraph — {len(GRAPH.graph)} nodes"); return True
    except: pass
    try:
        import searoute as sr
        GRAPH=sr; ENGINE='searoute'
        log.info("Engine: searoute-py"); return True
    except: pass
    log.error("No routing engine. pip install searoute"); return False

PASSAGE_MAP = {
    'suez':'suez','panama':'panama','malacca':'malacca',
    'gibraltar':'gibraltar','babalmandab':'babalmandab',
    'northwest':'northwest','northeast':'northeast',
    'magellan':'chili','sunda':'sunda','ormuz':'ormuz','kiel':'kiel',
}

def detect_passages(coords):
    passages=[]
    for lon,lat in coords:
        if 32.0<lon<33.0 and 29.5<lat<31.5:
            if 'suez' not in passages: passages.append('suez')
        if -80.0<lon<-79.0 and 8.7<lat<9.5:
            if 'panama' not in passages: passages.append('panama')
        if 99.0<lon<104.0 and 1.0<lat<6.0:
            if 'malacca' not in passages: passages.append('malacca')
        if -6.0<lon<-5.0 and 35.7<lat<36.2:
            if 'gibraltar' not in passages: passages.append('gibraltar')
        if 43.0<lon<44.0 and 11.5<lat<13.5:
            if 'babalmandab' not in passages: passages.append('babalmandab')
    return passages

def name_from_passages(passages):
    if not passages: return "OPEN OCEAN"
    p=[x.lower() for x in passages]
    if 'suez' in p:      return "VIA SUEZ CANAL"
    if 'panama' in p:    return "VIA PANAMA CANAL"
    if 'chili' in p:     return "VIA STRAIT OF MAGELLAN"
    if 'northwest' in p: return "VIA NORTHWEST PASSAGE"
    return "VIA "+" & ".join([x.upper() for x in p])

def needs_babalmandab(olon, olat, dlon, dlat):
    west=(olon<50 and olat>0) or (dlon<50 and dlat>0)
    east=(olon>55) or (dlon>55)
    return west and east

def route_scgraph(olon,olat,dlon,dlat,restrictions):
    try:
        result=GRAPH.get_shortest_path(
            origin_node={"latitude":olat,"longitude":olon},
            destination_node={"latitude":dlat,"longitude":dlon},
            output_units='km',node_addition_type='quadrant',
            destination_node_addition_type='all',
        )
        if not result or 'coordinate_path' not in result:
            return {"error":"No route found"}
        coords=[[c['longitude'],c['latitude']] for c in result['coordinate_path']]
        total_km=result.get('length',0)
        passages=detect_passages(coords)
        return {"coordinates":coords,"distance_km":round(total_km,1),
                "distance_nm":round(total_km/1.852,1),
                "route_name":name_from_passages(passages),
                "passages":passages,"node_count":len(coords),"warning":None}
    except Exception as e:
        return {"error":str(e)}

def route_searoute(olon,olat,dlon,dlat,restrictions):
    import searoute as sr
    sr_r=[PASSAGE_MAP[r] for r in restrictions if r in PASSAGE_MAP]
    if needs_babalmandab(olon,olat,dlon,dlat):
        if 'babalmandab' in sr_r: sr_r.remove('babalmandab')
    try:
        route=sr.searoute([olon,olat],[dlon,dlat],units="km",
                          append_orig_dest=True,restrictions=sr_r,return_passages=True)
    except Exception as e:
        return {"error":str(e)}
    if not route: return {"error":"No route found"}
    geom=route.get("geometry",{}); coords=geom.get("coordinates",[])
    props=route.get("properties",{}); total_km=props.get("length",0)
    passages=props.get("passages",[])
    if isinstance(passages,str): passages=[passages] if passages else []
    return {"coordinates":coords,"distance_km":round(total_km,1),
            "distance_nm":round(total_km/1.852,1),
            "route_name":name_from_passages(passages),
            "passages":passages,"node_count":len(coords),"warning":None}

# ══════════════════════════════════════════════════════════════════════════════
# GFS WEATHER ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def get_latest_gfs_cycle():
    """
    Returns (date_str YYYYMMDD, cycle_hour '00'/'06'/'12'/'18')
    for the most recent completed GFS run (runs take ~4h to complete).
    """
    now = datetime.now(timezone.utc)
    # GFS runs at 00,06,12,18 UTC. Each takes ~4h to complete.
    # Go back 6h to ensure we get a completed run.
    run_time = now - timedelta(hours=6)
    cycle = (run_time.hour // 6) * 6
    return run_time.strftime('%Y%m%d'), f'{cycle:02d}'


def fetch_gfs_grid(var_type, forecast_hour_str):
    """
    Fetch GFS 1-degree grid for the requested variable type.
    var_type: 'wind' | 'wave' | 'current'
    forecast_hour_str: '000','006','012',...,'384'

    Uses NOAA NOMADS grib-filter to download only the variables we need.
    Returns list of {lat, lon, u, v, spd, dir} dicts.
    """
    import requests

    date_str, cycle = get_latest_gfs_cycle()
    fhr = forecast_hour_str.zfill(3)

    # Cache key
    cache_key = f"{var_type}_{date_str}_{cycle}_{fhr}"
    with _wx_lock:
        if cache_key in _wx_cache:
            entry = _wx_cache[cache_key]
            if time.time() - entry['fetched_at'] < WX_CACHE_TTL:
                log.info(f"Cache hit: {cache_key}")
                return entry['data'], None

    log.info(f"Fetching GFS {var_type} for {date_str} cycle {cycle}z fhr {fhr}")

    try:
        if var_type == 'wave':
            # GFS Wave model — separate product
            # Variables: HTSGW (significant wave height), WVDIR (wave direction)
            url = (
                f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfswave.pl"
                f"?file=gfswave.t{cycle}z.global.0p25.f{fhr}.grib2"
                f"&var_HTSGW=on&var_WDIR=on"
                f"&lev_surface=on"
                f"&subregion=&leftlon=-180&rightlon=180&toplat=80&bottomlat=-80"
                f"&dir=%2Fgfs.{date_str}%2F{cycle}%2Fwave%2Fgridded"
            )
        else:
            # GFS Atmospheric — wind (UGRD/VGRD at 10m)
            url = (
                f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_1p00.pl"
                f"?file=gfs.t{cycle}z.pgrb2.1p00.f{fhr}"
                f"&var_UGRD=on&var_VGRD=on"
                f"&lev_10_m_above_ground=on"
                f"&subregion=&leftlon=-180&rightlon=180&toplat=80&bottomlat=-80"
                f"&dir=%2Fgfs.{date_str}%2F{cycle}%2Fatmos"
            )

        resp = requests.get(url, timeout=60, stream=True)
        if resp.status_code != 200:
            return None, f"NOMADS HTTP {resp.status_code}"

        grib_bytes = resp.content
        if len(grib_bytes) < 100:
            return None, "Empty response from NOMADS"

        # Parse GRIB2 data
        data = parse_grib2_wind(grib_bytes, var_type)
        if not data:
            return None, "GRIB2 parse failed"

        with _wx_lock:
            _wx_cache[cache_key] = {'data': data, 'fetched_at': time.time()}

        log.info(f"Fetched {len(data)} grid points for {cache_key}")
        return data, None

    except Exception as e:
        log.error(f"GFS fetch error: {e}")
        return None, str(e)


def parse_grib2_wind(grib_bytes, var_type):
    """
    Pure-Python GRIB2 parser for wind U/V components and wave height.
    Extracts the data section and returns a list of {lat,lon,u,v,spd,dir} dicts.

    GRIB2 structure:
    Section 0: Indicator (16 bytes)
    Section 1: Identification
    Section 2: Local use (optional)
    Section 3: Grid Definition
    Section 4: Product Definition
    Section 5: Data Representation
    Section 6: Bit Map
    Section 7: Data
    Section 8: End (4 bytes '7777')
    """
    try:
        # Try cfgrib first (most reliable)
        import cfgrib
        import tempfile, numpy as np

        with tempfile.NamedTemporaryFile(suffix='.grib2', delete=False) as f:
            f.write(grib_bytes)
            fname = f.name

        try:
            datasets = cfgrib.open_datasets(fname)
            result = []
            u_data = v_data = wave_data = None
            lats = lons = None

            for ds in datasets:
                if 'u10' in ds and 'v10' in ds:
                    u_data = ds['u10'].values
                    v_data = ds['v10'].values
                    lats = ds.coords['latitude'].values
                    lons = ds.coords['longitude'].values
                if 'swh' in ds:  # significant wave height
                    wave_data = ds['swh'].values
                    lats = ds.coords['latitude'].values
                    lons = ds.coords['longitude'].values

            if lats is None:
                return None

            for i, lat in enumerate(lats):
                for j, lon in enumerate(lons):
                    # Normalise longitude to -180..180
                    lo = float(lon)
                    if lo > 180: lo -= 360
                    la = float(lat)

                    if var_type == 'wave':
                        spd = float(wave_data[i,j]) if wave_data is not None else 1.5
                        if np.isnan(spd) or spd < 0: continue  # land
                        result.append({'lat':la,'lon':lo,'spd':spd,'dir':0,'u':0,'v':0})
                    else:
                        if u_data is None: continue
                        u = float(u_data[i,j]); v = float(v_data[i,j])
                        if np.isnan(u) or np.isnan(v): continue
                        spd = math.sqrt(u*u + v*v)
                        # Meteorological direction: direction FROM which wind blows
                        direction = (math.degrees(math.atan2(-u, -v)) + 360) % 360
                        result.append({'lat':la,'lon':lo,'u':u,'v':v,
                                       'spd':round(spd,2),'dir':round(direction,1)})
            os.unlink(fname)
            return result if result else None

        except Exception as e:
            log.warning(f"cfgrib parse failed: {e}, trying fallback")
            try: os.unlink(fname)
            except: pass
            return parse_grib2_simple(grib_bytes, var_type)

    except ImportError:
        # cfgrib not available — use simple parser
        return parse_grib2_simple(grib_bytes, var_type)


def parse_grib2_simple(grib_bytes, var_type):
    """
    Fallback: minimal GRIB2 parser that extracts a regular lat-lon grid.
    Works for GFS 1-degree and 0.25-degree regular grids.
    Only handles simple packing (data representation template 0).
    """
    try:
        data = io.BytesIO(grib_bytes)
        messages = []
        pos = 0
        buf = grib_bytes

        while pos < len(buf) - 4:
            # Find GRIB marker
            idx = buf.find(b'GRIB', pos)
            if idx < 0: break
            pos = idx

            # Section 0: Total length
            if len(buf) < pos + 16: break
            total_len = struct.unpack_from('>Q', buf, pos+8)[0]
            if total_len < 16 or pos + total_len > len(buf) + 1000:
                pos += 4; continue

            msg_bytes = buf[pos:pos+int(total_len)]
            msg = parse_single_grib2_message(msg_bytes)
            if msg: messages.append(msg)
            pos += max(int(total_len), 4)

        if not messages: return None

        # Build grid from messages
        # For wind: need both U and V components
        u_msg = v_msg = wave_msg = None
        for m in messages:
            cat, num = m.get('category',0), m.get('parameter',0)
            # Wind: category 2, U=2, V=3
            if cat == 2 and num == 2: u_msg = m
            if cat == 2 and num == 3: v_msg = m
            # Wave height: category 0, num 3 or discipline 10, cat 0, num 3
            if m.get('discipline',0) == 10 and cat == 0 and num == 3:
                wave_msg = m

        result = []
        if var_type == 'wave' and wave_msg:
            grid = wave_msg
            vals = grid['values']
            lats = grid['lats']
            lons = grid['lons']
            for i, (la, lo, spd) in enumerate(zip(lats, lons, vals)):
                if spd is None or spd < 0 or spd > 50: continue
                lo = lo if lo <= 180 else lo - 360
                result.append({'lat':round(la,2),'lon':round(lo,2),
                               'spd':round(spd,2),'dir':0,'u':0,'v':0})
        elif u_msg and v_msg:
            u_vals = u_msg['values']
            v_vals = v_msg['values']
            lats = u_msg['lats']
            lons = u_msg['lons']
            for la, lo, u, v in zip(lats, lons, u_vals, v_vals):
                if u is None or v is None: continue
                spd = math.sqrt(u*u + v*v)
                direction = (math.degrees(math.atan2(-u, -v)) + 360) % 360
                lo = lo if lo <= 180 else lo - 360
                result.append({'lat':round(la,2),'lon':round(lo,2),
                               'u':round(u,2),'v':round(v,2),
                               'spd':round(spd,2),'dir':round(direction,1)})

        return result if result else None

    except Exception as e:
        log.error(f"Simple GRIB2 parse error: {e}")
        return None


def parse_single_grib2_message(buf):
    """Parse one GRIB2 message, extract grid and data."""
    try:
        pos = 16  # Skip section 0
        ni = nj = la1 = lo1 = la2 = lo2 = None
        discipline = category = parameter = 0
        ref_val = bin_scale = dec_scale = num_packed = bits_per_val = 0
        bitmap = None
        values = []

        while pos < len(buf) - 5:
            if buf[pos:pos+4] == b'7777': break
            if pos + 5 > len(buf): break

            sec_len = struct.unpack_from('>I', buf, pos)[0]
            sec_num = buf[pos+4]

            if sec_len < 5 or pos + sec_len > len(buf): break

            sec = buf[pos:pos+sec_len]

            if sec_num == 0:
                discipline = buf[6] if len(buf) > 6 else 0

            elif sec_num == 3:  # Grid definition
                if len(sec) >= 72:
                    ni = struct.unpack_from('>I', sec, 30)[0]
                    nj = struct.unpack_from('>I', sec, 34)[0]
                    la1 = struct.unpack_from('>i', sec, 46)[0] * 1e-6
                    lo1 = struct.unpack_from('>i', sec, 50)[0] * 1e-6
                    la2 = struct.unpack_from('>i', sec, 55)[0] * 1e-6
                    lo2 = struct.unpack_from('>i', sec, 59)[0] * 1e-6

            elif sec_num == 4:  # Product definition
                if len(sec) >= 10:
                    category  = sec[9]
                    parameter = sec[10] if len(sec) > 10 else 0

            elif sec_num == 5:  # Data representation
                if len(sec) >= 21:
                    # Template 0: simple packing
                    raw_ref = struct.unpack_from('>I', sec, 11)[0]
                    # IEEE 754 float
                    ref_val = struct.unpack_from('>f',
                        struct.pack('>I', raw_ref))[0]
                    bin_scale = struct.unpack_from('>h', sec, 15)[0]
                    dec_scale = struct.unpack_from('>h', sec, 17)[0]
                    bits_per_val = sec[19]
                    num_packed = struct.unpack_from('>I', sec, 5)[0]

            elif sec_num == 6:  # Bit map
                if len(sec) > 6 and sec[5] == 0:
                    bitmap_bytes = sec[6:]
                    bitmap = []
                    for byte in bitmap_bytes:
                        for bit in range(7, -1, -1):
                            bitmap.append((byte >> bit) & 1)

            elif sec_num == 7:  # Data section
                if bits_per_val > 0 and ni and nj:
                    raw_data = sec[5:]
                    n_total = ni * nj
                    # Unpack bits
                    packed_vals = []
                    bit_offset = 0
                    raw_bits = 0
                    bits_avail = 0
                    byte_pos = 0

                    while len(packed_vals) < n_total and byte_pos < len(raw_data):
                        while bits_avail < bits_per_val and byte_pos < len(raw_data):
                            raw_bits = (raw_bits << 8) | raw_data[byte_pos]
                            bits_avail += 8
                            byte_pos += 1
                        if bits_avail >= bits_per_val:
                            bits_avail -= bits_per_val
                            val = (raw_bits >> bits_avail) & ((1 << bits_per_val) - 1)
                            packed_vals.append(val)

                    # Apply scale factors: X = (R + (scaled_val * 2^E)) / 10^D
                    scale_2 = math.pow(2, bin_scale)
                    scale_10 = math.pow(10, -dec_scale)
                    bm_idx = 0
                    pv_idx = 0
                    for k in range(n_total):
                        if bitmap:
                            if bm_idx < len(bitmap) and bitmap[bm_idx] == 0:
                                values.append(None)
                                bm_idx += 1
                                continue
                            bm_idx += 1
                        if pv_idx < len(packed_vals):
                            v = (ref_val + packed_vals[pv_idx] * scale_2) * scale_10
                            values.append(v)
                            pv_idx += 1
                        else:
                            values.append(None)

            pos += sec_len

        if ni is None or nj is None or not values:
            return None

        # Build lat/lon arrays
        lat_step = (la2 - la1) / (nj - 1) if nj > 1 else 0
        lon_step = (lo2 - lo1) / (ni - 1) if ni > 1 else 0
        lats = []
        lons = []
        for j in range(nj):
            for i in range(ni):
                lats.append(round(la1 + j * lat_step, 3))
                lons.append(round(lo1 + i * lon_step, 3))

        return {
            'discipline': discipline,
            'category': category,
            'parameter': parameter,
            'ni': ni, 'nj': nj,
            'la1': la1, 'lo1': lo1, 'la2': la2, 'lo2': lo2,
            'lats': lats, 'lons': lons, 'values': values
        }

    except Exception as e:
        log.debug(f"Message parse error: {e}")
        return None


def get_forecast_hour(target_iso):
    """
    Given a target ISO datetime string, return the GFS forecast hour string.
    GFS cycle base time is the most recent 00/06/12/18z run.
    Returns '000'..'384' or None if beyond range.
    """
    date_str, cycle = get_latest_gfs_cycle()
    cycle_dt = datetime(
        int(date_str[:4]), int(date_str[4:6]), int(date_str[6:]),
        int(cycle), 0, 0, tzinfo=timezone.utc
    )
    target_dt = datetime.fromisoformat(target_iso.replace('Z','+00:00'))
    diff_hours = (target_dt - cycle_dt).total_seconds() / 3600

    if diff_hours < 0:
        # Target in past — use hour 0 (analysis)
        return '000'
    if diff_hours > 384:
        return None  # Beyond GFS range — caller should use climatology

    # Round to nearest available hour
    # GFS: hours 0-120 at 3h intervals, 120-384 at 12h intervals
    if diff_hours <= 120:
        fhr = round(diff_hours / 3) * 3
    else:
        fhr = round(diff_hours / 12) * 12

    return str(int(fhr)).zfill(3)


# ── WEATHER API ENDPOINTS ─────────────────────────────────────────────────────

@app.route("/api/weather/grid")
def weather_grid():
    """
    Returns GFS weather grid for the visible map area.
    Params:
      var: wind|wave|current
      fhr: forecast hour string (000-384), default 000
      south,north,west,east: bounding box (optional, returns global if absent)
    """
    var_type  = request.args.get('var', 'wind')
    fhr       = request.args.get('fhr', '000').zfill(3)
    target_iso = request.args.get('time', None)

    # If time provided, calculate forecast hour
    if target_iso:
        fhr_calc = get_forecast_hour(target_iso)
        if fhr_calc is not None:
            fhr = fhr_calc
        else:
            # Beyond forecast range — return climatological flag
            r = jsonify({'error':'beyond_forecast', 'message':'Beyond 16-day GFS range'})
            r.headers["Access-Control-Allow-Origin"] = "*"
            return r, 200

    if var_type not in ('wind', 'wave', 'current'):
        return jsonify({'error': 'var must be wind|wave|current'}), 400

    data, err = fetch_gfs_grid(var_type, fhr)

    if err:
        r = jsonify({'error': err, 'fallback': True})
        r.headers["Access-Control-Allow-Origin"] = "*"
        return r, 503

    # Apply bounding box filter if provided
    try:
        south = float(request.args.get('south', -80))
        north = float(request.args.get('north', 80))
        west  = float(request.args.get('west', -180))
        east  = float(request.args.get('east', 180))
        if south != -80 or north != 80:
            data = [p for p in data
                    if south <= p['lat'] <= north
                    and west <= p['lon'] <= east]
    except:
        pass

    # Add cycle info to response
    date_str, cycle = get_latest_gfs_cycle()
    resp = {
        'points': data,
        'count': len(data),
        'cycle': f"{date_str} {cycle}z",
        'fhr': fhr,
        'var': var_type
    }
    r = jsonify(resp)
    r.headers["Access-Control-Allow-Origin"] = "*"
    r.headers["Cache-Control"] = "public, max-age=1800"  # 30 min browser cache
    return r


@app.route("/api/weather/route")
def weather_route():
    """
    Returns weather at specified waypoints for given ETAs.
    Params:
      points: JSON array of {lat,lon,eta_iso} objects
      var: wind|wave|current
    """
    var_type = request.args.get('var', 'wind')

    try:
        points = json.loads(request.args.get('points', '[]'))
    except:
        return jsonify({'error': 'Invalid points JSON'}), 400

    if not points:
        return jsonify({'error': 'No points provided'}), 400

    # Group points by forecast hour to minimise fetches
    fhr_groups = {}
    for i, pt in enumerate(points):
        eta = pt.get('eta_iso', None)
        if eta:
            fhr = get_forecast_hour(eta)
            if fhr is None:
                fhr = 'clim'
        else:
            fhr = '000'

        if fhr not in fhr_groups:
            fhr_groups[fhr] = []
        fhr_groups[fhr].append((i, pt))

    results = [None] * len(points)
    date_str, cycle = get_latest_gfs_cycle()

    for fhr, group in fhr_groups.items():
        if fhr == 'clim':
            # Climatological fallback
            for i, pt in group:
                lat = pt.get('lat', 0)
                month = datetime.fromisoformat(
                    pt.get('eta_iso','').replace('Z','+00:00')
                ).month if pt.get('eta_iso') else datetime.now().month
                clim_spd = get_clim_wind(lat, month)
                results[i] = {
                    'lat': lat, 'lon': pt.get('lon',0),
                    'spd': clim_spd, 'dir': 0, 'u': 0, 'v': 0,
                    'wave': clim_spd * 0.25,
                    'isClim': True, 'fhr': 'clim'
                }
            continue

        grid_data, err = fetch_gfs_grid(var_type, fhr)
        if err or not grid_data:
            for i, pt in group:
                lat = pt.get('lat',0)
                clim_spd = get_clim_wind(lat, datetime.now().month)
                results[i] = {'lat':lat,'lon':pt.get('lon',0),
                              'spd':clim_spd,'dir':0,'u':0,'v':0,
                              'wave':clim_spd*0.25,'isClim':True,'fhr':fhr}
            continue

        # Build spatial index for fast nearest-neighbour lookup
        grid_index = {}
        for pt_g in grid_data:
            la_k = round(pt_g['lat'] / 1.0) * 1.0  # 1-degree bins
            lo_k = round(pt_g['lon'] / 1.0) * 1.0
            key = (la_k, lo_k)
            if key not in grid_index:
                grid_index[key] = pt_g

        for i, pt in group:
            lat = pt.get('lat', 0)
            lon = pt.get('lon', 0)
            # Find nearest grid point
            best = None
            best_dist = float('inf')
            for dla in [-1,0,1]:
                for dlo in [-1,0,1]:
                    la_k = round((lat+dla) / 1.0) * 1.0
                    lo_k = round((lon+dlo) / 1.0) * 1.0
                    gpt = grid_index.get((la_k, lo_k))
                    if gpt:
                        d = (gpt['lat']-lat)**2 + (gpt['lon']-lon)**2
                        if d < best_dist:
                            best_dist = d
                            best = gpt

            if best:
                results[i] = {**best, 'isClim': False, 'fhr': fhr}
            else:
                clim_spd = get_clim_wind(lat, datetime.now().month)
                results[i] = {'lat':lat,'lon':lon,'spd':clim_spd,
                              'dir':0,'u':0,'v':0,'wave':clim_spd*0.25,
                              'isClim':True,'fhr':fhr}

    r = jsonify({'results': results, 'cycle': f"{date_str} {cycle}z"})
    r.headers["Access-Control-Allow-Origin"] = "*"
    return r


def get_clim_wind(lat, month):
    """Monthly climatological wind speed (m/s) by latitude."""
    clim = [
        (65, 90,  [9,9,8,7,6,5,5,6,7,8,9,9]),
        (30, 65,  [8,8,7,6,6,6,6,6,7,7,8,8]),
        (10, 30,  [6,6,5,5,5,6,6,6,6,6,6,6]),
        (-10,10,  [4,4,4,4,5,5,5,5,5,4,4,4]),
        (-30,-10, [6,6,6,6,7,7,7,7,7,7,6,6]),
        (-60,-30, [8,8,8,8,9,9,9,9,9,9,8,8]),
        (-80,-60, [9,9,9,8,8,7,7,8,8,9,9,9]),
    ]
    m = max(0, min(11, month - 1))
    for mn, mx, vals in clim:
        if mn <= lat < mx:
            return vals[m]
    return 6.0


@app.route("/api/weather/status")
def weather_status():
    """Returns GFS cycle info and cache status."""
    date_str, cycle = get_latest_gfs_cycle()
    with _wx_lock:
        cached_keys = list(_wx_cache.keys())
    r = jsonify({
        'latest_cycle': f"{date_str} {cycle}z",
        'cached_grids': len(cached_keys),
        'cache_keys': cached_keys[:10]
    })
    r.headers["Access-Control-Allow-Origin"] = "*"
    return r


# ── EXISTING ROUTING ENDPOINTS ────────────────────────────────────────────────

@app.route("/api/status")
def status():
    r=jsonify({
        "status":  "ready" if ENGINE else "unavailable",
        "engine":  ENGINE or "none",
        "nodes":   len(GRAPH.graph) if ENGINE=='scgraph' else 0,
        "backend": "scgraph (MARNET)" if ENGINE=='scgraph' else
                   ("searoute-py" if ENGINE=='searoute' else "not loaded")
    })
    r.headers["Access-Control-Allow-Origin"]="*"; return r

@app.route("/api/route")
def route_api():
    if not ENGINE: return jsonify({"error":"Engine not loaded"}),503
    try:
        olon=float(request.args["olon"]); olat=float(request.args["olat"])
        dlon=float(request.args["dlon"]); dlat=float(request.args["dlat"])
    except:
        return jsonify({"error":"Required: olon,olat,dlon,dlat"}),400

    restrictions=[x.strip().lower() for x in
                  request.args.get("avoid","").split(",") if x.strip()]

    result = route_scgraph(olon,olat,dlon,dlat,restrictions) \
             if ENGINE=='scgraph' \
             else route_searoute(olon,olat,dlon,dlat,restrictions)

    r=jsonify(result)
    r.headers["Access-Control-Allow-Origin"]="*"
    return r,(400 if "error" in result else 200)

@app.route("/")
def index():
    return send_from_directory(Path(__file__).parent,"index.html")

@app.route("/<path:f>")
def static_f(f):
    return send_from_directory(Path(__file__).parent,f)

load_engine()

if __name__=="__main__":
    port=int(os.environ.get("PORT",5050))
    log.info(f"RoutePlannerPro — http://localhost:{port}")
    app.run(host="0.0.0.0",port=port,debug=False)
