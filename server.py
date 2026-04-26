#!/usr/bin/env python3
"""
RoutePlannerPro — Maritime Routing + Copernicus Marine Weather Proxy
Deploy to Render.com

Environment variables required:
  CMEMS_USER  — Copernicus Marine username (email)
  CMEMS_PASS  — Copernicus Marine password

pip install flask searoute gunicorn requests staticmap Pillow matplotlib scipy numpy
"""
import math, os, json, time, threading, base64, logging, io
from pathlib import Path
from flask import Flask, jsonify, request, send_from_directory, Response
import requests as req_lib

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder=".")
ENGINE = None
GRAPH  = None

# ── SUPABASE CONFIG (sea condition charts) ───────────────────────────────────
SUPABASE_URL  = os.environ.get('SUPABASE_URL',  '')
SUPABASE_ANON = os.environ.get('SUPABASE_ANON', '')

# ── MAPTILER CONFIG ───────────────────────────────────────────────────────────
MAPTILER_KEY = os.environ.get('MAPTILER_KEY', 'XaQgaXRnIoyC1qZKH1xl')

# ── COPERNICUS MARINE CONFIG ──────────────────────────────────────────────────
CMEMS_USER = os.environ.get('CMEMS_USER', '')
CMEMS_PASS = os.environ.get('CMEMS_PASS', '')
CMEMS_WMTS = 'https://wmts.marine.copernicus.eu/teroWmts'

_bearer_token     = None
_bearer_token_exp = 0
_token_lock       = threading.Lock()
CMEMS_TOKEN_URL   = 'https://iam.marine.copernicus.eu/realms/ocean/protocol/openid-connect/token'

def get_bearer_token():
    global _bearer_token, _bearer_token_exp
    with _token_lock:
        if _bearer_token and time.time() < _bearer_token_exp - 30:
            return _bearer_token
        if not CMEMS_USER or not CMEMS_PASS:
            return None
        try:
            r = req_lib.post(CMEMS_TOKEN_URL, data={
                'client_id': 'cmems-marine-public',
                'username':  CMEMS_USER,
                'password':  CMEMS_PASS,
                'grant_type':'password',
            }, timeout=10)
            if r.status_code == 200:
                d = r.json()
                _bearer_token     = d.get('access_token')
                _bearer_token_exp = time.time() + d.get('expires_in', 300)
                log.info('CMEMS bearer token OK, expires in %ds', d.get('expires_in', 300))
                return _bearer_token
        except Exception as e:
            log.debug('Bearer token fetch failed: %s', e)
        return None


def cmems_request(params):
    if not CMEMS_USER or not CMEMS_PASS:
        return None, 'CMEMS_USER/CMEMS_PASS not set in environment', 503
    headers = {'User-Agent': 'RoutePlannerPro/1.0', 'Accept': 'image/png,image/jpeg,application/json,*/*'}
    try:
        r = req_lib.get(CMEMS_WMTS, params=params, headers=headers, auth=(CMEMS_USER, CMEMS_PASS), timeout=30)
        log.debug('CMEMS Basic Auth response: %d', r.status_code)
        if r.status_code == 200:
            return r.content, r.headers.get('Content-Type','image/png'), 200
        if r.status_code == 401:
            log.warning('CMEMS Basic Auth 401 — trying Bearer token')
            token = get_bearer_token()
            if token:
                headers['Authorization'] = f'Bearer {token}'
                r2 = req_lib.get(CMEMS_WMTS, params=params, headers=headers, timeout=30)
                return r2.content, r2.headers.get('Content-Type','image/png'), r2.status_code
            return r.content, r.headers.get('Content-Type','text/plain'), 401
        return r.content, r.headers.get('Content-Type','text/plain'), r.status_code
    except req_lib.exceptions.Timeout:
        return b'', 'text/plain', 504
    except Exception as e:
        log.error('CMEMS request error: %s', e)
        return b'', 'text/plain', 503


def verify_cmems_auth():
    if not CMEMS_USER or not CMEMS_PASS:
        return False, 'none', 'CMEMS_USER or CMEMS_PASS not set'
    test_params = {'SERVICE':'WMTS','REQUEST':'GetCapabilities','VERSION':'1.0.0'}
    try:
        content, ctype, status = cmems_request(test_params)
        if status == 200:
            method = 'bearer' if _bearer_token else 'basic'
            return True, method, 'Authentication successful'
        elif status == 401:
            return False, 'failed', 'Authentication failed (401) — check CMEMS_USER/CMEMS_PASS'
        else:
            return False, 'error', f'CMEMS returned HTTP {status}'
    except Exception as e:
        return False, 'error', str(e)


def cmems_tile_proxy(path, params):
    content, ctype, status = cmems_request(params)
    return content, ctype, status


# ═══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
# TRAFFIC SEPARATION SCHEMES (TSS)
# Source: IMO Ships Routeing (official PDF) + visual verification via OpenSeaMap
#
# Lane waypoints: centreline coordinates verified against OpenSeaMap TSS overlays.
# Each lane covers only the TSS extent — not extended approach routes.
# Exit logic: inject_tss_waypoints clips lane to closest WP to destination.
#
# PROXIMITY-BASED DETECTION:
#   reference  : [lon, lat] strait midpoint
#   trigger_nm : inject if route passes within this many nm of reference
# ═══════════════════════════════════════════════════════════════════════════════

TSS_ZONES = {

    # ── Dover Strait ──────────────────────────────────────────────────────────
    # Extent: ~0E (English Channel) to ~2.2E (North Sea entry)
    # Points beyond 2.2E are North Sea/Rotterdam approaches — NOT Dover TSS
    'dover': {
        'name':       'Dover Strait TSS',
        'reference':  [1.20, 51.00],
        'trigger_nm': 40,
        # NE-bound (Atlantic->North Sea, SW->NE)
        'northeast': [
            [ 0.0440, 50.3275],
            [ 0.9833, 50.4974],
            [ 1.3348, 50.6876],
            [ 1.4502, 50.9134],
            [ 1.7386, 51.1571],
            [ 2.0132, 51.3240],
        ],
        # SW-bound (North Sea->Atlantic, NE->SW)
        'southwest': [
            [ 1.9308, 51.4435],
            [ 1.7770, 51.2392],
            [ 1.3815, 50.9925],
            [ 0.9860, 50.7861],
            [ 0.5768, 50.6074],
            [-0.0577, 50.4887],
        ],
    },

    # ── Strait of Gibraltar ───────────────────────────────────────────────────
    'gibraltar': {
        'name':       'Strait of Gibraltar TSS',
        'reference':  [-5.00, 36.00],  # moved E to widen removal zone
        'trigger_nm': 50,              # removal_nm = 30nm, catches [-4.700,36.000]
        # Eastbound (Atlantic->Med, W->E) — verified via OpenSeaMap
        'east': [
            [-6.9351, 35.8735],
            [-5.6552, 35.9068],
            [-4.8395, 36.0757],
        ],
        # Westbound (Med->Atlantic, E->W) — verified via OpenSeaMap
        'west': [
            [-4.4192, 36.1977],
            [-5.6168, 35.9580],
            [-6.8967, 35.9535],
        ],
    },

    # ── Off Casquets ──────────────────────────────────────────────────────────
    # English Channel approaches W of Cherbourg — verified via OpenSeaMap
    'casquets': {
        'name':       'Off Casquets TSS',
        'reference':  [-2.651, 49.950],
        'trigger_nm': 25,
        # NE-bound (toward Dover, W->E)
        'northeast': [
            [-3.1998, 49.7617],
            [-2.6010, 49.8681],
            [-1.9061, 49.9830],
        ],
        # SW-bound (toward Atlantic, E->W)
        'southwest': [
            [-1.9336, 50.1452],
            [-2.6724, 50.0289],
            [-3.3728, 49.9123],
        ],
    },

    # ── Off Cape S. Vicente ───────────────────────────────────────────────────
    # SW Portugal — verified via OpenSeaMap
    'cape_st_vicente': {
        'name':       'Off Cape S. Vicente TSS',
        'reference':  [-9.073, 36.923],
        'trigger_nm': 25,
        # Northbound (S->N)
        'north': [
            [-8.7671, 36.5869],
            [-9.2107, 36.6794],
            [-9.3933, 36.8412],
            [-9.4798, 37.1428],
        ],
        # Southbound (N->S)
        'south': [
            [-9.6735, 37.2281],
            [-9.5499, 36.7807],
            [-9.3041, 36.5736],
            [-8.7753, 36.4533],
        ],
    },

    # ── Bab el Mandeb ─────────────────────────────────────────────────────────
    'babelmandab': {
        'name':       'Bab-el-Mandeb TSS',
        'reference':  [43.35, 12.70],
        'trigger_nm': 25,
        # Northbound (Gulf of Aden->Red Sea, S->N)
        'north': [
            [43.4983, 12.5565],
            [43.3637, 12.6309],
            [43.3081, 12.7414],
            [43.1502, 13.0721],
            [43.0643, 13.2427],
        ],
        # Southbound (Red Sea->Gulf of Aden, N->S)
        'south': [
            [43.0328, 13.2119],
            [43.1059, 13.0684],
            [43.2679, 12.7238],
            [43.3273, 12.6075],
            [43.4629, 12.5290],
        ],
    },

    # ── Strait of Hormuz ──────────────────────────────────────────────────────
    # Trimmed to strait only (~56.3-56.7E) — western outliers removed
    'hormuz': {
        'name':       'Strait of Hormuz TSS',
        'reference':  [56.50, 26.55],
        'trigger_nm': 25,
        # SE-bound (out of Persian Gulf, NW->SE)
        'southeast': [
            [56.3873, 26.5218],
            [56.4725, 26.5587],
            [56.5494, 26.5538],
            [56.6071, 26.4677],
        ],
        # NW-bound (into Persian Gulf, SE->NW)
        'northwest': [
            [56.6730, 26.4973],
            [56.6016, 26.6177],
            [56.4752, 26.6275],
            [56.3516, 26.6030],
        ],
    },

    # ── Singapore Strait ──────────────────────────────────────────────────────
    'singapore': {
        'name':       'Singapore Strait TSS',
        'reference':  [103.90, 1.20],
        'trigger_nm': 30,
        # Eastbound (W->E)
        'east': [
            [103.5461,  1.1066],
            [103.6505,  1.0516],
            [103.7521,  1.1244],
            [103.8043,  1.1588],
            [103.9059,  1.2082],
            [103.9925,  1.2385],
            [104.0680,  1.2522],
            [104.2328,  1.2673],
            [104.2918,  1.2756],
            [104.3372,  1.2948],
            [104.3990,  1.3470],
            [104.4731,  1.4013],
        ],
        # Westbound (E->W)
        'west': [
            [104.4381,  1.4163],
            [104.3221,  1.3105],
            [104.2493,  1.3009],
            [104.0515,  1.2672],
            [103.9760,  1.2523],
            [103.9005,  1.2179],
            [103.8706,  1.2063],
            [103.8380,  1.1902],
            [103.8167,  1.1826],
            [103.7302,  1.1373],
            [103.6677,  1.1819],
            [103.5932,  1.1943],
        ],
    },

    # ── Strait of Malacca ─────────────────────────────────────────────────────
    # Western points (lon 100.7-101.5) confirmed in water west of Malay Peninsula
    'malacca': {
        'name':       'Strait of Malacca TSS',
        'reference':  [101.35, 2.50],
        'trigger_nm': 55,
        # NW-bound (Singapore->Andaman, SE->NW)
        'northwest': [
            [103.4953, 1.2178],
            [103.4129, 1.2391],
            [103.2152, 1.4177],
            [102.9295, 1.6230],
            [102.6769, 1.7549],
            [102.4166, 1.8734],
            [102.2649, 1.9517],
            [102.0760, 2.1121],
            [101.6798, 2.4243],
            [101.4532, 2.6056],
            [101.2122, 2.7319],
            [101.0179, 2.8373],
            [100.7865, 3.0384],
        ],
        # SE-bound (Andaman->Singapore, NW->SE)
        'southeast': [
            [100.7481, 2.9705],
            [100.9142, 2.8476],
            [100.9726, 2.7976],
            [101.1806, 2.6967],
            [101.4182, 2.5669],
            [101.6421, 2.3990],
            [102.0053, 2.1059],
            [102.2182, 1.9055],
            [102.7840, 1.6323],
            [103.1891, 1.3742],
            [103.3745, 1.2137],
            [103.4981, 1.1422],
        ],
    },

    # ── Off Ushant (Ouessant) ─────────────────────────────────────────────────
    'ushant': {
        'name':       'Off Ushant (Ouessant) TSS',
        'reference':  [-5.65, 48.75],
        'trigger_nm': 35,
        # NE-bound (toward Channel, SW->NE)
        'northeast': [
            [-5.7651, 48.5901],
            [-5.6223, 48.7616],
            [-5.4066, 48.8503],
        ],
        # SW-bound (toward Atlantic, NE->SW)
        'southwest': [
            [-5.5563, 48.9893],
            [-5.7884, 48.8973],
            [-5.9766, 48.6746],
        ],
    },

    # ── Off Cape Finisterre ───────────────────────────────────────────────────
    'finisterre': {
        'name':       'Off Finisterre TSS',
        'reference':  [-9.95, 43.20],
        'trigger_nm': 35,
        # Northbound (S->N)
        'north': [
            [-9.8060, 42.8820],
            [-9.8060, 43.1980],
            [-9.6790, 43.3770],
        ],
        # Southbound (N->S)
        'south': [
            [-9.9591, 43.4927],
            [-10.0909, 43.2890],
            [-10.1074, 42.8895],
        ],
    },

    # ── German Bight ──────────────────────────────────────────────────────────
    # Trimmed to actual German Bight TSS extent (~7.5-8.3E)
    'german_bight': {
        'name':       'German Bight TSS',
        'reference':  [8.00, 53.98],
        'trigger_nm': 30,
        # NE-bound
        'northeast': [
            [7.7920, 53.9418],
            [8.0722, 53.9790],
            [8.2562, 53.9887],
        ],
        # SW-bound
        'southwest': [
            [8.2370, 54.0145],
            [7.7481, 54.0291],
            [7.5092, 53.9968],
        ],
    },

    # ── Red Sea N of Bab el Mandeb ────────────────────────────────────────────
    # Verified via OpenSeaMap. TSS just north of Bab el Mandeb exit.
    'bab_extension': {
        'name':       'Red Sea N of Bab el Mandeb TSS',
        'reference':  [42.630, 13.550],
        'trigger_nm': 20,
        # Northbound (S->N, into Red Sea)
        'north': [
            [42.7639, 13.4873],
            [42.6637, 13.5544],
            [42.5854, 13.7009],
        ],
        # Southbound (N->S, into Gulf of Aden)
        'south': [
            [42.4968, 13.6692],
            [42.5799, 13.5104],
            [42.7291, 13.4022],
        ],
    },

    # ── Off Cape Roca (Portugal) ──────────────────────────────────────────────
    # Verified via OpenSeaMap.
    'cape_roca': {
        'name':       'Off Cape Roca TSS',
        'reference':  [-9.985, 38.747],
        'trigger_nm': 25,
        # Northbound (S->N)
        'north': [
            [-9.8142, 38.5958],
            [-9.8643, 38.7164],
            [-9.8568, 38.8977],
        ],
        # Southbound (N->S)
        'south': [
            [-10.1164, 38.8972],
            [-10.1129, 38.7223],
            [-10.0628, 38.5673],
        ],
    },

    # ── Gulf of Suez ──────────────────────────────────────────────────────────
    # Verified via OpenSeaMap. Decimated from 94/113 pts to 17/20 using RDP.
    'gulf_of_suez': {
        'name':       'Gulf of Suez TSS',
        'reference':  [32.493, 30.315],
        'trigger_nm': 45,
        # Northbound (S->N, toward Suez Canal): E lane
        'north': [
            [34.1553, 27.5255],
            [33.9280, 27.6788],
            [33.3552, 28.1725],
            [33.0513, 28.6094],
            [32.5760, 29.5783],
            [32.5453, 29.9044],
            [32.5863, 29.9685],
            [32.5663, 30.2006],
            [32.3736, 30.3617],
            [32.3042, 30.5659],
            [32.3439, 30.7079],
            [32.3091, 31.0964],
            [32.4117, 31.4587],
            [32.2293, 31.5402],
            [31.9833, 31.7470],
            [31.7958, 31.6425],
            [31.6798, 31.7852],
        ],
        # Southbound (N->S, out of Suez): W lane
        'south': [
            [31.6633, 31.7728],
            [31.7877, 31.6347],
            [31.9714, 31.7340],
            [32.2888, 31.4464],
            [32.3571, 31.4180],
            [32.3676, 31.3219],
            [32.3050, 31.2440],
            [32.3151, 30.7922],
            [32.3436, 30.7078],
            [32.3036, 30.5670],
            [32.3749, 30.3518],
            [32.5666, 30.1975],
            [32.5861, 29.9764],
            [32.5449, 29.9103],
            [32.5175, 29.6251],
            [32.7211, 29.1586],
            [32.8749, 28.8175],
            [33.3175, 28.1465],
            [33.6786, 27.8653],
            [34.0906, 27.4807],
        ],
    },
}






# ── TSS helper: distance from point to segment (nautical miles) ───────────────
def _pt_to_seg_nm(plon, plat, lon1, lat1, lon2, lat2):
    """
    Minimum distance (nm) from point (plon,plat) to segment (lon1,lat1)-(lon2,lat2).
    Uses simple planar approximation — accurate enough for TSS detection (< 100nm).
    """
    dx = lon2 - lon1
    dy = lat2 - lat1
    if dx == 0 and dy == 0:
        return haversine_km(plon, plat, lon1, lat1) / 1.852
    t = max(0.0, min(1.0, ((plon - lon1) * dx + (plat - lat1) * dy) / (dx*dx + dy*dy)))
    cx = lon1 + t * dx
    cy = lat1 + t * dy
    return haversine_km(plon, plat, cx, cy) / 1.852


def bearing_deg(lon1, lat1, lon2, lat2):
    """Initial bearing from point 1 to point 2, degrees true."""
    r = math.pi / 180
    dlon = (lon2 - lon1) * r
    lat1r, lat2r = lat1 * r, lat2 * r
    x = math.sin(dlon) * math.cos(lat2r)
    y = math.cos(lat1r) * math.sin(lat2r) - math.sin(lat1r) * math.cos(lat2r) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _select_tss_lane(tss, seg_bearing, overall_bearing, approach_coord=None):
    """
    Select the correct TSS lane based on vessel bearing.
    Lane keys are cardinal/ordinal direction names matching the vessel's heading.

    Strategy (in priority order):
    1. If approach_coord is given, use the vessel's position RELATIVE to the
       TSS reference to determine passage direction for N/S and E/W straits.
       This is the most reliable signal and works regardless of MARNET density.
    2. Try seg_bearing (bearing from approach coord toward reference) against
       direction ranges with a ±20° tolerance buffer.
    3. Fallback: pick lane whose centre is closest to seg_bearing.
       (NOT overall_bearing — that's too unreliable for transits.)
    """
    keys = [k for k in tss.keys() if k not in ('name', 'reference', 'trigger_nm',
                                                 'trigger_box')]
    if not keys:
        return None

    # ── Strategy 1: relative position ────────────────────────────────────────
    # For N/S straits (north/south lanes), the vessel's latitude relative to
    # the reference tells us which direction it's traversing the strait:
    #   approach_coord lat > ref lat  → approaching from north → heading south
    #   approach_coord lat < ref lat  → approaching from south → heading north
    # For E/W straits (east/west lanes), use longitude similarly.
    if approach_coord is not None:
        ref_lon, ref_lat = tss['reference']
        ap_lon, ap_lat   = approach_coord

        has_ns = any(k in keys for k in ('north', 'south'))
        has_ew = any(k in keys for k in ('east', 'west'))

        if has_ns and not has_ew:
            # Pure N/S strait: latitude decides direction
            if ap_lat > ref_lat + 0.3 and 'south' in keys:
                return tss['south']  # approaching from north → heading south
            if ap_lat < ref_lat - 0.3 and 'north' in keys:
                return tss['north']  # approaching from south → heading north

        if has_ew and not has_ns:
            # Pure E/W strait: longitude decides direction
            if ap_lon < ref_lon - 0.3 and 'east' in keys:
                return tss['east']   # approaching from west → heading east
            if ap_lon > ref_lon + 0.3 and 'west' in keys:
                return tss['west']   # approaching from east → heading west

    # ── Strategy 2: bearing range match with ±20° tolerance ──────────────────
    direction_map = {
        'northeast': (22.5,  112.5),
        'east':      (67.5,  112.5),
        'southeast': (112.5, 202.5),
        'south':     (157.5, 202.5),
        'southwest': (202.5, 292.5),
        'west':      (247.5, 292.5),
        'northwest': (292.5, 382.5),
        'north':     (337.5, 382.5),
    }
    TOLERANCE = 20.0  # degrees — widens each range to catch near-misses

    def matches(brg, rmin, rmax):
        b = brg % 360
        lo, hi = rmin - TOLERANCE, rmax + TOLERANCE
        if hi > 360:
            return b >= lo or b < (hi - 360)
        return lo <= b < hi

    # Try seg_bearing first, then overall_bearing
    for use_bearing in [seg_bearing, overall_bearing]:
        for key in keys:
            if key in direction_map and matches(use_bearing, *direction_map[key]):
                return tss[key]

    # ── Strategy 3: closest centre to seg_bearing ────────────────────────────
    def centre(key):
        if key not in direction_map:
            return 0
        rmin, rmax = direction_map[key]
        return ((rmin + rmax) / 2) % 360

    def angle_diff(a, b):
        return abs((a - b + 180) % 360 - 180)

    best_key = min(keys, key=lambda k: angle_diff(seg_bearing, centre(k)))
    return tss[best_key]


def inject_tss_waypoints(coords):
    """
    TSS injection — processes ONE TSS zone at a time, in route order.

    For each TSS zone the route passes through:

      1. DETECT  — does the route pass within trigger_nm of the TSS reference?
                   If not, skip this zone entirely.

      2. LANE    — which lane does the vessel use?
                   Primary: latitude/longitude of approach coord relative to ref
                   (north of ref → heading south → south lane, etc.)
                   Fallback: bearing ranges with ±20° tolerance.

      3. ENTRY   — find the MARNET coord whose segment is closest to the lane
                   entry WP (lane_wps[0]). Everything before that coord in
                   the MARNET is kept as-is. The lane starts here.

      4. EXIT    — find the MARNET coord whose segment is closest to the lane
                   exit WP (lane_wps[-1]). If the destination is inside the TSS,
                   clip the lane at the WP nearest to the destination.
                   The MARNET resumes after the exit coord.

      5. SPLICE  — replace the MARNET section from entry..exit with the lane WPs.

    This approach is direction-agnostic: it never tries to infer direction from
    large-scale bearings, never removes coords outside the immediate TSS region,
    and handles origins/destinations inside TSS zones naturally.
    """
    if len(coords) < 2:
        return coords, []

    dest_lon, dest_lat = coords[-1]
    overall_bearing    = bearing_deg(coords[0][0], coords[0][1], dest_lon, dest_lat)
    tss_applied        = []
    splices            = []

    for tss_key, tss in TSS_ZONES.items():
        ref_lon, ref_lat = tss['reference']
        trigger_nm       = tss['trigger_nm']

        # ── Step 1: DETECT ────────────────────────────────────────────────────
        # Find the closest segment of the MARNET to the TSS reference point.
        min_dist_nm = float('inf')
        closest_seg = -1
        for i in range(len(coords) - 1):
            d = _pt_to_seg_nm(ref_lon, ref_lat,
                              coords[i][0], coords[i][1],
                              coords[i+1][0], coords[i+1][1])
            if d < min_dist_nm:
                min_dist_nm = d
                closest_seg = i

        if min_dist_nm > trigger_nm:
            continue   # route doesn't pass near this TSS — skip

        # ── Step 2: LANE ──────────────────────────────────────────────────────
        # Find the last MARNET coord that is OUTSIDE the trigger zone, walking
        # backward from closest_seg. That coord is on the approach side.
        pre_trigger_idx = None
        for i in range(closest_seg, -1, -1):
            if haversine_km(coords[i][0], coords[i][1], ref_lon, ref_lat) / 1.852 > trigger_nm:
                pre_trigger_idx = i
                break

        if pre_trigger_idx is not None:
            approach_coord   = (coords[pre_trigger_idx][0], coords[pre_trigger_idx][1])
            approach_bearing = bearing_deg(approach_coord[0], approach_coord[1],
                                           ref_lon, ref_lat)
        else:
            # Origin is inside the trigger zone — use the start of the closest seg
            approach_coord   = (coords[closest_seg][0], coords[closest_seg][1])
            approach_bearing = bearing_deg(coords[closest_seg][0], coords[closest_seg][1],
                                           coords[closest_seg+1][0], coords[closest_seg+1][1])

        lane_wps = list(_select_tss_lane(tss, approach_bearing, overall_bearing,
                                          approach_coord=approach_coord) or [])
        if len(lane_wps) < 2:
            continue

        lane_entry = lane_wps[0]   # first WP of the correct lane
        lane_exit  = lane_wps[-1]  # last WP of the correct lane

        # ── Step 3: ENTRY ─────────────────────────────────────────────────────
        # Find which MARNET index is closest to the lane entry WP.
        # Everything before that index is kept; the lane starts there.
        entry_cut = min(range(len(coords)),
                        key=lambda i: haversine_km(coords[i][0], coords[i][1],
                                                   lane_entry[0], lane_entry[1]))

        # ── Step 4: EXIT ──────────────────────────────────────────────────────
        # Find which MARNET index is closest to the lane exit WP.
        # The MARNET resumes from after that index.
        exit_cut = min(range(len(coords)),
                       key=lambda i: haversine_km(coords[i][0], coords[i][1],
                                                  lane_exit[0], lane_exit[1]))

        # Safety: entry must come before exit in the route
        if entry_cut >= exit_cut:
            # Treat the whole closest_seg region as the splice point
            entry_cut = closest_seg
            exit_cut  = min(closest_seg + 1, len(coords) - 1)

        # Clip lane at the destination if it is inside the TSS.
        # Find the lane WP nearest to the destination; truncate there.
        exit_lane_idx = len(lane_wps) - 1
        dist_dest_to_exit = haversine_km(lane_exit[0], lane_exit[1],
                                          dest_lon, dest_lat) / 1.852
        if dist_dest_to_exit < trigger_nm:
            # Destination is inside or near the TSS — clip at the nearest lane WP
            nearest_idx = min(range(len(lane_wps)),
                              key=lambda j: haversine_km(lane_wps[j][0], lane_wps[j][1],
                                                          dest_lon, dest_lat))
            exit_lane_idx = nearest_idx

        clipped_lane = lane_wps[:exit_lane_idx + 1]

        splices.append((entry_cut, exit_cut, clipped_lane, tss['name'], min_dist_nm))
        log.info('TSS: %s dist=%.1fnm entry[%d] exit[%d] lane=%d',
                 tss['name'], min_dist_nm, entry_cut, exit_cut, len(clipped_lane))

    if not splices:
        return coords, []

    # ── Step 5: SPLICE ────────────────────────────────────────────────────────
    # Sort by entry_cut so we process zones in route order.
    # Build the result by walking the MARNET and substituting each TSS section
    # with the corresponding lane WPs.
    splices.sort(key=lambda s: s[0])

    result   = []
    prev_idx = 0

    for entry_cut, exit_cut, lane_wps, tss_name, dist_nm in splices:
        # Skip if this splice has already been consumed by a previous one
        if entry_cut < prev_idx:
            entry_cut = prev_idx
        if exit_cut < prev_idx:
            continue

        # Append MARNET coords from where we left off up to (not including) entry_cut
        for k in range(prev_idx, entry_cut):
            result.append([coords[k][0], coords[k][1]])

        # Append the lane WPs
        for wp in lane_wps:
            result.append([wp[0], wp[1]])

        tss_applied.append(tss_name)
        prev_idx = exit_cut + 1   # resume MARNET AFTER the exit coord

    # Append remaining MARNET after the last TSS
    for k in range(prev_idx, len(coords)):
        result.append([coords[k][0], coords[k][1]])

    # Deduplicate consecutive near-identical points (< 0.5nm apart)
    deduped = [result[0]]
    for pt in result[1:]:
        if haversine_km(deduped[-1][0], deduped[-1][1], pt[0], pt[1]) > 0.5 * 1.852:
            deduped.append(pt)

    return deduped, tss_applied


# ── ROUTING ENGINE ────────────────────────────────────────────────────────────
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
        log.info('Engine: scgraph — %d nodes', len(GRAPH.graph)); return True
    except: pass
    try:
        import searoute as sr
        GRAPH=sr; ENGINE='searoute'
        log.info('Engine: searoute-py'); return True
    except: pass
    log.error('No routing engine. pip install searoute'); return False

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
        passages=detect_passages(coords)

        # Apply TSS compliance — proximity-based, no trigger boxes
        coords, tss_applied = inject_tss_waypoints(coords)
        total_km = sum(haversine_km(coords[i][0],coords[i][1],coords[i+1][0],coords[i+1][1])
                       for i in range(len(coords)-1))

        return {"coordinates":coords,"distance_km":round(total_km,1),
                "distance_nm":round(total_km/1.852,1),
                "route_name":name_from_passages(passages),
                "passages":passages,"node_count":len(coords),
                "tss_applied":tss_applied,"warning":None}
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

    # Apply TSS compliance — proximity-based, no trigger boxes
    coords, tss_applied = inject_tss_waypoints(coords)
    total_km = sum(haversine_km(coords[i][0],coords[i][1],coords[i+1][0],coords[i+1][1])
                   for i in range(len(coords)-1))

    return {"coordinates":coords,"distance_km":round(total_km,1),
            "distance_nm":round(total_km/1.852,1),
            "route_name":name_from_passages(passages),
            "passages":passages,"node_count":len(coords),
            "tss_applied":tss_applied,"warning":None}


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTE MAP — SVG WITH EMBEDDED WORLD COASTLINE
# Pure Python, zero external dependencies beyond stdlib.
# Renders a dark-themed SVG world map with Mercator projection, route overlay,
# and origin/destination markers. Works for all routes including Pacific crossings.
#
# Coastline data: simplified world coastlines (32 polylines) encoded as
# compressed binary. Decoded on first use and cached in memory.
# ═══════════════════════════════════════════════════════════════════════════════

import gzip, struct

# ── Optional scientific rendering (sea condition charts) ─────────────────────
try:
    import numpy as np
    import matplotlib
    matplotlib.use('Agg')  # non-interactive backend — must be set before pyplot import
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    import matplotlib.contour as mcontour
    from scipy.ndimage import gaussian_filter
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False
    log.warning("scipy/matplotlib not installed — sea condition charts unavailable")

# ── Embedded coastline data ───────────────────────────────────────────────────
# Simplified world coastlines — 32 polylines, scale factor 10
# Source: manually digitised from Natural Earth 110m, major landmasses only
COASTLINE_B64 = (
    'H4sIAJyO3GkC/4WWP2hqVxjAv3O8V+xr+hryfNSCgxQpQi1IeZRQwsPB4VEcMjg4hJLBwSGUN4Ti'
    '4JAhQygODhkyODg4ZJDi4OAQSigODg4ZQnFwcMiQwcHBIXnec05/99a2FApFfvd8/7/vnnNvciUj'
    'nz+/1+nnc+jrL5/v9XcfdnXpw76ufjiGCxigz/SXG1HrTU4tYLQ5UM3NofphU1OVzY+qCKnNUFab'
    'uUw2KxkGdekGbfhNaoGVQ/kqvJo9tCn8Ij8GPWlFEU2iv5dfg29lEnyrPMgHTfVF0Gb9BaYqFSzk'
    'Q7BQL82e+sGkVduUVM+cqN/MT+p3c62m5kyNIv0a/UEtzJ1amwedMDc6bS51wZzpkqnqqinoE5PQ'
    'J8FC/xSM9GXQ1NdBUd8EKX3H+hAsYjumFHsjL+07XbVH+md7rq9dVt+4sn5y+7GMvY+9tZnYW3MT'
    'y5gzoi/1g7nTNzajL8Mc+TS8T+ZNBlO3DBZuHKxdxyTcqSm4sim5rCnZe1OwVyZtjyBjCubJVM2D'
    'OTF3shdezSUWKpon27cZ+2jfuaw9cqf23HXslRvbvpvZcxG7I0nzJDlzLQfmkt09kZopSCPa79eu'
    'o+puoJrQQx6xTt1MLdxSLYSTlKROSE6n5UAX5FCXWKvoJ/INHOozaelL6eprGeobmesbxR7J6//I'
    'LEgNGtCCLpWGVJqwztFXMI/kKvKJ8vQZlc5UHjmlq+hVeenGas1sa2Zbi1BddNotdcHNdMmNYYA8'
    'wDbWCWyJKJYcefV/E9MnXK/pfYntbDtDFVuJ9d/zfUbNOjM0JanacqBGcgg1aEBL9aQLQ3wTYuaw'
    'Uk3lqbZKQR65qOqqAnXk8NeO5Mi2rT5TFToU2cc8XYp0qdClTpc6XSp0KdKlSJc8XfJ0ydMlT5dU'
    '9MtDESrRmmeNespeVH1A9QuVcqcyd8fShQZyzXXkwA0kB0k3cEsYuw6/C3cKZa5Zd2wf4V5eRffb'
    'Y6Ie07XDvQj3JKrfof4plCGrKjZQRXuv8ravUuDZW1lRYW77MoQu8kfRNdRCK94wKooOs8LslA0k'
    'HvWcwjqihnwYziCfcXfhzrLD7EO4++FJbCN5Bruc3+Tv5ys6U0gjJ9i7NTkLcqfUGEEP/jkpToAn'
    '588nI3xi7qj0QMUnacR2pBbLwBvkjLTQu6xD1gn+OXEr4lfkrbZP1yqsJZ/EuNvYI9GBNLxdqXlZ'
    '2Efely7yENsQX5eYFrEhQ0mgHWDN4U3CCy8uL9CT2HOhj7q3SPeRlsQj3q5belk3gzHyOBa4WezR'
    'LYkRYpNhvHwcefeh7AbeMZzCxXYN9fLWTxzR+3QvwzGTXEAnIoksZCzJWOKXME6+xpKl1r7XcWVv'
    'DDN37C3dqZ90F34ODqGG3nDHfsuV/a7b94cu60/crj+HlQ3ing1YH/25vYdb5D5cIZ/7ExhCF1rY'
    'GlDDfwgHxOYgCWJvvSXMgL+OXgcu4BSOsfFkIwfhvLIXH9l+fGqv4gt7Hl9HvEc+wnaE7x28ifeg'
    'HfEOObS/h3PkK+iHNWTX5/31c6ri8w77vMM+77DfUE2/pdo+fylghDzC1sPXJqZJbD3KCd/9WFQh'
    'iYZFdryBzL2xrLyZ8ryxSnkDlWctQgVb3VuSK/BXxiecxYSzmnu875zRirNaUWVFhke2F+kXkT+M'
    'm4Tx8mI715S5Fsy1YK4pc02pOYIe8ii0yUfxik7Eizodz+sClJCr2KrxOnIFO3559fSkE887fD9k'
    'dOH5jS4983/w+QjeI5/DFXIfbuEe/RECvinC74zs9luj/Of3hrzYVktQKUGV9N9fJ1fRF0qBKgWq'
    'FP4AmrVxKroIAAA='
)
COASTLINE_SCALE = 10

_COASTLINE_CACHE = None

def _load_coastlines():
    """Decode and cache coastline polylines. Called once on first map render."""
    global _COASTLINE_CACHE
    if _COASTLINE_CACHE is not None:
        return _COASTLINE_CACHE
    raw = gzip.decompress(base64.b64decode(''.join(COASTLINE_B64.split())))
    pos = 0
    n_lines = struct.unpack_from('>H', raw, pos)[0]; pos += 2
    lines = []
    for _ in range(n_lines):
        n_pts = struct.unpack_from('>H', raw, pos)[0]; pos += 2
        pts = []
        for _ in range(n_pts):
            lo, la = struct.unpack_from('>hh', raw, pos); pos += 4
            pts.append((lo / COASTLINE_SCALE, la / COASTLINE_SCALE))
        lines.append(pts)
    _COASTLINE_CACHE = lines
    log.info('Coastlines loaded: %d polylines', len(lines))
    return lines


def generate_route_png(waypoints, width=1200, height=500):
    """
    Generate a professional PNG route chart by stitching real map tiles.
    Uses Esri Ocean Basemap (free, no API key) — matching the maritime chart
    style expected. Falls back to CartoDB Positron then OSM if unavailable.
    Overlays route with glow, waypoint dots, and port markers via PIL.
    Handles antimeridian (Pacific) crossings. Returns PNG bytes.
    """
    import io, math, concurrent.futures
    import requests as req_lib
    from PIL import Image, ImageDraw

    TILE_PROVIDERS = [
        'https://services.arcgisonline.com/arcgis/rest/services/Ocean/World_Ocean_Base/MapServer/tile/{z}/{y}/{x}',
        'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
        'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
    ]
    TILE_SIZE    = 256
    TILE_TIMEOUT = 8
    UA = 'RoutePlannerPro/2.4 (maritime voyage planning)'

    def lon_to_tx(lon, z): return (lon + 180) / 360 * (2 ** z)
    def lat_to_ty(lat, z):
        lat_r = math.radians(max(-85.05, min(85.05, lat)))
        return (1 - math.log(math.tan(lat_r) + 1/math.cos(lat_r)) / math.pi) / 2 * (2**z)
    def tile_nw_lat(ty, z):
        n = math.pi - 2*math.pi*ty/(2**z)
        return math.degrees(math.atan(math.sinh(n)))

    route_lons = [p[0] for p in waypoints]
    route_lats = [p[1] for p in waypoints]
    has_pos = any(lo > 90 for lo in route_lons)
    has_neg = any(lo < -90 for lo in route_lons)
    cross_anti = has_pos and has_neg
    route_lons_r = [lo + 360 if (cross_anti and lo < 0) else lo for lo in route_lons]

    lon_min = min(route_lons_r); lon_max = max(route_lons_r)
    lat_min = min(route_lats);   lat_max = max(route_lats)
    pad_lon = max((lon_max - lon_min) * 0.15, 3.0)
    pad_lat = max((lat_max - lat_min) * 0.20, 2.5)
    vlon_min = lon_min - pad_lon; vlon_max = lon_max + pad_lon
    vlat_min = max(lat_min - pad_lat, -85.0)
    vlat_max = min(lat_max + pad_lat,  85.0)

    # Pick zoom so canvas is ~1200px wide
    z = 3
    for zoom in range(2, 8):
        z = zoom
        if (lon_to_tx(vlon_max, zoom) - lon_to_tx(vlon_min, zoom)) * TILE_SIZE >= 900:
            break

    n_tiles = 2 ** z
    tx_min = int(math.floor(lon_to_tx(vlon_min, z)))
    tx_max = int(math.floor(lon_to_tx(vlon_max, z)))
    ty_min = int(math.floor(lat_to_ty(vlat_max, z)))
    ty_max = int(math.floor(lat_to_ty(vlat_min, z)))
    tiles_x = tx_max - tx_min + 1
    tiles_y = ty_max - ty_min + 1
    canvas_w = tiles_x * TILE_SIZE
    canvas_h = tiles_y * TILE_SIZE

    def fetch_tile(args):
        tx, ty, url_tpl = args
        url = url_tpl.format(z=z, x=tx % n_tiles, y=ty)
        try:
            r = req_lib.get(url, timeout=TILE_TIMEOUT, headers={'User-Agent': UA})
            if r.status_code == 200 and r.content:
                return (tx, ty, Image.open(io.BytesIO(r.content)).convert('RGBA'))
        except Exception:
            pass
        return (tx, ty, None)

    tile_map = {}
    for provider in TILE_PROVIDERS:
        tasks = [(tx, ty, provider)
                 for ty in range(ty_min, ty_max+1)
                 for tx in range(tx_min, tx_max+1)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(fetch_tile, tasks))
        ok = sum(1 for _, _, img in results if img)
        if ok >= max(1, len(tasks) * 0.5):
            tile_map = {(tx, ty): img for tx, ty, img in results if img}
            log.info('[route-png] %s — %d/%d tiles OK', provider.split('/')[5], ok, len(tasks))
            break
        log.warning('[route-png] provider failed %d/%d: %s', ok, len(tasks), provider[:60])

    # Stitch
    canvas = Image.new('RGBA', (canvas_w, canvas_h), (20, 40, 70, 255))
    for (tx, ty), img in tile_map.items():
        canvas.paste(img, ((tx-tx_min)*TILE_SIZE, (ty-ty_min)*TILE_SIZE))

    # Geo → pixel
    def geo_to_px(lon, lat):
        lo = lon + 360 if (cross_anti and lon < 0) else lon
        return (
            (lon_to_tx(lo, z)  - tx_min) * TILE_SIZE,
            (lat_to_ty(lat, z) - ty_min) * TILE_SIZE,
        )

    route_px = [geo_to_px(lo, la) for lo, la in zip(route_lons, route_lats)]

    draw = ImageDraw.Draw(canvas, 'RGBA')
    if len(route_px) >= 2:
        # Glow layers
        draw.line([(int(x), int(y)) for x, y in route_px], fill=(3, 105, 161, 80),  width=11)
        draw.line([(int(x), int(y)) for x, y in route_px], fill=(56, 189, 248, 140), width=6)
        draw.line([(int(x), int(y)) for x, y in route_px], fill=(56, 189, 248, 230), width=3)

    # Waypoint dots
    step = max(1, len(route_px) // 40)
    for i in range(1, len(route_px)-1, step):
        cx, cy = int(route_px[i][0]), int(route_px[i][1])
        draw.ellipse([cx-4, cy-4, cx+4, cy+4], fill=(125, 211, 252, 200), outline=(255,255,255,120), width=1)

    # Origin (green) + Destination (red)
    for (px, py), col in [(route_px[0], (22,163,74,255)), (route_px[-1], (220,38,38,255))]:
        cx, cy = int(px), int(py)
        draw.ellipse([cx-10, cy-10, cx+10, cy+10], fill=col, outline=(255,255,255,255), width=2)

    # Crop to route bounds with padding, maintain aspect ratio
    px_xs = [p[0] for p in route_px]; px_ys = [p[1] for p in route_px]
    pad_px = 70
    x0 = max(0, min(px_xs)-pad_px); x1 = min(canvas_w, max(px_xs)+pad_px)
    y0 = max(0, min(px_ys)-pad_px); y1 = min(canvas_h, max(px_ys)+pad_px)
    aspect = width / height
    cw, ch = x1-x0, y1-y0
    if cw/max(ch,1) < aspect:
        extra = ch*aspect - cw
        x0 = max(0, x0-extra/2); x1 = min(canvas_w, x1+extra/2)
    else:
        extra = cw/aspect - ch
        y0 = max(0, y0-extra/2); y1 = min(canvas_h, y1+extra/2)

    final = canvas.crop((int(x0), int(y0), int(x1), int(y1)))
    final = final.resize((width, height), Image.LANCZOS).convert('RGB')

    buf = io.BytesIO()
    final.save(buf, format='PNG')
    buf.seek(0)
    return buf.read()


def generate_route_svg(waypoints, width=1200, height=520):
    """
    Generate an SVG world map with the route overlaid.
    - Dark background matching the PDF theme
    - Mercator projection, auto-fitted to route bounds with padding
    - Handles antimeridian (Pacific) crossings by shifting to 0-360 lon range
    - No external dependencies — pure Python + stdlib
    Returns SVG string.
    """
    coastlines = _load_coastlines()

    route_lons = [p[0] for p in waypoints]
    route_lats = [p[1] for p in waypoints]

    # Detect antimeridian crossing (Pacific routes)
    has_pos = any(lo > 90  for lo in route_lons)
    has_neg = any(lo < -90 for lo in route_lons)
    cross_anti = has_pos and has_neg

    if cross_anti:
        # Shift to 0-360 so Pacific route is continuous
        route_lons_r = [lo + 360 if lo < 0 else lo for lo in route_lons]
        waypoints_r  = [[lo + 360 if lo < 0 else lo, la] for lo, la in waypoints]
    else:
        route_lons_r = route_lons
        waypoints_r  = [[lo, la] for lo, la in waypoints]

    # Compute view bounds with generous padding
    lon_min = min(route_lons_r); lon_max = max(route_lons_r)
    lat_min = min(route_lats);   lat_max = max(route_lats)
    lon_span = max(lon_max - lon_min, 15)
    lat_span = max(lat_max - lat_min,  8)
    pad_lon  = max(lon_span * 0.18, 4.0)
    pad_lat  = max(lat_span * 0.22, 3.0)

    vlon_min = lon_min - pad_lon
    vlon_max = lon_max + pad_lon
    vlat_min = max(lat_min - pad_lat, -82)
    vlat_max = min(lat_max + pad_lat,  82)

    def merc(lat):
        lat = max(-82, min(82, lat))
        return math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))

    merc_min = merc(vlat_min)
    merc_max = merc(vlat_max)

    def to_xy(lon, lat):
        """Project [lon, lat] to SVG pixel coordinates."""
        if cross_anti and lon < 0:
            lon += 360
        nx = (lon - vlon_min) / (vlon_max - vlon_min)
        ny = (merc_max - merc(lat)) / (merc_max - merc_min)
        return nx * width, ny * height

    # ── Build SVG ─────────────────────────────────────────────────────────────
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {width} {height}" width="{width}" height="{height}">',
        # Ocean background
        f'<rect width="{width}" height="{height}" fill="#0c1829"/>',
    ]

    # Graticule — subtle 30° grid
    for glon in range(int(vlon_min // 30) * 30, int(vlon_max // 30) * 30 + 31, 30):
        x, _ = to_xy(glon, vlat_min)
        if -10 <= x <= width + 10:
            _, y1 = to_xy(glon, vlat_min)
            _, y2 = to_xy(glon, vlat_max)
            parts.append(f'<line x1="{x:.1f}" y1="0" x2="{x:.1f}" y2="{height}" '
                         f'stroke="#1a2d4a" stroke-width="0.6"/>')

    for glat in range(int(vlat_min // 30) * 30, int(vlat_max // 30) * 30 + 31, 30):
        if vlat_min <= glat <= vlat_max:
            x1, y = to_xy(vlon_min, glat)
            x2, _ = to_xy(vlon_max, glat)
            parts.append(f'<line x1="0" y1="{y:.1f}" x2="{width}" y2="{y:.1f}" '
                         f'stroke="#1a2d4a" stroke-width="0.6"/>')

    # Landmasses — draw each coastline polyline
    for line in coastlines:
        pts = []
        for lon, lat in line:
            # Shift if cross-antimeridian map
            if cross_anti and lon < 0:
                lon += 360
            # Only include points within the extended view box
            if (vlon_min - 35 <= lon <= vlon_max + 35 and
                vlat_min - 10 <= lat <= vlat_max + 10):
                x, y = to_xy(lon, lat)
                pts.append(f'{x:.1f},{y:.1f}')
        if len(pts) >= 2:
            parts.append(
                f'<polyline points="{" ".join(pts)}" '
                f'fill="none" stroke="#2a4a6a" stroke-width="1.0" '
                f'stroke-linejoin="round" stroke-linecap="round"/>'
            )

    # Route — glow layer then sharp line
    rpts = []
    for lon, lat in waypoints_r:
        x, y = to_xy(lon, lat)
        rpts.append(f'{x:.1f},{y:.1f}')

    if len(rpts) >= 2:
        pts_str = ' '.join(rpts)
        # Outer glow
        parts.append(
            f'<polyline points="{pts_str}" fill="none" stroke="#3b82f6" '
            f'stroke-width="6" stroke-opacity="0.25" '
            f'stroke-linejoin="round" stroke-linecap="round"/>'
        )
        # Main route line
        parts.append(
            f'<polyline points="{pts_str}" fill="none" stroke="#60a5fa" '
            f'stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>'
        )

    # Origin marker — green circle with white ring
    ox, oy = to_xy(waypoints_r[0][0],  waypoints_r[0][1])
    parts.append(f'<circle cx="{ox:.1f}" cy="{oy:.1f}" r="7" fill="#22c55e" stroke="#fff" stroke-width="2"/>')
    parts.append(f'<circle cx="{ox:.1f}" cy="{oy:.1f}" r="3" fill="#fff"/>')

    # Destination marker — red circle with white ring
    dx, dy = to_xy(waypoints_r[-1][0], waypoints_r[-1][1])
    parts.append(f'<circle cx="{dx:.1f}" cy="{dy:.1f}" r="7" fill="#ef4444" stroke="#fff" stroke-width="2"/>')
    parts.append(f'<circle cx="{dx:.1f}" cy="{dy:.1f}" r="3" fill="#fff"/>')

    parts.append('</svg>')
    return '\n'.join(parts)


# ── COPERNICUS WMTS PROXY ENDPOINTS ──────────────────────────────────────────

@app.route("/api/cmems/tile")
def cmems_tile():
    params = {k: v for k, v in request.args.items()}
    params.setdefault('SERVICE', 'WMTS')
    params.setdefault('REQUEST', 'GetTile')
    params.setdefault('VERSION', '1.0.0')
    params.setdefault('FORMAT', 'image/png')
    content, ctype, status = cmems_request(params)
    resp = Response(content, status=status, content_type=ctype)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Cache-Control'] = 'public, max-age=3600'
    return resp


@app.route("/api/cmems/capabilities")
def cmems_capabilities():
    layer = request.args.get('layer', '')
    params = {'SERVICE':'WMTS','REQUEST':'GetCapabilities','VERSION':'1.0.0'}
    content, ctype, status = cmems_tile_proxy(layer, params)
    resp = Response(content, status=status, content_type=ctype)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Cache-Control'] = 'public, max-age=300'
    return resp


@app.route("/api/cmems/layers")
def cmems_layers():
    params = {'SERVICE':'WMTS','REQUEST':'GetCapabilities','VERSION':'1.0.0'}
    content, ctype, status = cmems_request(params)
    if status != 200:
        return Response(f"GetCapabilities failed: HTTP {status}\n{content[:500]}",
                       status=status, content_type='text/plain')
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(content)
        ns = {'wmts':'http://www.opengis.net/wmts/1.0','ows':'http://www.opengis.net/ows/1.1'}
        layers = [el.find('ows:Identifier',ns).text
                  for el in root.findall('.//wmts:Layer',ns)
                  if el.find('ows:Identifier',ns) is not None]
        keywords = ['wind','wav','cur','WIND','WAV','CUR','PHY','NRT','L4']
        relevant = [l for l in layers if any(k in l for k in keywords)]
        text = f"Total layers: {len(layers)}\nRelevant ({len(relevant)}):\n"
        text += "\n".join(relevant) + f"\n\nALL:\n" + "\n".join(layers)
        r = Response(text, content_type='text/plain')
        r.headers['Access-Control-Allow-Origin'] = '*'
        return r
    except Exception as e:
        return Response(f"Parse error: {e}\n{content[:2000].decode('utf-8','ignore')}",
                       content_type='text/plain')


@app.route("/api/cmems/featureinfo")
def cmems_featureinfo():
    params = {k: v for k, v in request.args.items()}
    params.setdefault('SERVICE','WMTS'); params.setdefault('REQUEST','GetFeatureInfo')
    params.setdefault('VERSION','1.0.0'); params.setdefault('INFOFORMAT','application/json')
    params.setdefault('TILEMATRIXSET','EPSG:3857')
    content, ctype, status = cmems_request(params)
    resp = Response(content, status=status, content_type=ctype)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp


@app.route("/api/cmems/status")
def cmems_status():
    configured = bool(CMEMS_USER and CMEMS_PASS)
    auth_ok, auth_method, auth_msg = verify_cmems_auth() if configured else (False,'none','Not configured')
    r = jsonify({
        'configured': configured, 'user': CMEMS_USER if configured else None,
        'auth_type': auth_method, 'token_valid': auth_ok, 'message': auth_msg,
        'layers': {
            'wind': {'product':'WIND_GLO_PHY_L4_NRT_012_004','variable':'wind'},
            'wave': {'product':'GLOBAL_ANALYSISFORECAST_WAV_001_027','variable':'VHM0'},
            'current': {'product':'GLOBAL_ANALYSISFORECAST_PHY_001_024','variable':'sea_water_velocity'},
        }
    })
    r.headers['Access-Control-Allow-Origin'] = '*'
    return r



# ── SEA CONDITION CHART ENGINE ────────────────────────────────────────────────
# Generates professional synoptic weather charts (isobars + wind barbs + wave fill)
# matching Geoserve/ZeroNorth quality for PDF embedding and interactive map overlay.
#
# Data flow:
#   1. Fetch MSLP + wind grid from Supabase DB1 for given bbox + timestamp
#   2. scipy.ndimage.gaussian_filter fills land gaps → smooth contours
#   3. matplotlib renders: wave gradient fill + isobar lines + wind barbs + route
#   4. Returns base64 PNG (for PDF) or GeoJSON LineStrings (for MapLibre)

# Colour scale matching Geoserve: blue(0m) → cyan → green → yellow → orange → red → magenta(15m+)
_WAVE_COLORS = [
    (0.0,  '#0033aa'), (0.07, '#0055cc'), (0.13, '#0077ee'),
    (0.20, '#00aaff'), (0.27, '#00ccdd'), (0.33, '#00cc88'),
    (0.40, '#44dd00'), (0.47, '#aaee00'), (0.53, '#ffee00'),
    (0.60, '#ffaa00'), (0.67, '#ff6600'), (0.73, '#ff2200'),
    (0.80, '#dd0033'), (0.87, '#bb0066'), (0.93, '#990099'),
    (1.00, '#770099'),
]
_WAVE_CMAP = mcolors.LinearSegmentedColormap.from_list(
    'geoserve_wave',
    [(v, c) for v, c in _WAVE_COLORS]
) if HAS_SCIPY else None


def _fetch_mslp_wind_grid(bbox, snap_ts_iso):
    """
    Fetch MSLP + wind grid from Supabase DB1 for a bounding box + timestamp.
    Uses paginated REST requests to bypass 1000-row limit.
    Returns list of dicts: {lat, lon, mslp, wind_speed, wind_dir}
    """
    if not SUPABASE_URL or not SUPABASE_ANON:
        raise RuntimeError("SUPABASE_URL / SUPABASE_ANON env vars not set")

    min_lon, min_lat, max_lon, max_lat = bbox

    # Snap timestamp to nearest 6h GFS step
    from datetime import datetime, timezone
    try:
        d = datetime.fromisoformat(snap_ts_iso.replace('Z', '+00:00'))
    except Exception:
        d = datetime.now(timezone.utc)
    epoch = d.timestamp()
    snapped = round(epoch / 21600) * 21600
    snap_dt = datetime.fromtimestamp(snapped, tz=timezone.utc)
    snap_str = snap_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    base_url = (
        f"{SUPABASE_URL}/rest/v1/weather_grid"
        f"?select=lat,lon,mslp,wind_speed,wind_dir"
        f"&lat=gte.{min_lat:.1f}&lat=lte.{max_lat:.1f}"
        f"&lon=gte.{min_lon:.1f}&lon=lte.{max_lon:.1f}"
        f"&valid_time=eq.{snap_str}"
    )
    headers = {
        'apikey':        SUPABASE_ANON,
        'Authorization': f'Bearer {SUPABASE_ANON}',
        'Prefer':        'count=none',
    }

    all_rows = []
    offset, page = 0, 1000
    while True:
        url = base_url + f"&limit={page}&offset={offset}"
        r = req_lib.get(url, headers=headers, timeout=20)
        if r.status_code != 200:
            raise RuntimeError(f"Supabase query failed: HTTP {r.status_code} - {r.text[:200]}")
        rows = r.json()
        all_rows.extend(rows)
        if len(rows) < page:
            break
        offset += page

    # Decode scaled values
    decoded = []
    for row in all_rows:
        decoded.append({
            'lat':        float(row['lat']),
            'lon':        float(row['lon']),
            'mslp':       (float(row['mslp']) + 950.0) if row['mslp'] is not None else None,
            'wind_speed': (float(row['wind_speed']) / 10.0) if row['wind_speed'] is not None else None,
            'wind_dir':   float(row['wind_dir']) if row['wind_dir'] is not None else None,
        })
    log.info(f"Sea condition: fetched {len(decoded)} grid points for {snap_str}")
    return decoded, snap_dt


def _build_regular_grid(rows, bbox, resolution=0.5):
    """
    Convert sparse ocean-masked rows into a regular rectangular grid.
    Land points (missing from ocean mask) are filled with NaN,
    then smoothed with gaussian_filter to produce continuous contours.
    Returns (lats_1d, lons_1d, mslp_grid_2d, wind_speed_2d, wind_dir_2d)
    """
    import numpy as np

    min_lon, min_lat, max_lon, max_lat = bbox

    # Build regular coordinate axes
    lats = np.arange(min_lat, max_lat + resolution/2, resolution)
    lons = np.arange(min_lon, max_lon + resolution/2, resolution)
    nla, nlo = len(lats), len(lons)

    mslp_g  = np.full((nla, nlo), np.nan)
    wsp_g   = np.full((nla, nlo), np.nan)
    wdir_g  = np.full((nla, nlo), np.nan)

    # Index rows into grid
    for row in rows:
        la_idx = round((row['lat'] - min_lat) / resolution)
        lo_idx = round((row['lon'] - min_lon) / resolution)
        if 0 <= la_idx < nla and 0 <= lo_idx < nlo:
            if row['mslp']       is not None: mslp_g[la_idx, lo_idx] = row['mslp']
            if row['wind_speed'] is not None: wsp_g [la_idx, lo_idx] = row['wind_speed']
            if row['wind_dir']   is not None: wdir_g[la_idx, lo_idx] = row['wind_dir']

    # Fill NaN land gaps via interpolation then smooth
    # Strategy: replace NaN with nearest valid value, then gaussian smooth
    from scipy.ndimage import gaussian_filter, label

    def fill_and_smooth(grid, sigma=1.5):
        filled = grid.copy()
        nan_mask = np.isnan(filled)
        if not nan_mask.any():
            return gaussian_filter(filled, sigma=sigma)
        # Simple nearest-neighbour fill for NaN cells
        from scipy.ndimage import distance_transform_edt
        ind = distance_transform_edt(nan_mask, return_distances=False, return_indices=True)
        filled = filled[tuple(ind)]
        return gaussian_filter(filled, sigma=sigma)

    mslp_smooth = fill_and_smooth(mslp_g, sigma=1.2)
    wsp_smooth  = fill_and_smooth(wsp_g,  sigma=0.8)
    wdir_smooth = fill_and_smooth(wdir_g, sigma=0.8)

    return lats, lons, mslp_smooth, mslp_g, wsp_smooth, wsp_g, wdir_smooth


def normLon180(lon):
    """Normalise longitude to -180..180."""
    while lon > 180:  lon -= 360
    while lon <= -180: lon += 360
    return lon


def _is_land_simple(lat, lon):
    """
    Fast land/ocean test for chart rendering.
    lon must be in 0-360 range (as from GFS grid).
    Returns True if the point is land.
    """
    # Convert 0-360 to -180..180 for the same logic as ETL is_land()
    lo = lon if lon <= 180 else lon - 360

    if lat < -75: return True

    # North America with tiered Pacific coast carve-out
    if 25 <= lat <= 72 and -168 <= lo <= -52:
        if lat >= 60 and lo < -136: return False
        if 48 <= lat < 60 and lo < -126: return False
        if 35 <= lat < 48 and lo < -124: return False
        if 25 <= lat < 35 and lo < -117: return False
        if 15 <= lat <= 31 and -98 <= lo <= -60: return False
        if 51 <= lat <= 65 and -95 <= lo <= -65: return False
        return True

    if 60 <= lat <= 84 and -57 <= lo <= -17: return True   # Greenland
    if -56 <= lat <= 13 and -82 <= lo <= -34: return True  # S America
    if 36 <= lat <= 71 and 0 < lo <= 30:
        if 53 <= lat <= 66 and 4 <= lo <= 15: return False
        return True
    if 55 <= lat <= 71 and 5 <= lo <= 32: return True
    if 10 <= lat <= 37 and -18 <= lo <= 52:
        if 12 <= lat <= 30 and 32 <= lo <= 43: return False
        return True
    if -20 <= lat <= 10 and 2 <= lo <= 15: return True
    if -20 <= lat <= 10 and 15 < lo <= 45:
        if -20 <= lat <= -10 and 38 <= lo <= 45: return False
        return True
    if -35 <= lat <= -17 and 12 <= lo <= 36: return True
    if 12 <= lat <= 42 and 26 <= lo <= 63:
        if 5 <= lat <= 25 and 55 <= lo <= 63: return False
        if 12 <= lat <= 30 and 32 <= lo <= 43: return False
        return True
    if 5 <= lat <= 55 and 60 <= lo <= 95:
        if 5 <= lat <= 25 and 60 <= lo <= 75: return False
        if 5 <= lat <= 22 and 78 <= lo <= 95: return False
        return True
    if 18 <= lat <= 55 and 95 <= lo <= 145:
        if 24 <= lat <= 41 and 118 <= lo <= 130: return False
        if 30 <= lat <= 46 and 130 <= lo <= 142: return False
        return True
    if 0 <= lat <= 28 and 95 <= lo <= 110:
        if 0 <= lat <= 15 and 100 <= lo <= 110: return False
        return True
    if 50 <= lat <= 78 and 95 <= lo <= 180:
        if 50 <= lat <= 62 and 140 <= lo <= 160: return False
        return True
    if -39 <= lat <= -10 and 113 <= lo <= 154: return True
    if 63 <= lat <= 67 and -25 <= lo <= -12: return True
    return False


def generate_sea_condition_png(
    bbox, timestamp_iso, waypoints=None,
    vessel_pos=None, width_px=1200, height_px=680,
    title_suffix=''
):
    """
    Render a professional sea condition chart PNG.
    Layer stack: Esri Ocean tiles → wave heatmap (ocean-only) →
                 isobars (ocean-only) → route/vessel → title/colorbar.
    """
    if not HAS_SCIPY:
        raise RuntimeError("scipy/matplotlib not installed")

    import io as _io, math as _math, concurrent.futures
    import numpy as np
    import requests as req_lib
    from PIL import Image as _Image, ImageDraw as _ImageDraw
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    from scipy.ndimage import gaussian_filter

    min_lon, min_lat, max_lon, max_lat = bbox

    # ── 1. Fetch + build grid ─────────────────────────────────────────────────
    rows, snap_dt = _fetch_mslp_wind_grid(bbox, timestamp_iso)
    if not rows:
        raise RuntimeError(f"No data in DB for {timestamp_iso} / {bbox}")

    lats, lons, mslp_s, mslp_raw, wsp_s, wsp_raw, wdir_s = _build_regular_grid(rows, bbox)
    nla, nlo = len(lats), len(lons)
    LON_G, LAT_G = np.meshgrid(lons, lats)

    # Land mask at DB grid resolution
    land_mask_2d = np.zeros((nla, nlo), dtype=bool)
    for i, la in enumerate(lats):
        for j, lo in enumerate(lons):
            lo360 = lo + 360 if lo < 0 else lo
            land_mask_2d[i, j] = _is_land_simple(float(la), float(lo360))
    ocean_mask_2d = ~land_mask_2d

    # ── 2. Stitch tile basemap ────────────────────────────────────────────────
    TILE_PROVIDERS = [
        'https://services.arcgisonline.com/arcgis/rest/services/Ocean/World_Ocean_Base/MapServer/tile/{z}/{y}/{x}',
        'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
        'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
    ]
    TILE_SIZE = 256
    UA = 'RoutePlannerPro/2.4 (maritime voyage planning)'

    def lon_to_tx(lon, z): return (lon + 180) / 360 * (2 ** z)
    def lat_to_ty(lat, z):
        lr = _math.radians(max(-85.05, min(85.05, lat)))
        return (1 - _math.log(_math.tan(lr) + 1/_math.cos(lr)) / _math.pi) / 2 * (2**z)
    def tile_nw_lon(tx, z): return tx / (2**z) * 360 - 180
    def tile_nw_lat(ty, z):
        n = _math.pi - 2*_math.pi*ty/(2**z)
        return _math.degrees(_math.atan(_math.sinh(n)))

    z = 3
    for zoom in range(2, 7):
        z = zoom
        if (lon_to_tx(max_lon, zoom) - lon_to_tx(min_lon, zoom)) * TILE_SIZE >= 900:
            break

    n_tiles = 2 ** z
    tx_min = int(_math.floor(lon_to_tx(min_lon, z)))
    tx_max = int(_math.floor(lon_to_tx(max_lon, z)))
    ty_min = int(_math.floor(lat_to_ty(max_lat, z)))
    ty_max = int(_math.floor(lat_to_ty(min_lat, z)))
    canvas_w = (tx_max - tx_min + 1) * TILE_SIZE
    canvas_h = (ty_max - ty_min + 1) * TILE_SIZE

    def fetch_tile(args):
        tx, ty, url_tpl = args
        url = url_tpl.format(z=z, x=tx % n_tiles, y=ty)
        try:
            r = req_lib.get(url, timeout=8, headers={'User-Agent': UA})
            if r.status_code == 200 and r.content:
                return (tx, ty, _Image.open(_io.BytesIO(r.content)).convert('RGBA'))
        except Exception:
            pass
        return (tx, ty, None)

    tile_map = {}
    for provider in TILE_PROVIDERS:
        tasks = [(tx, ty, provider)
                 for ty in range(ty_min, ty_max+1)
                 for tx in range(tx_min, tx_max+1)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(fetch_tile, tasks))
        ok = sum(1 for _, _, img in results if img)
        if ok >= max(1, len(tasks) * 0.5):
            tile_map = {(tx, ty): img for tx, ty, img in results if img}
            log.info('[sea-cond] tiles %d/%d from %s', ok, len(tasks), provider.split('/')[5])
            break

    tile_canvas = _Image.new('RGBA', (canvas_w, canvas_h), (20, 40, 70, 255))
    for (tx, ty), img in tile_map.items():
        tile_canvas.paste(img, ((tx-tx_min)*TILE_SIZE, (ty-ty_min)*TILE_SIZE))

    canvas_lon_min = tile_nw_lon(tx_min, z)
    canvas_lon_max = tile_nw_lon(tx_max + 1, z)
    canvas_lat_max = tile_nw_lat(ty_min, z)
    canvas_lat_min = tile_nw_lat(ty_max + 1, z)

    def geo_to_px(lon, lat):
        return (
            (lon_to_tx(lon, z) - tx_min) * TILE_SIZE,
            (lat_to_ty(lat, z) - ty_min) * TILE_SIZE,
        )

    # ── 3. Wave heatmap on transparent layer ──────────────────────────────────
    # PM wave estimate; cap at 15m to match reference colour scale
    vms = wsp_s * 0.5144
    wave_est = np.clip(0.0248 * vms**2, 0, 15)

    # Key fix for rectangular gap artefacts:
    # The DB only has ocean points — missing cells (land + edge gaps) are NaN.
    # We fill ALL NaNs with nearest-ocean values, smooth, THEN re-apply the
    # ocean mask so land shows transparent. This removes hard rectangle edges.
    from scipy.ndimage import distance_transform_edt
    wave_raw = np.where(ocean_mask_2d, wave_est, np.nan)
    nan_mask = np.isnan(wave_raw)
    if nan_mask.any():
        ind = distance_transform_edt(nan_mask, return_distances=False, return_indices=True)
        wave_filled = wave_raw.copy()
        wave_filled[nan_mask] = wave_raw[tuple([idx[nan_mask] for idx in ind])]
        # Handle remaining NaNs (all-NaN rows/cols at edges)
        wave_filled = np.nan_to_num(wave_filled, nan=0.0)
    else:
        wave_filled = wave_raw.copy()
    wave_smooth = gaussian_filter(wave_filled, sigma=1.5)
    # Re-apply ocean mask — land stays NaN → transparent in colormap
    wave_ocean = np.where(ocean_mask_2d, wave_smooth, np.nan)

    # Render wave on transparent figure aligned to tile canvas geo extent
    fig_w, fig_h = canvas_w / 100, canvas_h / 100
    fig_wave, ax_wave = plt.subplots(figsize=(fig_w, fig_h), dpi=100)
    fig_wave.patch.set_alpha(0)
    ax_wave.set_facecolor((0, 0, 0, 0))
    ax_wave.set_xlim(canvas_lon_min, canvas_lon_max)
    ax_wave.set_ylim(canvas_lat_min, canvas_lat_max)
    ax_wave.axis('off')
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

    cmap_wave = _WAVE_CMAP.copy()
    cmap_wave.set_bad((0, 0, 0, 0))   # transparent for NaN (land + no-data)

    ax_wave.imshow(
        wave_ocean,
        extent=[min_lon, max_lon, min_lat, max_lat],
        origin='lower', aspect='auto',
        cmap=cmap_wave, vmin=0, vmax=15,
        interpolation='bilinear', alpha=0.80,
    )
    buf_wave = _io.BytesIO()
    fig_wave.savefig(buf_wave, format='png', dpi=100, transparent=True,
                     bbox_inches='tight', pad_inches=0)
    plt.close(fig_wave)
    buf_wave.seek(0)
    wave_layer = _Image.open(buf_wave).convert('RGBA').resize((canvas_w, canvas_h), _Image.LANCZOS)

    combined = tile_canvas.copy()
    combined.paste(wave_layer, (0, 0), wave_layer)

    # ── 4. Isobar overlay — ocean-only, transparent figure ───────────────────
    mslp_data = mslp_raw[~np.isnan(mslp_raw)]
    if len(mslp_data) >= 4:
        p_min = int(_math.floor(np.nanmin(mslp_data) / 4) * 4)
        p_max = int(_math.ceil( np.nanmax(mslp_data) / 4) * 4)
        levels = list(range(p_min, p_max + 4, 4))

        # Fill + smooth MSLP for continuous contours, then mask land
        mslp_nan = mslp_raw.copy().astype(float)
        mslp_nan[land_mask_2d] = np.nan
        mslp_nan_mask = np.isnan(mslp_nan)
        if mslp_nan_mask.any():
            ind2 = distance_transform_edt(mslp_nan_mask, return_distances=False, return_indices=True)
            mslp_filled = mslp_nan.copy()
            mslp_filled[mslp_nan_mask] = mslp_nan[tuple([idx[mslp_nan_mask] for idx in ind2])]
            mslp_filled = np.nan_to_num(mslp_filled, nan=float(np.nanmean(mslp_data)))
        else:
            mslp_filled = mslp_nan.copy()
        mslp_for_contour = gaussian_filter(mslp_filled, sigma=1.2)
        # Re-mask land to NaN so contours don't cross land
        mslp_ocean = np.where(ocean_mask_2d, mslp_for_contour, np.nan)

        fig_iso, ax_iso = plt.subplots(figsize=(fig_w, fig_h), dpi=100)
        fig_iso.patch.set_alpha(0)
        ax_iso.set_facecolor((0, 0, 0, 0))
        ax_iso.set_xlim(canvas_lon_min, canvas_lon_max)
        ax_iso.set_ylim(canvas_lat_min, canvas_lat_max)
        ax_iso.axis('off')
        plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

        for level in levels:
            # Bold, visible isobars matching professional reference style
            if   level <= 996:  col, lw, ls = '#cc0022', 1.8, 'solid'
            elif level <= 1004: col, lw, ls = '#ee4466', 1.3, 'solid'
            elif level <= 1016: col, lw, ls = '#555566', 1.1, 'solid'
            elif level <= 1024: col, lw, ls = '#444455', 1.2, 'solid'
            else:               col, lw, ls = '#333344', 1.4, 'solid'
            try:
                cs = ax_iso.contour(LON_G, LAT_G, mslp_ocean,
                                    levels=[level], colors=[col],
                                    linewidths=lw, linestyles=ls, alpha=0.92)
                # Label every 4 hPa for readability
                if level % 4 == 0:
                    ax_iso.clabel(cs, fmt='%d', fontsize=8.5, colors=[col],
                                  inline=True, inline_spacing=4,
                                  fontproperties={'weight': 'bold'})
            except Exception:
                pass

        buf_iso = _io.BytesIO()
        fig_iso.savefig(buf_iso, format='png', dpi=100, transparent=True,
                        bbox_inches='tight', pad_inches=0)
        plt.close(fig_iso)
        buf_iso.seek(0)
        iso_layer = _Image.open(buf_iso).convert('RGBA').resize((canvas_w, canvas_h), _Image.LANCZOS)
        combined.paste(iso_layer, (0, 0), iso_layer)

    # ── 5. Route + vessel drawn with PIL ─────────────────────────────────────
    draw = _ImageDraw.Draw(combined, 'RGBA')

    if waypoints and len(waypoints) >= 2:
        step_wp = max(1, len(waypoints) // 120)
        disp_wps = waypoints[::step_wp]
        if waypoints[-1] not in disp_wps:
            disp_wps = list(disp_wps) + [waypoints[-1]]
        route_px = [(int(x), int(y)) for x, y in
                    (geo_to_px(normLon180(p[0]), p[1]) for p in disp_wps)]
        if len(route_px) >= 2:
            # Thin dashed-style: draw full line then overdraw gaps
            draw.line(route_px, fill=(0, 220, 130, 60),  width=6)   # glow
            draw.line(route_px, fill=(0, 220, 130, 200), width=2)   # main
            # Dashes: overdraw with transparent gaps every ~15px
            seg_len = 12; gap_len = 8
            acc = 0
            drawing = True
            for i in range(1, len(route_px)):
                x0, y0 = route_px[i-1]; x1, y1 = route_px[i]
                seg = _math.hypot(x1-x0, y1-y0)
                if seg < 1: continue
                dx, dy = (x1-x0)/seg, (y1-y0)/seg
                pos = 0.0
                while pos < seg:
                    remaining = seg - pos
                    chunk = seg_len - acc if drawing else gap_len - acc
                    chunk = min(chunk, remaining)
                    if not drawing:
                        # overdraw with basemap color to simulate gap
                        draw.line([
                            (int(x0 + dx*(pos)), int(y0 + dy*(pos))),
                            (int(x0 + dx*(pos+chunk)), int(y0 + dy*(pos+chunk)))
                        ], fill=(0, 0, 0, 0), width=3)
                    acc += chunk; pos += chunk
                    if (drawing and acc >= seg_len) or (not drawing and acc >= gap_len):
                        drawing = not drawing; acc = 0

        ox, oy = route_px[0]
        draw.ellipse([ox-9, oy-9, ox+9, oy+9], fill=(0,210,120,255), outline=(255,255,255,220), width=2)
        dx2, dy2 = route_px[-1]
        draw.ellipse([dx2-9, dy2-9, dx2+9, dy2+9], fill=(220,40,60,255), outline=(255,255,255,220), width=2)

    if vessel_pos:
        vlo, vla = normLon180(vessel_pos[0]), vessel_pos[1]
        vx, vy = int(geo_to_px(vlo, vla)[0]), int(geo_to_px(vlo, vla)[1])
        s = 11
        draw.polygon([(vx,vy-s),(vx+s,vy),(vx,vy+s),(vx-s,vy)],
                     fill=(255,221,0,255), outline=(20,30,50,255))

    # ── 6. Crop to bbox ───────────────────────────────────────────────────────
    cx0, cy1 = geo_to_px(min_lon, min_lat)
    cx1, cy0 = geo_to_px(max_lon, max_lat)
    cx0 = max(0, int(cx0)); cx1 = min(canvas_w, int(cx1))
    cy0 = max(0, int(cy0)); cy1 = min(canvas_h, int(cy1))
    cropped = combined.crop((cx0, cy0, cx1, cy1))

    chart_h_px = height_px - 90   # reserve 90px for title bar + colorbar
    chart_img  = cropped.resize((width_px, chart_h_px), _Image.LANCZOS).convert('RGB')
    chart_arr  = np.array(chart_img)

    # ── 7. Final figure: embed chart + colorbar + title ───────────────────────
    # Use ScalarMappable for a proper, visible colorbar
    from matplotlib.cm import ScalarMappable
    from matplotlib.colors import Normalize

    fig_final = plt.figure(figsize=(width_px/100, height_px/100), dpi=100,
                           facecolor='#0c1829')
    # Axes layout: chart takes most space, colorbar strip at bottom
    ax_chart = fig_final.add_axes([0, 90/height_px, 1, (height_px-90)/height_px])
    ax_chart.imshow(chart_arr, aspect='auto', interpolation='lanczos')
    ax_chart.axis('off')

    # Title — two lines, left-aligned, white
    ts_label = snap_dt.strftime('%d %b %Y, %H:%M UTC')
    title_line1 = 'Sea Condition — Wave Height, Wind & Pressure'
    title_line2 = ts_label + ((' — ' + title_suffix) if title_suffix else '')
    ax_chart.set_title(title_line1 + '\n' + title_line2,
                       fontsize=9, color='#e2e8f0', pad=5, loc='left',
                       fontweight='bold')

    # Colorbar using ScalarMappable (always works, no imshow reference needed)
    ax_cbar = fig_final.add_axes([0.05, 0.01, 0.90, 0.06])
    ax_cbar.set_facecolor('#0c1829')
    sm = ScalarMappable(cmap=_WAVE_CMAP, norm=Normalize(vmin=0, vmax=15))
    sm.set_array([])
    cbar = fig_final.colorbar(sm, cax=ax_cbar, orientation='horizontal')
    cbar.set_label('Estimated Wave Height (m)', fontsize=8.5, color='#94a3b8', labelpad=3)
    cbar.ax.tick_params(labelsize=8, colors='#94a3b8')
    cbar.outline.set_edgecolor('#334466')
    cbar.set_ticks([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15])

    buf_final = _io.BytesIO()
    fig_final.savefig(buf_final, format='png', dpi=100,
                      facecolor='#0c1829', edgecolor='none', bbox_inches='tight')
    plt.close(fig_final)
    buf_final.seek(0)
    return base64.b64encode(buf_final.read()).decode('utf-8')


def generate_sea_condition_geojson(bbox, timestamp_iso):
    """
    Generate isobar GeoJSON for MapLibre rendering.
    Returns {type: FeatureCollection, features: [...LineString features]}
    Each feature has properties: {pressure, color, width}
    """
    if not HAS_SCIPY:
        raise RuntimeError("scipy/matplotlib not installed")

    import numpy as np

    rows, snap_dt = _fetch_mslp_wind_grid(bbox, timestamp_iso)
    if not rows:
        return {'type': 'FeatureCollection', 'features': [], 'timestamp': timestamp_iso}

    lats, lons, mslp_s, mslp_raw, wsp_s, _, wdir_s =         _build_regular_grid(rows, bbox)

    mslp_data = mslp_raw[~np.isnan(mslp_raw)]
    if len(mslp_data) == 0:
        return {'type': 'FeatureCollection', 'features': [], 'timestamp': timestamp_iso}

    p_min = int(math.floor(np.min(mslp_data) / 4) * 4)
    p_max = int(math.ceil( np.max(mslp_data) / 4) * 4)
    levels = list(range(p_min, p_max + 4, 4))

    LON_G, LAT_G = np.meshgrid(lons, lats)

    features = []
    fig_tmp, ax_tmp = plt.subplots()

    for level in levels:
        try:
            cs = ax_tmp.contour(LON_G, LAT_G, mslp_s, levels=[level])
            for path in cs.get_paths():
                coords = path.vertices.tolist()
                if len(coords) < 2:
                    continue
                # Colour coding
                if level <= 996:   col, width = '#ff2244', 2.0
                elif level <= 1004: col, width = '#ff6688', 1.5
                elif level <= 1012: col, width = '#888888', 1.0
                elif level <= 1020: col, width = '#555555', 1.2
                else:               col, width = '#333333', 1.4

                features.append({
                    'type': 'Feature',
                    'geometry': {'type': 'LineString', 'coordinates': coords},
                    'properties': {
                        'pressure': level,
                        'color':    col,
                        'width':    width,
                        'label':    str(level) if level % 8 == 0 else '',
                    }
                })
        except Exception:
            pass

    plt.close(fig_tmp)

    # Also include wind vectors for barb rendering in JS
    step = max(1, min(len(lats), len(lons)) // 20)
    wind_pts = []
    for bi, la in enumerate(lats[::step]):
        for bj, lo in enumerate(lons[::step]):
            la_i = min(round((la - lats[0]) / 0.5), len(lats)-1)
            lo_j = min(round((lo - lons[0]) / 0.5), len(lons)-1)
            ws  = float(wsp_raw[la_i, lo_j]) if not np.isnan(wsp_s[la_i, lo_j]) else None
            wd  = float(wdir_s[la_i, lo_j])  if not np.isnan(wdir_s[la_i, lo_j]) else None
            if ws is not None and ws > 1:
                wind_pts.append({'lat': la, 'lon': lo, 'speed': round(ws, 1), 'dir': round(wd or 0)})

    return {
        'type':      'FeatureCollection',
        'features':  features,
        'timestamp': snap_dt.isoformat(),
        'wind':      wind_pts,
    }


# ── SEA CONDITION ENDPOINT ────────────────────────────────────────────────────

@app.route("/api/sea-condition", methods=["POST", "OPTIONS"])
def sea_condition():
    """
    Generate sea condition chart.

    Body (JSON):
      bbox:       [minLon, minLat, maxLon, maxLat]
      timestamp:  ISO 8601 string (snapped to nearest 6h internally)
      waypoints:  [[lon, lat], ...]  optional route overlay
      vessel_pos: [lon, lat]         optional vessel marker
      format:     "png" (default, for PDF) | "geojson" (for MapLibre)
      width:      px (default 1200, PNG only)
      height:     px (default 680,  PNG only)
      title:      string appended to chart title

    Returns:
      format=png:     {"png": "<base64>", "timestamp": "..."}
      format=geojson: GeoJSON FeatureCollection + wind array
    """
    if request.method == 'OPTIONS':
        r = jsonify({'ok': True})
        r.headers['Access-Control-Allow-Origin'] = '*'
        r.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        return r

    if not HAS_SCIPY:
        r = jsonify({'error': 'scipy/matplotlib not installed on server'})
        r.headers['Access-Control-Allow-Origin'] = '*'
        return r, 503

    try:
        data      = request.get_json(force=True) or {}
        bbox      = data.get('bbox')
        ts        = data.get('timestamp', '')
        waypoints = data.get('waypoints')
        vpos      = data.get('vessel_pos')
        fmt       = data.get('format', 'png')
        width     = min(int(data.get('width',  1200)), 2400)
        height    = min(int(data.get('height',  680)),  900)
        title     = data.get('title', '')

        if not bbox or len(bbox) != 4:
            return jsonify({'error': 'bbox required: [minLon, minLat, maxLon, maxLat]'}), 400
        if not ts:
            return jsonify({'error': 'timestamp required (ISO 8601)'}), 400

        if fmt == 'geojson':
            result = generate_sea_condition_geojson(bbox, ts)
            r = jsonify(result)
        else:
            b64 = generate_sea_condition_png(
                bbox, ts, waypoints=waypoints, vessel_pos=vpos,
                width_px=width, height_px=height, title_suffix=title
            )
            r = jsonify({'png': b64, 'timestamp': ts})

        r.headers['Access-Control-Allow-Origin'] = '*'
        return r

    except Exception as e:
        log.error('sea-condition error: %s', e, exc_info=True)
        r = jsonify({'error': str(e)})
        r.headers['Access-Control-Allow-Origin'] = '*'
        return r, 500


# ── ROUTE MAP ENDPOINT ───────────────────────────────────────────────────────

@app.route("/api/route-map", methods=["POST"])
def route_map():
    """
    Generate an SVG map of the route for PDF embedding.
    Body: {"waypoints": [[lon, lat], ...], "width": 1200, "height": 520}
    Returns: {"svg": "<svg...>", "format": "svg"}
    """
    try:
        data      = request.get_json(force=True)
        waypoints = data.get('waypoints', [])
        width     = min(int(data.get('width',  1200)), 2400)
        height    = min(int(data.get('height',  520)),  900)

        if len(waypoints) < 2:
            return jsonify({"error": "Need at least 2 waypoints"}), 400

        svg = generate_route_svg(waypoints, width, height)
        r   = jsonify({"svg": svg, "format": "svg", "width": width, "height": height})
        r.headers['Access-Control-Allow-Origin'] = '*'
        return r

    except Exception as e:
        log.error('route-map error: %s', e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/static-map", methods=["POST"])
def static_map():
    """
    Generate a route chart PNG by stitching real map tiles server-side.
    Uses Esri Ocean Basemap (free, no API key) — professional maritime style.
    Body: {"waypoints": [[lon, lat], ...], "width": 1200, "height": 500}
    Returns: PNG image bytes (Content-Type: image/png)
    """
    try:
        data      = request.get_json(force=True)
        waypoints = data.get('waypoints', [])
        width     = min(int(data.get('width',  1200)), 2048)
        height    = min(int(data.get('height',  500)), 2048)

        if len(waypoints) < 2:
            return jsonify({"error": "Need at least 2 waypoints"}), 400

        log.info('[static-map] generating route PNG: %d waypoints %dx%d', len(waypoints), width, height)
        png_bytes = generate_route_png(waypoints, width, height)

        return png_bytes, 200, {
            'Content-Type': 'image/png',
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
        }

    except Exception as e:
        log.error('static-map error: %s', e)
        return jsonify({"error": str(e)}), 500


# ── TSS DEBUG ENDPOINT ────────────────────────────────────────────────────────

@app.route("/api/tss-debug", methods=["POST"])
def tss_debug():
    """
    Analyse a route for TSS proximity matches without modifying the route.
    Shows closest approach distance, selected lane, and splice indices per zone.
    Body: {"coords": [[lon, lat], ...]}
    """
    try:
        data   = request.get_json(force=True)
        coords = data.get('coords', [])
        if len(coords) < 2:
            return jsonify({"error": "Need at least 2 coords"}), 400

        overall_bearing = bearing_deg(coords[0][0], coords[0][1],
                                      coords[-1][0], coords[-1][1])
        findings = []

        for tss_key, tss in TSS_ZONES.items():
            ref_lon, ref_lat = tss['reference']
            trigger_nm       = tss['trigger_nm']

            # Find segment of closest approach
            min_dist_nm = float('inf')
            closest_seg = -1
            for i in range(len(coords) - 1):
                d = _pt_to_seg_nm(ref_lon, ref_lat,
                                  coords[i][0], coords[i][1],
                                  coords[i+1][0], coords[i+1][1])
                if d < min_dist_nm:
                    min_dist_nm = d
                    closest_seg = i

            triggered = min_dist_nm <= trigger_nm

            finding = {
                'tss':           tss_key,
                'name':          tss['name'],
                'reference':     [ref_lon, ref_lat],
                'trigger_nm':    trigger_nm,
                'closest_nm':    round(min_dist_nm, 1),
                'triggered':     triggered,
            }

            if triggered:
                seg_bearing = bearing_deg(
                    coords[closest_seg][0], coords[closest_seg][1],
                    coords[closest_seg+1][0], coords[closest_seg+1][1]
                )
                lane_wps = _select_tss_lane(tss, seg_bearing, overall_bearing)

                # Find entry/exit segment indices
                entry_seg, exit_seg = closest_seg, closest_seg
                if lane_wps:
                    search_start = max(0, closest_seg - 30)
                    search_end   = min(len(coords) - 1, closest_seg + 30)
                    min_e, min_x = float('inf'), float('inf')
                    for i in range(search_start, search_end):
                        de = _pt_to_seg_nm(lane_wps[0][0], lane_wps[0][1],
                                           coords[i][0], coords[i][1],
                                           coords[i+1][0], coords[i+1][1])
                        dx = _pt_to_seg_nm(lane_wps[-1][0], lane_wps[-1][1],
                                           coords[i][0], coords[i][1],
                                           coords[i+1][0], coords[i+1][1])
                        if de < min_e: min_e, entry_seg = de, i
                        if dx < min_x: min_x, exit_seg  = dx, i

                finding.update({
                    'closest_seg':   closest_seg,
                    'seg_bearing':   round(seg_bearing, 1),
                    'lane_selected': list(tss.keys() - {'name','reference','trigger_nm'})[0]
                                     if not lane_wps else 'matched',
                    'lane_first_wp': lane_wps[0]  if lane_wps else None,
                    'lane_last_wp':  lane_wps[-1] if lane_wps else None,
                    'lane_wp_count': len(lane_wps) if lane_wps else 0,
                    'entry_seg':     entry_seg,
                    'exit_seg':      exit_seg,
                    'entry_wp':      [round(coords[entry_seg][0],4), round(coords[entry_seg][1],4)],
                    'exit_wp':       [round(coords[exit_seg+1][0],4), round(coords[exit_seg+1][1],4)]
                                     if exit_seg + 1 < len(coords) else None,
                })

            findings.append(finding)

        triggered_list = [f for f in findings if f['triggered']]
        r = jsonify({
            'overall_bearing': round(overall_bearing, 1),
            'total_tss_zones': len(TSS_ZONES),
            'zones_triggered': len(triggered_list),
            'findings':        findings,
        })
        r.headers['Access-Control-Allow-Origin'] = '*'
        return r

    except Exception as e:
        log.error('tss-debug error: %s', e)
        return jsonify({"error": str(e)}), 500


# ── TSS INFO ENDPOINT ─────────────────────────────────────────────────────────

@app.route("/api/tss-zones")
def tss_zones_info():
    zones = {k: {
        'name':       v['name'],
        'reference':  v['reference'],
        'trigger_nm': v['trigger_nm'],
        'lanes':      [key for key in v if key not in ('name','reference','trigger_nm')],
    } for k, v in TSS_ZONES.items()}
    r = jsonify({'count': len(zones), 'zones': zones})
    r.headers['Access-Control-Allow-Origin'] = '*'
    return r


# ── MAIN ROUTING ENDPOINTS ────────────────────────────────────────────────────

@app.route("/api/status")
def status():
    r = jsonify({
        "status":  "ready" if ENGINE else "unavailable",
        "engine":  ENGINE or "none",
        "nodes":   len(GRAPH.graph) if ENGINE=='scgraph' else 0,
        "backend": "scgraph (MARNET)" if ENGINE=='scgraph' else
                   ("searoute-py" if ENGINE=='searoute' else "not loaded"),
        "tss_zones":  len(TSS_ZONES),
        "route_map":  "svg (built-in, no dependencies)",
    })
    r.headers["Access-Control-Allow-Origin"] = "*"
    return r

@app.route("/api/route")
def route_api():
    if not ENGINE: return jsonify({"error":"Engine not loaded"}), 503
    try:
        olon=float(request.args["olon"]); olat=float(request.args["olat"])
        dlon=float(request.args["dlon"]); dlat=float(request.args["dlat"])
    except:
        return jsonify({"error":"Required: olon,olat,dlon,dlat"}), 400
    restrictions=[x.strip().lower() for x in
                  request.args.get("avoid","").split(",") if x.strip()]
    result = route_scgraph(olon,olat,dlon,dlat,restrictions) \
             if ENGINE=='scgraph' \
             else route_searoute(olon,olat,dlon,dlat,restrictions)
    r = jsonify(result)
    r.headers["Access-Control-Allow-Origin"] = "*"
    return r, (400 if "error" in result else 200)

@app.route("/")
def index():
    return send_from_directory(Path(__file__).parent, "index.html")

@app.route("/<path:f>")
def static_f(f):
    return send_from_directory(Path(__file__).parent, f)

load_engine()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    log.info("RoutePlannerPro v2.5 — http://localhost:%d", port)
    log.info("CMEMS user: %s", CMEMS_USER or "NOT SET")
    log.info("TSS zones loaded: %d", len(TSS_ZONES))
    log.info("staticmap available: %s", _check_staticmap())
    app.run(host="0.0.0.0", port=port, debug=False)
