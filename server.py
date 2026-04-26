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
    Elements:
      - Wave height gradient fill (PM-estimated, ocean only, land masked)
      - Smooth isobar contour lines (4 hPa interval, colour-coded by pressure)
      - Route overlay as dashed line with departure/arrival markers
      - Vessel position marker (optional)
      - Land fill + coastline outlines
      - Proper Mercator aspect ratio
    """
    if not HAS_SCIPY:
        raise RuntimeError("scipy/matplotlib not installed")

    import numpy as np

    rows, snap_dt = _fetch_mslp_wind_grid(bbox, timestamp_iso)
    if not rows:
        raise RuntimeError(f"No data in DB for timestamp {timestamp_iso} / bbox {bbox}")

    lats, lons, mslp_s, mslp_raw, wsp_s, wsp_raw, wdir_s = _build_regular_grid(rows, bbox)

    nla, nlo = len(lats), len(lons)
    min_lon, min_lat, max_lon, max_lat = bbox
    mid_lat = (min_lat + max_lat) / 2.0

    # ── Figure setup ──────────────────────────────────────────────────────────
    fig_w = width_px / 100.0
    fig_h = height_px / 100.0
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=100)
    fig.patch.set_facecolor('#0c1829')
    ax.set_facecolor('#0c1829')
    ax.set_xlim(min_lon, max_lon)
    ax.set_ylim(min_lat, max_lat)

    # No set_aspect — let the figure dimensions control proportions.
    # set_aspect() with adjustable='box' shrinks the axes leaving grey borders.
    # The figsize (width_px x height_px) already controls the output proportions.

    LON_G, LAT_G = np.meshgrid(lons, lats)

    # ── 1. Build pixel-level land mask using is_land() ─────────────────────────
    # Resolution: one mask pixel per 0.5° grid cell (same as DB grid)
    land_mask_2d = np.zeros((nla, nlo), dtype=bool)
    for i, la in enumerate(lats):
        for j, lo in enumerate(lons):
            # Convert -180..180 lon to 0..360 for is_land
            lo360 = lo + 360 if lo < 0 else lo
            land_mask_2d[i, j] = _is_land_simple(float(la), float(lo360))
    ocean_mask_2d = ~land_mask_2d

    # ── 2. Wave height fill — ocean only, land = NaN ─────────────────────────
    vms = wsp_s * 0.5144  # kts to m/s for PM formula
    wave_est = np.clip(0.0248 * vms ** 2, 0, 6)
    wave_ocean = np.where(ocean_mask_2d, wave_est, np.nan)

    cmap = _WAVE_CMAP.copy()
    cmap.set_bad('#1a2e44')  # land colour for NaN

    # Use imshow for smooth rendering (flip vertically: imshow origin='lower')
    im = ax.imshow(
        wave_ocean,
        extent=[min_lon, max_lon, min_lat, max_lat],
        origin='lower', aspect='auto',
        cmap=cmap, vmin=0, vmax=6,
        interpolation='bilinear', zorder=1, alpha=0.9
    )

    # ── 3. Land colour overlay ────────────────────────────────────────────────
    land_colour = np.where(land_mask_2d[:, :, np.newaxis],
                           np.array([26, 46, 68], dtype=np.uint8),
                           0).astype(np.uint8)
    alpha_land = (land_mask_2d * 255).astype(np.uint8)
    land_rgba = np.dstack([land_colour, alpha_land])
    ax.imshow(land_rgba, extent=[min_lon, max_lon, min_lat, max_lat],
              origin='lower', aspect='auto', zorder=2, interpolation='nearest')

    # ── 4. Coastline outlines ─────────────────────────────────────────────────
    coastlines = _load_coastlines()
    for line in coastlines:
        xs, ys = [], []
        for lo, la in line:
            if min_lon - 5 <= lo <= max_lon + 5 and min_lat - 3 <= la <= max_lat + 3:
                xs.append(lo); ys.append(la)
        if len(xs) >= 2:
            ax.plot(xs, ys, color='#4a7aa0', linewidth=0.7, zorder=3, solid_capstyle='round')

    # ── 5. Isobar contours ────────────────────────────────────────────────────
    mslp_data = mslp_raw[~np.isnan(mslp_raw)]
    if len(mslp_data) >= 4:
        p_min = int(math.floor(np.min(mslp_data) / 4) * 4)
        p_max = int(math.ceil( np.max(mslp_data) / 4) * 4)
        levels = list(range(p_min, p_max + 4, 4))

        for level in levels:
            if   level <= 996:  col, lw = '#ff2244', 1.3
            elif level <= 1004: col, lw = '#ff7799', 0.9
            elif level <= 1012: col, lw = '#999999', 0.7
            elif level <= 1020: col, lw = '#666666', 0.8
            else:               col, lw = '#444444', 1.0
            try:
                cs = ax.contour(LON_G, LAT_G, mslp_s,
                                levels=[level], colors=[col],
                                linewidths=lw, zorder=4)
                if level % 8 == 0:
                    ax.clabel(cs, fmt='%d', fontsize=7.5, colors=[col],
                              inline=True, inline_spacing=3)
            except Exception:
                pass

    # ── 6. Route overlay ──────────────────────────────────────────────────────
    if waypoints and len(waypoints) >= 2:
        # Subsample for display clarity
        step_wp = max(1, len(waypoints) // 80)
        disp_wps = waypoints[::step_wp]
        if waypoints[-1] not in disp_wps:
            disp_wps = list(disp_wps) + [waypoints[-1]]

        rlo = [normLon180(p[0]) for p in disp_wps]
        rla = [p[1] for p in disp_wps]

        ax.plot(rlo, rla, color='#00ff9d', linewidth=2.0,
                linestyle='--', dashes=(10, 6),
                zorder=6, alpha=0.95, solid_capstyle='round',
                marker=None)

        # Departure: green circle
        ax.plot(rlo[0], rla[0], 'o', color='#00ff9d', markersize=9,
                zorder=7, markeredgecolor='white', markeredgewidth=1.5)
        # Destination: red circle
        ax.plot(rlo[-1], rla[-1], 'o', color='#ff4466', markersize=9,
                zorder=7, markeredgecolor='white', markeredgewidth=1.5)

    # ── 8. Vessel position ────────────────────────────────────────────────────
    if vessel_pos:
        vlo, vla = normLon180(vessel_pos[0]), vessel_pos[1]
        ax.plot(vlo, vla, 'D', color='#ffdd00', markersize=10, zorder=8,
                markeredgecolor='#0c1829', markeredgewidth=1.5)

    # ── 9. Graticule ──────────────────────────────────────────────────────────
    lon_span = max_lon - min_lon
    lat_span = max_lat - min_lat
    lon_step = 30 if lon_span > 90 else 20 if lon_span > 50 else 10
    lat_step = 20 if lat_span > 50 else 10 if lat_span > 25 else 5

    for glo in range(int(math.ceil(min_lon / lon_step)) * lon_step,
                     int(math.floor(max_lon / lon_step)) * lon_step + 1, lon_step):
        if min_lon <= glo <= max_lon:
            ax.axvline(glo, color='#1e3050', linewidth=0.4, zorder=2)
            lbl = f"{abs(glo)}{'W' if glo < 0 else 'E'}"
            ax.text(glo, min_lat + (lat_span * 0.02), lbl,
                    fontsize=6.5, color='#4a6a8a', ha='center', va='bottom', zorder=9)

    for gla in range(int(math.ceil(min_lat / lat_step)) * lat_step,
                     int(math.floor(max_lat / lat_step)) * lat_step + 1, lat_step):
        if min_lat <= gla <= max_lat:
            ax.axhline(gla, color='#1e3050', linewidth=0.4, zorder=2)
            lbl = f"{abs(gla)}{'S' if gla < 0 else 'N'}"
            ax.text(min_lon + (lon_span * 0.01), gla, lbl,
                    fontsize=6.5, color='#4a6a8a', ha='left', va='center', zorder=9)

    # ── 10. Colour bar ────────────────────────────────────────────────────────
    cbar = fig.colorbar(im, ax=ax, orientation='horizontal',
                        pad=0.03, fraction=0.022, aspect=55)
    cbar.set_label('Estimated Wave Height (m)', fontsize=8, color='#94a3b8')
    cbar.ax.tick_params(labelsize=7, colors='#94a3b8')
    cbar.outline.set_edgecolor('#334466')
    cbar.set_ticks([0, 1, 2, 3, 4, 5, 6])

    # ── 11. Title ─────────────────────────────────────────────────────────────
    ts_label = snap_dt.strftime('%d %b %Y, %H:%M UTC')
    title_line1 = 'Sea Condition - Visual overview of Total Wave height, Wind speed and Pressure'
    title_line2 = 'at ' + ts_label + ((' - ' + title_suffix) if title_suffix else '')
    ax.set_title(title_line1 + '\n' + title_line2,
                 fontsize=8.5, color='#94a3b8', pad=6, loc='left')

    ax.tick_params(colors='#4a6a8a', labelsize=7)
    for spine in ax.spines.values():
        spine.set_edgecolor('#1e3050')

    # Remove axes margins/padding so image fills the figure fully
    ax.margins(0)
    plt.subplots_adjust(left=0, right=1, top=0.93, bottom=0.07)

    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight',
                facecolor='#0c1829', edgecolor='none')
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')


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
