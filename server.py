#!/usr/bin/env python3
"""
RoutePlannerPro — Maritime Routing + Copernicus Marine Weather Proxy
Deploy to Render.com

Environment variables required:
  CMEMS_USER  — Copernicus Marine username (email)
  CMEMS_PASS  — Copernicus Marine password

pip install flask searoute gunicorn requests staticmap Pillow
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
# TRAFFIC SEPARATION SCHEMES (TSS)
# Source: IMO Ships' Routeing, Edition 2023 + UKHO NP136
# Each TSS entry defines:
#   trigger_box : [lon_min, lat_min, lon_max, lat_max]
#     — bounding box that, if the GC route passes through, triggers TSS injection
#   inbound     : [[lon,lat], ...] waypoints for vessels heading roughly E/NE/N
#   outbound    : [[lon,lat], ...] waypoints for vessels heading roughly W/SW/S
#   name        : human-readable name for logging
# The route API checks if any route segment midpoint falls within the trigger_box,
# then determines inbound/outbound based on overall voyage direction.
# ═══════════════════════════════════════════════════════════════════════════════

TSS_ZONES = {

    # ── Dover Strait / English Channel ───────────────────────────────────────
    # IMO Ships' Routeing Part B, Section I — one of the busiest in the world
    # Inbound (westbound, SW): NE lane → SW
    # Outbound (eastbound, NE): SW lane → NE
    'dover': {
        'name': 'Dover Strait TSS',
        'trigger_box': [-2.0, 50.5, 2.5, 51.5],
        # NE-bound (heading toward North Sea): southern lane
        'northeast': [
            [1.55, 51.10],   # South Falls area entry
            [1.25, 51.07],   # Lane centre
            [0.90, 51.05],   # Mid-Channel
            [0.50, 51.05],   # Varne area
            [0.12, 51.08],   # Off Folkestone
            [-0.05, 51.12],  # South Goodwin
            [-0.30, 51.18],  # Off Dungeness
            [-1.20, 50.85],  # Off Beachy Head
        ],
        # SW-bound (heading toward Atlantic): northern lane
        'southwest': [
            [-1.10, 50.92],  # Off Beachy Head inbound
            [-0.25, 51.25],  # North of TSS centre
            [0.08, 51.20],   # Off South Foreland
            [0.55, 51.18],   # Colbart area
            [0.95, 51.18],   # Mid separation zone
            [1.32, 51.20],   # Off Cap Gris-Nez
            [1.62, 51.22],   # North Goodwin
            [2.00, 51.25],   # West Hinder area
        ],
    },

    # ── Strait of Gibraltar ───────────────────────────────────────────────────
    # IMO Ships' Routeing Part B, Section II
    'gibraltar': {
        'name': 'Strait of Gibraltar TSS',
        'trigger_box': [-6.5, 35.5, -4.5, 36.5],
        # Eastbound (into Mediterranean): northern lane
        'east': [
            [-6.00, 36.05],
            [-5.80, 36.00],
            [-5.50, 35.92],
            [-5.25, 35.90],
            [-5.00, 35.88],
            [-4.80, 35.90],
        ],
        # Westbound (into Atlantic): southern lane
        'west': [
            [-4.85, 35.80],
            [-5.10, 35.78],
            [-5.40, 35.80],
            [-5.65, 35.83],
            [-5.88, 35.88],
            [-6.10, 35.92],
        ],
    },

    # ── Singapore Strait ─────────────────────────────────────────────────────
    # IMO Ships' Routeing Part B, Section XIII
    # Critical — compulsory reporting (STRAITREP) and pilotage applies
    'singapore': {
        'name': 'Singapore Strait TSS',
        'trigger_box': [103.5, 1.0, 104.5, 1.5],
        # Eastbound: main lane (deep water)
        'east': [
            [103.55, 1.18],
            [103.70, 1.22],
            [103.85, 1.25],
            [103.98, 1.25],
            [104.10, 1.22],
            [104.25, 1.20],
        ],
        # Westbound: main lane (slightly south)
        'west': [
            [104.22, 1.15],
            [104.08, 1.17],
            [103.95, 1.18],
            [103.82, 1.18],
            [103.68, 1.15],
            [103.55, 1.12],
        ],
    },

    # ── Strait of Malacca ────────────────────────────────────────────────────
    # IMO Ships' Routeing Part B, Section XIII
    # Note: vessels > 250m LOA use VTIS West (Horsburgh to One Fathom Bank)
    'malacca': {
        'name': 'Strait of Malacca TSS',
        'trigger_box': [98.5, 1.0, 104.0, 6.5],
        # Northwestbound (toward Andaman Sea)
        'northwest': [
            [103.48, 1.35],
            [103.20, 1.65],
            [102.85, 2.10],
            [102.40, 2.58],
            [101.90, 3.10],
            [101.35, 3.80],
            [100.80, 4.50],
            [100.30, 5.15],
            [99.80,  5.80],
            [99.30,  6.20],
            [98.85,  6.50],
        ],
        # Southeastbound (toward Singapore)
        'southeast': [
            [98.90,  6.35],
            [99.35,  6.05],
            [99.85,  5.65],
            [100.35, 5.00],
            [100.88, 4.35],
            [101.40, 3.65],
            [101.95, 2.95],
            [102.45, 2.45],
            [102.92, 1.97],
            [103.25, 1.55],
            [103.52, 1.25],
        ],
    },

    # ── Strait of Hormuz ─────────────────────────────────────────────────────
    # IMO Ships' Routeing Part B, Section IX
    'hormuz': {
        'name': 'Strait of Hormuz TSS',
        'trigger_box': [56.0, 25.5, 57.5, 26.8],
        # Inbound (NW, into Persian Gulf): northern lane
        'northwest': [
            [57.30, 25.75],
            [57.10, 25.90],
            [56.85, 26.10],
            [56.60, 26.28],
            [56.40, 26.40],
        ],
        # Outbound (SE, into Gulf of Oman): southern lane
        'southeast': [
            [56.35, 26.28],
            [56.58, 26.15],
            [56.80, 25.98],
            [57.05, 25.80],
            [57.28, 25.65],
        ],
    },

    # ── Bab-el-Mandeb ────────────────────────────────────────────────────────
    # IMO Ships' Routeing Part B, Section X
    'babelmandab': {
        'name': 'Bab-el-Mandeb TSS',
        'trigger_box': [43.0, 11.5, 44.0, 13.0],
        # Northbound (into Red Sea)
        'north': [
            [43.45, 11.75],
            [43.40, 12.15],
            [43.38, 12.50],
            [43.42, 12.85],
        ],
        # Southbound (into Gulf of Aden)
        'south': [
            [43.55, 12.80],
            [43.52, 12.45],
            [43.55, 12.10],
            [43.58, 11.80],
        ],
    },

    # ── Cape Finisterre / Off Finisterre ──────────────────────────────────────
    # IMO Ships' Routeing Part B, Section VI
    'finisterre': {
        'name': 'Off Finisterre TSS',
        'trigger_box': [-10.5, 42.0, -8.5, 44.5],
        # Northbound: offshore lane
        'north': [
            [-9.50, 42.20],
            [-9.65, 42.80],
            [-9.70, 43.30],
            [-9.60, 43.80],
            [-9.40, 44.20],
        ],
        # Southbound: offshore lane
        'south': [
            [-9.35, 44.15],
            [-9.55, 43.75],
            [-9.65, 43.25],
            [-9.60, 42.75],
            [-9.45, 42.15],
        ],
    },

    # ── Off Ushant (Ouessant) ─────────────────────────────────────────────────
    # IMO Ships' Routeing Part B, Section IV — major Atlantic/Channel junction
    'ushant': {
        'name': 'Off Ushant (Ouessant) TSS',
        'trigger_box': [-6.0, 47.5, -4.5, 49.0],
        # Northeastbound (toward Channel)
        'northeast': [
            [-5.80, 47.80],
            [-5.50, 48.15],
            [-5.20, 48.40],
            [-5.00, 48.65],
            [-4.85, 48.85],
        ],
        # Southwestbound (toward Atlantic)
        'southwest': [
            [-4.90, 48.75],
            [-5.10, 48.55],
            [-5.35, 48.30],
            [-5.60, 48.00],
            [-5.85, 47.70],
        ],
    },

    # ── Off Île d'Ouessant (Deep Water Route) ────────────────────────────────
    # For VLCC/large vessels using the deep water route west of Ushant
    'ushant_dw': {
        'name': 'Ushant Deep Water Route',
        'trigger_box': [-7.5, 47.5, -5.5, 48.5],
        'northeast': [
            [-7.20, 47.65],
            [-6.80, 47.95],
            [-6.40, 48.20],
            [-6.00, 48.40],
        ],
        'southwest': [
            [-6.05, 48.35],
            [-6.45, 48.12],
            [-6.85, 47.88],
            [-7.25, 47.58],
        ],
    },

    # ── North Sea — German Bight / Elbe approaches ───────────────────────────
    # For vessels routing to Hamburg/Bremen/Dutch ports
    'german_bight': {
        'name': 'German Bight / Elbe TSS',
        'trigger_box': [7.0, 53.5, 9.5, 55.5],
        'northeast': [
            [7.50, 53.80],
            [7.80, 54.20],
            [8.10, 54.55],
            [8.35, 54.90],
            [8.45, 55.20],
        ],
        'southwest': [
            [8.40, 55.15],
            [8.25, 54.82],
            [8.00, 54.48],
            [7.72, 54.12],
            [7.45, 53.75],
        ],
    },
}


def point_in_box(lon, lat, box):
    """Check if [lon, lat] is within bounding box [lon_min, lat_min, lon_max, lat_max]."""
    return box[0] <= lon <= box[2] and box[1] <= lat <= box[3]


def bearing_deg(lon1, lat1, lon2, lat2):
    """Initial bearing from point 1 to point 2, degrees true."""
    r = math.pi / 180
    dlon = (lon2 - lon1) * r
    lat1r, lat2r = lat1 * r, lat2 * r
    x = math.sin(dlon) * math.cos(lat2r)
    y = math.cos(lat1r) * math.sin(lat2r) - math.sin(lat1r) * math.cos(lat2r) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def inject_tss_waypoints(coords):
    """
    Check each route segment against TSS trigger boxes.
    If a segment passes through a TSS zone, inject the appropriate
    inbound/outbound TSS lane waypoints at that point in the route.

    Returns (new_coords, tss_applied: list of TSS names applied)
    """
    if len(coords) < 2:
        return coords, []

    # Overall voyage bearing (origin → destination)
    overall_bearing = bearing_deg(coords[0][0], coords[0][1],
                                  coords[-1][0], coords[-1][1])

    result = [coords[0]]
    tss_applied = []
    inserted_tss = set()  # prevent double-insertion

    for i in range(len(coords) - 1):
        lon1, lat1 = coords[i]
        lon2, lat2 = coords[i+1]
        mid_lon = (lon1 + lon2) / 2
        mid_lat = (lat1 + lat2) / 2

        seg_bearing = bearing_deg(lon1, lat1, lon2, lat2)

        for tss_key, tss in TSS_ZONES.items():
            if tss_key in inserted_tss:
                continue
            box = tss['trigger_box']

            # Check if midpoint OR either endpoint falls in the trigger box
            if (point_in_box(mid_lon, mid_lat, box) or
                point_in_box(lon1, lat1, box) or
                point_in_box(lon2, lat2, box)):

                # Determine which lane to use based on segment bearing
                lane_wps = _select_tss_lane(tss, seg_bearing, overall_bearing)

                if lane_wps:
                    # Insert TSS waypoints before the current endpoint
                    for wp in lane_wps:
                        result.append(wp)
                    inserted_tss.add(tss_key)
                    tss_applied.append(tss['name'])
                    log.info('TSS applied: %s (bearing %.0f°)', tss['name'], seg_bearing)
                    break

        result.append(coords[i+1])

    # Deduplicate consecutive near-identical points (< 0.5nm apart)
    deduped = [result[0]]
    for pt in result[1:]:
        d = haversine_km(deduped[-1][0], deduped[-1][1], pt[0], pt[1])
        if d > 0.5 * 1.852:  # 0.5nm in km
            deduped.append(pt)

    return deduped, tss_applied


def _select_tss_lane(tss, seg_bearing, overall_bearing):
    """
    Select the correct TSS lane based on vessel bearing.
    Returns list of [lon, lat] waypoints for the appropriate lane, or None.
    """
    keys = [k for k in tss.keys() if k not in ('name', 'trigger_box')]
    if not keys:
        return None

    # Map cardinal/ordinal direction keys to bearing ranges
    direction_map = {
        'northeast':  (22.5,  112.5),
        'east':       (67.5,  112.5),
        'southeast':  (112.5, 202.5),
        'south':      (157.5, 202.5),
        'southwest':  (202.5, 292.5),
        'west':       (247.5, 292.5),
        'northwest':  (292.5, 382.5),  # wraps 360→22.5
        'north':      (337.5, 382.5),  # wraps
    }

    def bearing_matches(bearing, range_min, range_max):
        b = bearing % 360
        if range_max > 360:
            return b >= range_min or b < (range_max - 360)
        return range_min <= b < range_max

    # Try segment bearing first, fall back to overall voyage bearing
    for use_bearing in [seg_bearing, overall_bearing]:
        for key in keys:
            if key in direction_map:
                rmin, rmax = direction_map[key]
                if bearing_matches(use_bearing, rmin, rmax):
                    return tss[key]

    # If no directional match, return the first available lane
    return tss[keys[0]]


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
        total_km=result.get('length',0)
        passages=detect_passages(coords)

        # Apply TSS waypoint injection
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

    # Apply TSS waypoint injection
    coords, tss_applied = inject_tss_waypoints(coords)
    total_km = sum(haversine_km(coords[i][0],coords[i][1],coords[i+1][0],coords[i+1][1])
                   for i in range(len(coords)-1))

    return {"coordinates":coords,"distance_km":round(total_km,1),
            "distance_nm":round(total_km/1.852,1),
            "route_name":name_from_passages(passages),
            "passages":passages,"node_count":len(coords),
            "tss_applied":tss_applied,"warning":None}


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTE MAP IMAGE GENERATION
# Uses staticmap library to render OpenStreetMap/CARTO tiles + route polyline.
# Returns PNG as base64 for embedding in PDF.
# ═══════════════════════════════════════════════════════════════════════════════

def rdp_simplify(points, epsilon=0.15):
    """
    Ramer-Douglas-Peucker polyline simplification.
    Reduces waypoint count for map rendering without visible quality loss.
    epsilon in degrees (~15nm at mid-latitudes).
    """
    if len(points) <= 2:
        return points

    def point_line_dist(p, a, b):
        """Perpendicular distance from point p to line a–b."""
        if a == b:
            return math.hypot(p[0]-a[0], p[1]-a[1])
        dx, dy = b[0]-a[0], b[1]-a[1]
        t = ((p[0]-a[0])*dx + (p[1]-a[1])*dy) / (dx*dx + dy*dy)
        t = max(0, min(1, t))
        return math.hypot(p[0]-(a[0]+t*dx), p[1]-(a[1]+t*dy))

    dmax, idx = 0, 0
    for i in range(1, len(points)-1):
        d = point_line_dist(points[i], points[0], points[-1])
        if d > dmax:
            dmax, idx = d, i

    if dmax > epsilon:
        left  = rdp_simplify(points[:idx+1], epsilon)
        right = rdp_simplify(points[idx:],   epsilon)
        return left[:-1] + right
    return [points[0], points[-1]]


def generate_route_map(waypoints, width=1200, height=600):
    """
    Generate a PNG map image of the route using staticmap + CARTO Dark tiles.
    waypoints: [[lon, lat], ...]
    Returns PNG bytes, or None on failure.

    Tile source: CARTO Dark Matter (free, no API key needed, ~5nm resolution at z=4)
    """
    try:
        from staticmap import StaticMap, Line, CircleMarker
    except ImportError:
        log.error('staticmap not installed — pip install staticmap')
        return None

    if len(waypoints) < 2:
        return None

    # Simplify long routes — reduces tile requests, speeds render
    # Keep first/last always; simplify intermediates
    simplified = rdp_simplify(waypoints, epsilon=0.3)
    if len(simplified) < 2:
        simplified = [waypoints[0], waypoints[-1]]

    # Handle antimeridian crossing — split into segments if needed
    # (staticmap doesn't handle 180° wrapping natively)
    segments = []
    seg = [simplified[0]]
    for pt in simplified[1:]:
        if abs(pt[0] - seg[-1][0]) > 180:
            # Antimeridian crossing detected — start new segment
            segments.append(seg)
            seg = [pt]
        else:
            seg.append(pt)
    segments.append(seg)

    # Compute bounding box with 8% padding
    all_lons = [p[0] for p in simplified]
    all_lats = [p[1] for p in simplified]
    lon_span = max(all_lons) - min(all_lons)
    lat_span = max(all_lats) - min(all_lats)
    pad_lon  = max(lon_span * 0.12, 3.0)  # at least 3° padding
    pad_lat  = max(lat_span * 0.12, 2.0)

    # CARTO Dark Matter — elegant dark basemap, no API key, reliable CDN
    # Fallback: OpenStreetMap (lighter)
    tile_url = 'https://cartodb-basemaps-{s}.global.ssl.fastly.net/dark_matter_all/{z}/{x}/{y}.png'

    m = StaticMap(width, height, url_template=tile_url,
                  tile_request_timeout=8,
                  headers={'User-Agent': 'RoutePlannerPro/2.5 (voyage planning tool)'})

    # Add route line segments
    ROUTE_COLOR  = '#3b82f6'   # blue route line
    ROUTE_WIDTH  = 3
    for seg in segments:
        if len(seg) >= 2:
            # staticmap expects [(lon,lat), ...] as (x,y)
            line_coords = [(p[0], p[1]) for p in seg]
            m.add_line(Line(line_coords, ROUTE_COLOR, ROUTE_WIDTH))

    # Origin marker (green)
    m.add_marker(CircleMarker(
        (simplified[0][0], simplified[0][1]),
        '#22c55e', 10
    ))
    # Destination marker (red)
    m.add_marker(CircleMarker(
        (simplified[-1][0], simplified[-1][1]),
        '#ef4444', 10
    ))

    # Render — may take 3–8s depending on tile server latency
    try:
        img = m.render(zoom=None)  # auto-zoom to fit all points
        buf = io.BytesIO()
        img.save(buf, format='PNG', optimize=True)
        return buf.getvalue()
    except Exception as e:
        log.warning('staticmap render failed, trying fallback tile server: %s', e)
        # Fallback to OpenStreetMap
        try:
            m2 = StaticMap(width, height,
                           url_template='https://tile.openstreetmap.org/{z}/{x}/{y}.png',
                           tile_request_timeout=8,
                           headers={'User-Agent': 'RoutePlannerPro/2.5'})
            for seg in segments:
                if len(seg) >= 2:
                    m2.add_line(Line([(p[0],p[1]) for p in seg], '#2563eb', ROUTE_WIDTH))
            m2.add_marker(CircleMarker((simplified[0][0],  simplified[0][1]),  '#16a34a', 10))
            m2.add_marker(CircleMarker((simplified[-1][0], simplified[-1][1]), '#dc2626', 10))
            img2 = m2.render(zoom=None)
            buf2 = io.BytesIO()
            img2.save(buf2, format='PNG', optimize=True)
            return buf2.getvalue()
        except Exception as e2:
            log.error('Both tile sources failed: %s', e2)
            return None


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


# ── ROUTE MAP IMAGE ENDPOINT ──────────────────────────────────────────────────

@app.route("/api/route-map", methods=["POST"])
def route_map():
    """
    Generate a PNG map image of the route for PDF embedding.
    Body: {"waypoints": [[lon, lat], ...], "width": 1200, "height": 600}
    Returns: {"image_b64": "<base64 PNG>", "format": "png"}
    """
    try:
        data = request.get_json(force=True)
        waypoints = data.get('waypoints', [])
        width     = int(data.get('width',  1200))
        height    = int(data.get('height',  550))

        if len(waypoints) < 2:
            return jsonify({"error": "Need at least 2 waypoints"}), 400

        # Cap dimensions for safety
        width  = min(width,  2400)
        height = min(height, 1200)

        png_bytes = generate_route_map(waypoints, width, height)

        if png_bytes is None:
            return jsonify({"error": "Map generation failed — staticmap not available"}), 500

        image_b64 = base64.b64encode(png_bytes).decode('utf-8')
        r = jsonify({"image_b64": image_b64, "format": "png",
                     "width": width, "height": height})
        r.headers['Access-Control-Allow-Origin'] = '*'
        return r

    except Exception as e:
        log.error('route-map error: %s', e)
        return jsonify({"error": str(e)}), 500


# ── TSS INFO ENDPOINT ─────────────────────────────────────────────────────────

@app.route("/api/tss-zones")
def tss_zones_info():
    """Return TSS zone metadata (for frontend display/debugging)."""
    zones = {k: {'name': v['name'], 'trigger_box': v['trigger_box']}
             for k, v in TSS_ZONES.items()}
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
        "tss_zones": len(TSS_ZONES),
        "route_map": "available" if _check_staticmap() else "unavailable (pip install staticmap)",
    })
    r.headers["Access-Control-Allow-Origin"] = "*"
    return r

def _check_staticmap():
    try:
        import staticmap
        return True
    except ImportError:
        return False

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
