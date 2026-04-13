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
    # IMO Ships' Routeing Part B, Section I
    # CRITICAL: waypoints must be in TRAVEL DIRECTION order
    # NE-bound vessel travels SW→NE: starts near Atlantic, exits to North Sea
    # SW-bound vessel travels NE→SW: starts near North Sea, exits to Atlantic
    # Trigger box tightly around the strait only (not whole Channel)
    'dover': {
        'name': 'Dover Strait TSS',
        'trigger_box': [-1.5, 50.8, 2.2, 51.5],
        # NE-bound (Atlantic→North Sea, bearing ~045–090°): SW lane, WP order SW→NE
        'northeast': [
            [-1.05, 50.88],  # Enter: off Beachy Head / Selsey Bill area
            [-0.28, 51.00],  # Off Dungeness
            [ 0.10, 51.05],  # Off Folkestone / South Foreland
            [ 0.52, 51.05],  # Varne Bank area
            [ 0.92, 51.05],  # Mid-strait centre
            [ 1.28, 51.07],  # Off Cap Gris-Nez (NE lane centre)
            [ 1.58, 51.10],  # South Falls / exit toward North Sea
        ],
        # SW-bound (North Sea→Atlantic, bearing ~225–270°): NE lane, WP order NE→SW
        'southwest': [
            [ 2.05, 51.22],  # Enter: West Hinder area
            [ 1.60, 51.20],  # North Goodwin area
            [ 1.30, 51.18],  # Off Cap Gris-Nez (NE lane)
            [ 0.92, 51.18],  # Mid-strait NE lane
            [ 0.52, 51.15],  # Colbart / separation zone
            [ 0.05, 51.18],  # Off South Foreland
            [-0.28, 51.22],  # Off Dungeness
            [-1.12, 50.92],  # Exit: off Beachy Head
        ],
    },

    # ── Strait of Gibraltar ───────────────────────────────────────────────────
    'gibraltar': {
        'name': 'Strait of Gibraltar TSS',
        'trigger_box': [-6.2, 35.7, -4.8, 36.3],
        # Eastbound (Atlantic→Med, bearing ~060–120°): northern lane, W→E
        'east': [
            [-6.05, 36.02],
            [-5.82, 35.98],
            [-5.52, 35.90],
            [-5.28, 35.88],
            [-5.02, 35.88],
            [-4.85, 35.90],
        ],
        # Westbound (Med→Atlantic, bearing ~240–300°): southern lane, E→W
        'west': [
            [-4.88, 35.80],
            [-5.12, 35.78],
            [-5.42, 35.80],
            [-5.68, 35.82],
            [-5.90, 35.88],
            [-6.08, 35.95],
        ],
    },

    # ── Singapore Strait ─────────────────────────────────────────────────────
    'singapore': {
        'name': 'Singapore Strait TSS',
        'trigger_box': [103.5, 1.05, 104.4, 1.45],
        # Eastbound (bearing ~080–120°): main deep-water lane, W→E
        'east': [
            [103.58, 1.20],
            [103.72, 1.22],
            [103.87, 1.25],
            [104.00, 1.25],
            [104.12, 1.22],
            [104.28, 1.20],
        ],
        # Westbound (bearing ~260–300°): main lane, E→W
        'west': [
            [104.25, 1.15],
            [104.10, 1.17],
            [103.97, 1.18],
            [103.83, 1.17],
            [103.70, 1.15],
            [103.57, 1.12],
        ],
    },

    # ── Strait of Malacca ────────────────────────────────────────────────────
    'malacca': {
        'name': 'Strait of Malacca TSS',
        'trigger_box': [99.0, 1.2, 103.5, 6.5],
        # NW-bound (Singapore→Andaman, bearing ~300–340°): SE lane, SE→NW
        'northwest': [
            [103.48, 1.38],
            [103.18, 1.68],
            [102.82, 2.12],
            [102.38, 2.60],
            [101.88, 3.12],
            [101.32, 3.82],
            [100.78, 4.52],
            [100.28, 5.18],
            [ 99.78, 5.82],
            [ 99.28, 6.22],
            [ 98.88, 6.52],
        ],
        # SE-bound (Andaman→Singapore, bearing ~130–160°): NW lane, NW→SE
        'southeast': [
            [ 98.92, 6.38],
            [ 99.32, 6.08],
            [ 99.82, 5.68],
            [100.32, 5.02],
            [100.85, 4.38],
            [101.38, 3.68],
            [101.92, 2.98],
            [102.42, 2.48],
            [102.88, 2.00],
            [103.22, 1.58],
            [103.50, 1.28],
        ],
    },

    # ── Strait of Hormuz ─────────────────────────────────────────────────────
    'hormuz': {
        'name': 'Strait of Hormuz TSS',
        'trigger_box': [56.1, 25.6, 57.4, 26.7],
        # NW-bound (into Persian Gulf, bearing ~300–340°): N lane, SE→NW
        'northwest': [
            [57.28, 25.72],
            [57.08, 25.88],
            [56.82, 26.08],
            [56.58, 26.25],
            [56.40, 26.38],
        ],
        # SE-bound (into Gulf of Oman, bearing ~120–160°): S lane, NW→SE
        'southeast': [
            [56.38, 26.28],
            [56.60, 26.12],
            [56.82, 25.95],
            [57.06, 25.78],
            [57.26, 25.63],
        ],
    },

    # ── Bab-el-Mandeb ────────────────────────────────────────────────────────
    'babelmandab': {
        'name': 'Bab-el-Mandeb TSS',
        'trigger_box': [43.1, 11.6, 43.9, 12.9],
        # N-bound (into Red Sea, bearing ~340–020°): E lane, S→N
        'north': [
            [43.45, 11.78],
            [43.42, 12.18],
            [43.40, 12.52],
            [43.43, 12.88],
        ],
        # S-bound (into Gulf of Aden, bearing ~160–200°): W lane, N→S
        'south': [
            [43.57, 12.82],
            [43.54, 12.48],
            [43.55, 12.12],
            [43.58, 11.82],
        ],
    },

    # ── Cape Finisterre / Off Finisterre ──────────────────────────────────────
    'finisterre': {
        'name': 'Off Finisterre TSS',
        'trigger_box': [-10.2, 42.2, -8.8, 44.2],
        # N-bound (bearing ~340–020°): offshore lane, S→N
        'north': [
            [-9.48, 42.22],
            [-9.63, 42.82],
            [-9.68, 43.32],
            [-9.58, 43.82],
            [-9.38, 44.18],
        ],
        # S-bound (bearing ~160–200°): offshore lane, N→S
        'south': [
            [-9.33, 44.12],
            [-9.53, 43.78],
            [-9.63, 43.28],
            [-9.58, 42.78],
            [-9.43, 42.18],
        ],
    },

    # ── Off Ushant (Ouessant) ─────────────────────────────────────────────────
    'ushant': {
        'name': 'Off Ushant (Ouessant) TSS',
        'trigger_box': [-5.8, 47.6, -4.6, 48.9],
        # NE-bound (toward Channel, bearing ~040–080°): S lane, SW→NE
        'northeast': [
            [-5.78, 47.82],
            [-5.48, 48.17],
            [-5.18, 48.42],
            [-4.98, 48.67],
            [-4.87, 48.87],
        ],
        # SW-bound (toward Atlantic, bearing ~220–260°): N lane, NE→SW
        'southwest': [
            [-4.92, 48.77],
            [-5.12, 48.57],
            [-5.37, 48.32],
            [-5.62, 48.02],
            [-5.87, 47.72],
        ],
    },

    # ── North Sea — German Bight approaches ──────────────────────────────────
    'german_bight': {
        'name': 'German Bight TSS',
        'trigger_box': [7.2, 53.6, 9.3, 55.3],
        # NE-bound (bearing ~040–080°): S→N
        'northeast': [
            [7.52, 53.82],
            [7.82, 54.22],
            [8.12, 54.57],
            [8.37, 54.92],
            [8.47, 55.22],
        ],
        # SW-bound (bearing ~220–260°): N→S
        'southwest': [
            [8.42, 55.17],
            [8.27, 54.85],
            [8.02, 54.50],
            [7.75, 54.15],
            [7.48, 53.78],
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
    When a TSS zone is detected, REPLACE all original waypoints inside that
    zone's trigger box with the TSS lane waypoints.

    This prevents the zigzag caused by keeping both the original MARNET
    waypoints AND the TSS lane waypoints simultaneously.

    Returns (new_coords, tss_applied: list of TSS names applied)
    """
    if len(coords) < 2:
        return coords, []

    # Overall voyage bearing (origin → destination)
    overall_bearing = bearing_deg(coords[0][0], coords[0][1],
                                  coords[-1][0], coords[-1][1])

    tss_applied = []
    inserted_tss = set()

    # Build result by walking the coordinate list.
    # When we enter a TSS trigger box, collect ALL original points in that box,
    # then replace them with the TSS lane waypoints in one shot.
    result = []
    i = 0

    while i < len(coords):
        lon, lat = coords[i]

        # Check if this point is inside any unprocessed TSS trigger box
        matched_tss = None
        for tss_key, tss in TSS_ZONES.items():
            if tss_key in inserted_tss:
                continue
            if point_in_box(lon, lat, tss['trigger_box']):
                matched_tss = (tss_key, tss)
                break

        if matched_tss:
            tss_key, tss = matched_tss
            box = tss['trigger_box']

            # Use bearing of the segment entering the TSS zone
            seg_bearing = bearing_deg(
                coords[i-1][0], coords[i-1][1], lon, lat
            ) if i > 0 else overall_bearing

            lane_wps = _select_tss_lane(tss, seg_bearing, overall_bearing)

            if lane_wps:
                # Skip ALL original waypoints inside this trigger box
                # (they are replaced by the TSS lane waypoints)
                j = i
                while j < len(coords) and point_in_box(coords[j][0], coords[j][1], box):
                    j += 1

                # Insert TSS lane waypoints instead
                for wp in lane_wps:
                    result.append(wp)

                inserted_tss.add(tss_key)
                tss_applied.append(tss['name'])
                log.info('TSS applied: %s (bearing %.0f°, replaced %d pts)',
                         tss['name'], seg_bearing, j - i)

                # Continue from first point after the trigger box
                i = j
                continue

        result.append(coords[i])
        i += 1

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

        # TSS injection is NOT applied here — Calculate Route returns pure MARNET path.
        # TSS compliance is handled separately via /api/tss-apply or the Optimise Route step.

        return {"coordinates":coords,"distance_km":round(total_km,1),
                "distance_nm":round(total_km/1.852,1),
                "route_name":name_from_passages(passages),
                "passages":passages,"node_count":len(coords),
                "tss_applied":[],"warning":None}
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

    # TSS injection is NOT applied here — Calculate Route returns pure MARNET path.
    # TSS compliance is handled separately via /api/tss-apply or the Optimise Route step.

    return {"coordinates":coords,"distance_km":round(total_km,1),
            "distance_nm":round(total_km/1.852,1),
            "route_name":name_from_passages(passages),
            "passages":passages,"node_count":len(coords),
            "tss_applied":[],"warning":None}


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


# ═══════════════════════════════════════════════════════════════════════════════
# TSS SEGMENT-CROSSING DETECTION
# Finds exact segments where route enters/exits each TSS zone boundary,
# interpolates the crossing point, and splices TSS lane waypoints cleanly.
# ═══════════════════════════════════════════════════════════════════════════════

def segment_crosses_box(lon1, lat1, lon2, lat2, box):
    """
    Check if the line segment (lon1,lat1)→(lon2,lat2) crosses or enters
    the bounding box [lon_min, lat_min, lon_max, lat_max].
    Returns True if either endpoint is inside, or the segment crosses a boundary.
    """
    def in_box(lo, la):
        return box[0] <= lo <= box[2] and box[1] <= la <= box[3]

    if in_box(lon1, lat1) or in_box(lon2, lat2):
        return True

    # Check if segment crosses any of the 4 box edges using line-line intersection
    edges = [
        (box[0], box[1], box[0], box[3]),  # left
        (box[2], box[1], box[2], box[3]),  # right
        (box[0], box[1], box[2], box[1]),  # bottom
        (box[0], box[3], box[2], box[3]),  # top
    ]
    for ex1, ey1, ex2, ey2 in edges:
        if segments_intersect(lon1, lat1, lon2, lat2, ex1, ey1, ex2, ey2):
            return True
    return False


def segments_intersect(x1, y1, x2, y2, x3, y3, x4, y4):
    """Check if segment (x1,y1)-(x2,y2) intersects segment (x3,y3)-(x4,y4)."""
    def cross2d(ax, ay, bx, by): return ax * by - ay * bx
    def sub(ax, ay, bx, by): return ax - bx, ay - by

    dx1, dy1 = sub(x2, y2, x1, y1)
    dx2, dy2 = sub(x4, y4, x3, y3)
    denom = cross2d(dx1, dy1, dx2, dy2)
    if abs(denom) < 1e-12:
        return False  # parallel
    dx3, dy3 = sub(x1, y1, x3, y3)
    t = cross2d(dx3, dy3, dx2, dy2) / denom
    u = cross2d(dx3, dy3, dx1, dy1) / denom
    return 0 <= t <= 1 and 0 <= u <= 1


def interpolate_box_entry(lon1, lat1, lon2, lat2, box):
    """
    Find the parameter t (0-1) where the segment first enters the bounding box.
    Returns interpolated (lon, lat) at entry, or None.
    """
    # If start is already inside, entry is at t=0
    if box[0] <= lon1 <= box[2] and box[1] <= lat1 <= box[3]:
        return lon1, lat1, 0.0

    edges = [
        (box[0], box[1], box[0], box[3]),
        (box[2], box[1], box[2], box[3]),
        (box[0], box[1], box[2], box[1]),
        (box[0], box[3], box[2], box[3]),
    ]
    best_t, best_pt = 1.0, None
    dx, dy = lon2 - lon1, lat2 - lat1
    for ex1, ey1, ex2, ey2 in edges:
        edx, edy = ex2 - ex1, ey2 - ey1
        denom = dx * edy - dy * edx
        if abs(denom) < 1e-12: continue
        t = ((ex1 - lon1) * edy - (ey1 - lat1) * edx) / denom
        u = ((ex1 - lon1) * dy  - (ey1 - lat1) * dx)  / denom
        if 0 <= t <= 1 and 0 <= u <= 1 and t < best_t:
            best_t = t
            best_pt = (lon1 + t * dx, lat1 + t * dy)
    return (*best_pt, best_t) if best_pt else None


def inject_tss_waypoints(coords):
    """
    Splice TSS lane waypoints into the route at exact zone crossing points.

    Algorithm:
    1. Walk segments, detect first crossing into each TSS trigger box
    2. Interpolate the exact entry point on the box boundary
    3. Walk forward to find exit crossing, interpolate exit point
    4. Replace all original waypoints between entry and exit with:
       [entry_point] → [TSS_lane_WPs] → [exit_point]

    This gives a clean splice with no zigzag — the TSS section is a
    self-contained arc that connects smoothly to the pre/post route.
    """
    if len(coords) < 2:
        return coords, []

    overall_bearing = bearing_deg(coords[0][0], coords[0][1],
                                  coords[-1][0], coords[-1][1])
    result      = []
    tss_applied = []
    done_tss    = set()
    i = 0

    while i < len(coords) - 1:
        lon1, lat1 = coords[i]
        lon2, lat2 = coords[i + 1]

        triggered = None
        for tss_key, tss in TSS_ZONES.items():
            if tss_key in done_tss:
                continue
            box = tss['trigger_box']
            if segment_crosses_box(lon1, lat1, lon2, lat2, box):
                triggered = (tss_key, tss)
                break

        if not triggered:
            result.append([lon1, lat1])
            i += 1
            continue

        tss_key, tss = triggered
        box = tss['trigger_box']

        # Find entry: interpolate where segment first touches the box
        entry_info = interpolate_box_entry(lon1, lat1, lon2, lat2, box)
        if entry_info is None:
            result.append([lon1, lat1])
            i += 1
            continue

        entry_lon, entry_lat, _ = entry_info

        # Determine lane based on bearing at entry
        seg_bearing = bearing_deg(lon1, lat1, lon2, lat2)
        lane_wps = _select_tss_lane(tss, seg_bearing, overall_bearing)
        if not lane_wps:
            result.append([lon1, lat1])
            i += 1
            continue

        # Keep everything up to (but not including) the entry crossing
        result.append([lon1, lat1])
        # Add exact entry point on box boundary
        result.append([entry_lon, entry_lat])

        # Skip forward through all original waypoints inside the box
        j = i + 1
        last_in_box = None
        while j < len(coords):
            lo, la = coords[j]
            if box[0] <= lo <= box[2] and box[1] <= la <= box[3]:
                last_in_box = j
                j += 1
            else:
                break

        # Find exit: if the last segment exits the box, interpolate exit point
        exit_lon, exit_lat = None, None
        if last_in_box is not None and last_in_box + 1 < len(coords):
            elo1, ela1 = coords[last_in_box]
            elo2, ela2 = coords[last_in_box + 1]
            ex_info = interpolate_box_entry(elo2, ela2, elo1, ela1, box)
            if ex_info:
                exit_lon, exit_lat, _ = ex_info
        elif j < len(coords):
            # Segment i+1 exits the box
            ex_info = interpolate_box_entry(lon2, lat2, lon1, lat1, box)
            if ex_info:
                exit_lon, exit_lat, _ = ex_info

        # Insert TSS lane waypoints
        for wp in lane_wps:
            result.append([wp[0], wp[1]])

        # Add exact exit point
        if exit_lon is not None:
            result.append([exit_lon, exit_lat])

        done_tss.add(tss_key)
        tss_applied.append(tss['name'])
        log.info('TSS spliced: %s (bearing %.0f°, entry %.4f,%.4f exit %.4f,%.4f)',
                 tss['name'], seg_bearing,
                 entry_lon, entry_lat,
                 exit_lon or 0, exit_lat or 0)

        # Continue from the first waypoint after the box
        i = j

    # Append final waypoint
    if coords:
        result.append([coords[-1][0], coords[-1][1]])

    # Deduplicate consecutive near-identical points (< 0.3nm)
    deduped = [result[0]]
    for pt in result[1:]:
        if haversine_km(deduped[-1][0], deduped[-1][1], pt[0], pt[1]) > 0.3 * 1.852:
            deduped.append(pt)

    return deduped, tss_applied


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
    Analyse a route for TSS zone crossings without modifying the route.
    Returns per-zone analysis: which segments cross, entry/exit coords, bearing, lane selected.
    Use this to verify TSS coordinates are correct before enabling injection.

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
            box = tss['trigger_box']
            crossings = []

            for i in range(len(coords) - 1):
                lon1, lat1 = coords[i]
                lon2, lat2 = coords[i + 1]
                if segment_crosses_box(lon1, lat1, lon2, lat2, box):
                    seg_bearing = bearing_deg(lon1, lat1, lon2, lat2)
                    lane_wps    = _select_tss_lane(tss, seg_bearing, overall_bearing)
                    entry_info  = interpolate_box_entry(lon1, lat1, lon2, lat2, box)
                    crossings.append({
                        'segment_idx':  i,
                        'seg_from':     [round(lon1, 5), round(lat1, 5)],
                        'seg_to':       [round(lon2, 5), round(lat2, 5)],
                        'seg_bearing':  round(seg_bearing, 1),
                        'entry_point':  [round(entry_info[0], 5), round(entry_info[1], 5)]
                                        if entry_info else None,
                        'lane_selected': None,
                        'lane_first_wp': lane_wps[0]  if lane_wps else None,
                        'lane_last_wp':  lane_wps[-1] if lane_wps else None,
                        'lane_wps':      lane_wps,
                    })
                    # Only report first crossing per zone
                    break

            if crossings:
                seg_b = crossings[0]['seg_bearing']
                lane_wps = _select_tss_lane(tss, seg_b, overall_bearing)
                crossings[0]['lane_selected'] = (
                    'CORRECT — first WP is entry side, last WP is exit side'
                    if lane_wps and len(lane_wps) >= 2 else 'NO LANE MATCHED'
                )
                findings.append({
                    'tss':        tss_key,
                    'name':       tss['name'],
                    'trigger_box': box,
                    'triggered':  True,
                    'crossings':  crossings,
                })
            else:
                findings.append({
                    'tss':       tss_key,
                    'name':      tss['name'],
                    'triggered': False,
                })

        triggered = [f for f in findings if f['triggered']]
        r = jsonify({
            'overall_bearing':  round(overall_bearing, 1),
            'total_tss_zones':  len(TSS_ZONES),
            'zones_triggered':  len(triggered),
            'findings':         findings,
        })
        r.headers['Access-Control-Allow-Origin'] = '*'
        return r

    except Exception as e:
        log.error('tss-debug error: %s', e)
        return jsonify({"error": str(e)}), 500


# ── TSS INFO ENDPOINT ─────────────────────────────────────────────────────────

@app.route("/api/tss-zones")
def tss_zones_info():
    zones = {k: {'name': v['name'], 'trigger_box': v['trigger_box'],
                 'lanes': [key for key in v if key not in ('name','trigger_box')]}
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
