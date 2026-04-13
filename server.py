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
# ═══════════════════════════════════════════════════════════════════════════════
# TRAFFIC SEPARATION SCHEMES (TSS)
# Source: IMO Ships Routeing (official PDF, boundary coords converted to centrelines)
#
# Lane waypoints = centrelines computed as midpoints of IMO boundary pairs:
#   - separation zone centreline (from IMO boundary coordinates)
#   - outer lane boundary (from IMO boundary coordinates)
# Coordinates: decimal degrees, converted from degrees-minutes in IMO document.
#
# PROXIMITY-BASED DETECTION:
#   reference  : [lon, lat] strait midpoint
#   trigger_nm : inject if route passes within this many nm of reference
# ═══════════════════════════════════════════════════════════════════════════════

TSS_ZONES = {

    # ── Dover Strait ──────────────────────────────────────────────────────────
    # IMO Ships Routeing Part B, Section II/11
    # NE lane: midpoint(outer S boundary pts 33-48, sep zone N side pts 1-13)
    # SW lane: midpoint(outer N boundary pts 22-26, sep zone S side pts 1-13)
    'dover': {
        'name':       'Dover Strait TSS',
        'reference':  [1.20, 51.00],
        'trigger_nm': 40,
        # NE-bound (Atlantic->North Sea, SW->NE)
        'northeast': [
            [ 0.725, 50.613],  # entry W of Dungeness
            [ 1.199, 50.696],  # approaching mid-strait
            [ 1.508, 50.878],  # Varne Bank area
            [ 1.662, 51.008],  # mid-strait
            [ 1.744, 51.130],  # NE section
            [ 1.913, 51.249],  # approaching North Sea
            [ 2.094, 51.327],  # exit toward North Sea
        ],
        # SW-bound (North Sea->Atlantic, NE->SW)
        'southwest': [
            [ 2.059, 51.487],  # entry from NE
            [ 1.946, 51.405],
            [ 1.861, 51.309],
            [ 1.577, 51.108],
            [ 1.220, 50.912],  # exit toward Atlantic
        ],
    },

    # ── Strait of Gibraltar ───────────────────────────────────────────────────
    # IMO Ships Routeing Part B, Section III/1
    # Sep zone centred pts 1-3; WB outer pts 4-6; EB outer pts 7-9
    'gibraltar': {
        'name':       'Strait of Gibraltar TSS',
        'reference':  [-5.59, 35.96],
        'trigger_nm': 30,
        # Eastbound (Atlantic->Med, W->E): midpoint(sep_zone, EB outer S boundary)
        'east': [
            [-5.748, 35.907],  # W entry
            [-5.607, 35.918],  # mid
            [-5.427, 35.967],  # E exit
        ],
        # Westbound (Med->Atlantic, E->W): midpoint(sep_zone, WB outer N boundary)
        'west': [
            [-5.427, 36.003],  # E entry
            [-5.607, 35.957],  # mid
            [-5.748, 35.957],  # W exit
        ],
    },

    # ── Bab el Mandeb ─────────────────────────────────────────────────────────
    # IMO Ships Routeing Part B, Section IV/1
    # Sep zone pts 1-6; SB outer pts 7-9; NB outer pts 10-12
    'babelmandab': {
        'name':       'Bab-el-Mandeb TSS',
        'reference':  [43.35, 12.63],
        'trigger_nm': 25,
        # Northbound (Gulf of Aden->Red Sea, S->N)
        # midpoint(sep_zone E side, NB outer E boundary)
        'north': [
            [43.234, 12.933],  # S entry (narrow end)
            [43.354, 12.628],  # mid
            [43.474, 12.560],  # N exit
        ],
        # Southbound (Red Sea->Gulf of Aden, N->S)
        # midpoint(sep_zone W side, SB outer W boundary)
        'south': [
            [43.202, 12.921],  # N entry
            [43.327, 12.605],  # mid
            [43.457, 12.532],  # S exit
        ],
    },

    # ── Strait of Hormuz ──────────────────────────────────────────────────────
    # IMO Ships Routeing Part B, Section IV/3
    # Sep zone pts 1-8; EB outer pts 9-12; WB outer pts 13-16
    'hormuz': {
        'name':       'Strait of Hormuz TSS',
        'reference':  [56.51, 26.56],
        'trigger_nm': 30,
        # Southeast-bound (out of Persian Gulf, NW->SE)
        # midpoint(sep_zone S side, EB outer S boundary)
        'southeast': [
            [56.369, 26.542],  # NW entry
            [56.428, 26.538],
            [56.456, 26.538],
            [56.626, 26.477],  # SE exit
        ],
        # Northwest-bound (into Persian Gulf, SE->NW)
        # midpoint(sep_zone N side, WB outer N boundary)
        'northwest': [
            [56.673, 26.503],  # SE entry
            [56.592, 26.625],
            [56.465, 26.625],
            [56.343, 26.596],  # NW exit
        ],
    },

    # ── Singapore Strait ──────────────────────────────────────────────────────
    # IMO Ships Routeing Part B, Section V/3
    # Sep zone pts 1-19; WB outer (N) pts 20-29; EB outer (S) pts 30-38
    'singapore': {
        'name':       'Singapore Strait TSS',
        'reference':  [103.73, 1.20],
        'trigger_nm': 30,
        # Eastbound (W->E): midpoint(sep_zone S side, EB outer S boundary)
        'east': [
            [103.199, 1.381],  # W entry
            [103.469, 1.184],
            [103.727, 1.104],  # south of Singapore
            [103.839, 1.179],
            [104.058, 1.243],  # E exit
        ],
        # Westbound (E->W): midpoint(sep_zone N side, WB outer N boundary)
        'west': [
            [104.055, 1.265],  # E entry
            [103.860, 1.200],
            [103.733, 1.135],  # south of Singapore
            [103.529, 1.206],
            [103.232, 1.410],  # W exit
        ],
    },

    # ── Strait of Malacca ─────────────────────────────────────────────────────
    # IMO Ships Routeing Part B, Section V (series of sub-schemes)
    # Centreline follows the deep-water route corridor
    'malacca': {
        'name':       'Strait of Malacca TSS',
        'reference':  [101.35, 3.50],
        'trigger_nm': 60,
        # NW-bound (Singapore->Andaman Sea, SE->NW)
        'northwest': [
            [103.500, 1.350],
            [103.000, 1.750],
            [102.500, 2.300],
            [102.000, 2.950],
            [101.350, 3.500],
            [100.750, 4.300],
            [100.200, 5.100],
            [ 99.750, 5.850],
            [ 99.200, 6.300],
            [ 98.900, 6.500],
        ],
        # SE-bound (Andaman Sea->Singapore, NW->SE)
        'southeast': [
            [ 98.900, 6.450],
            [ 99.250, 6.150],
            [ 99.800, 5.700],
            [100.250, 5.050],
            [100.800, 4.250],
            [101.400, 3.450],
            [102.050, 2.900],
            [102.550, 2.250],
            [103.050, 1.700],
            [103.500, 1.300],
        ],
    },

    # ── Off Ushant (Ouessant) ─────────────────────────────────────────────────
    # IMO Ships Routeing Part B, Section II/4
    # Sep zone A (inshore) centred pts 1-3; Sep zone B (offshore) centred pts 4-6
    'ushant': {
        'name':       'Off Ushant (Ouessant) TSS',
        'reference':  [-5.42, 48.60],
        'trigger_nm': 35,
        # NE-bound (toward Channel, SW->NE): inshore lane, ~2nm SE of zone A
        'northeast': [
            [-5.412, 48.476],
            [-5.337, 48.570],
            [-5.203, 48.642],
        ],
        # SW-bound (toward Atlantic, NE->SW): midpoint(zone A, zone B)
        'southwest': [
            [-5.328, 48.739],
            [-5.484, 48.655],
            [-5.575, 48.542],
        ],
    },

    # ── Off Cape Finisterre ───────────────────────────────────────────────────
    # IMO Ships Routeing Part B, Section II/7
    # Sep zone A pts 1-6; Sep zone B pts 7-12; SB outer pts 13-15
    'finisterre': {
        'name':       'Off Finisterre TSS',
        'reference':  [-9.81, 43.13],
        'trigger_nm': 35,
        # Northbound: midpoint(sep zone A, sep zone B), S->N
        'north': [
            [-9.806, 42.882],
            [-9.806, 43.198],
            [-9.679, 43.377],
        ],
        # Southbound: midpoint(sep zone B, outer), N->S
        'south': [
            [-9.795, 43.420],
            [-9.926, 43.230],
            [-9.926, 42.882],
        ],
    },

    # ── German Bight ──────────────────────────────────────────────────────────
    # IMO Ships Routeing Part B, Section II (Terschelling-German Bight)
    'german_bight': {
        'name':       'German Bight TSS',
        'reference':  [8.12, 54.55],
        'trigger_nm': 35,
        'northeast': [
            [7.52, 53.82],
            [7.82, 54.22],
            [8.12, 54.57],
            [8.37, 54.92],
            [8.47, 55.22],
        ],
        'southwest': [
            [8.42, 55.17],
            [8.27, 54.85],
            [8.02, 54.50],
            [7.75, 54.15],
            [7.48, 53.78],
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


def _select_tss_lane(tss, seg_bearing, overall_bearing):
    """
    Select the correct TSS lane based on vessel bearing.
    Lane keys are cardinal/ordinal direction names matching the vessel's heading.
    """
    keys = [k for k in tss.keys() if k not in ('name', 'reference', 'trigger_nm',
                                                 'trigger_box')]  # trigger_box kept for compat
    if not keys:
        return None

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

    def matches(bearing, rmin, rmax):
        b = bearing % 360
        return b >= rmin or b < (rmax - 360) if rmax > 360 else rmin <= b < rmax

    for use_bearing in [seg_bearing, overall_bearing]:
        for key in keys:
            if key in direction_map:
                rmin, rmax = direction_map[key]
                if matches(use_bearing, rmin, rmax):
                    return tss[key]

    return tss[keys[0]]  # fallback: first lane


def inject_tss_waypoints(coords):
    """
    Proximity-based TSS injection.

    For each TSS zone:
      1. Find closest approach to reference point. If > trigger_nm, skip.
      2. Select lane from bearing at closest segment.
      3. Remove all original coords within a removal_nm radius of the reference.
         These are the coords "inside" the strait that the lane replaces.
      4. Insert the lane waypoints at that position.

    This is the simplest correct approach:
    - No entry/exit segment hunting (which caused backward steps)
    - No box intersections (which had null entry points)
    - Just: find the strait coords, remove them, insert correct lane
    """
    if len(coords) < 2:
        return coords, []

    overall_bearing = bearing_deg(coords[0][0], coords[0][1],
                                  coords[-1][0], coords[-1][1])
    tss_applied = []
    splices = []  # (first_remove_idx, last_remove_idx, lane_wps, name)

    for tss_key, tss in TSS_ZONES.items():
        ref_lon, ref_lat = tss['reference']
        trigger_nm       = tss['trigger_nm']
        # Removal radius: coords within this distance of reference get replaced
        # Use 60% of trigger_nm so we don't remove too aggressively
        removal_nm = trigger_nm * 0.6

        # Step 1: find segment of closest approach
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
            continue

        # Step 2: lane selection
        seg_bearing = bearing_deg(coords[closest_seg][0], coords[closest_seg][1],
                                  coords[closest_seg+1][0], coords[closest_seg+1][1])
        lane_wps = _select_tss_lane(tss, seg_bearing, overall_bearing)
        if not lane_wps or len(lane_wps) < 2:
            continue

        # Step 3: find all coord indices within removal_nm of reference
        # These are the original MARNET waypoints that sit inside the strait
        in_zone = [i for i in range(len(coords))
                   if haversine_km(coords[i][0], coords[i][1],
                                   ref_lon, ref_lat) / 1.852 <= removal_nm]

        if not in_zone:
            # No coords inside removal zone — use closest_seg as the insertion point
            first_remove = closest_seg + 1
            last_remove  = closest_seg
        else:
            first_remove = in_zone[0]
            last_remove  = in_zone[-1]

        splices.append((first_remove, last_remove, lane_wps, tss['name'],
                        min_dist_nm, seg_bearing))
        log.info('TSS: %s dist=%.1fnm brg=%.0f° remove coords[%d:%d] (%d pts)',
                 tss['name'], min_dist_nm, seg_bearing,
                 first_remove, last_remove, last_remove - first_remove + 1)

    if not splices:
        return coords, []

    # Sort by first_remove index
    splices.sort(key=lambda s: s[0])

    result   = []
    prev_idx = 0

    for first_remove, last_remove, lane_wps, tss_name, dist_nm, bearing in splices:
        if first_remove < prev_idx:
            first_remove = prev_idx

        # Keep original route up to (not including) the removal zone
        for k in range(prev_idx, first_remove):
            result.append([coords[k][0], coords[k][1]])

        # Insert TSS lane
        for wp in lane_wps:
            result.append([wp[0], wp[1]])

        tss_applied.append(tss_name)
        prev_idx = last_remove + 1

    # Remainder
    for k in range(prev_idx, len(coords)):
        result.append([coords[k][0], coords[k][1]])

    # Deduplicate consecutive near-identical points (< 0.5nm)
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
