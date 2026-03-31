#!/usr/bin/env python3
"""
RoutePlannerPro — Maritime Routing + Copernicus Marine Weather Proxy
Deploy to Render.com

Environment variables required:
  CMEMS_USER  — Copernicus Marine username (email)
  CMEMS_PASS  — Copernicus Marine password

pip install flask searoute gunicorn requests
"""
import math, os, json, time, threading, base64, logging
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

# ── AUTH STRATEGY: HTTP Basic Auth (primary) + Bearer token (secondary) ───────
# Copernicus Marine WMTS supports HTTP Basic Auth directly.
# We try Basic Auth first — it is simpler, has no expiry, and always works
# as long as credentials are correct.
# Bearer token is attempted as a fallback for newer API endpoints.

_bearer_token     = None
_bearer_token_exp = 0
_token_lock       = threading.Lock()
CMEMS_TOKEN_URL   = 'https://iam.marine.copernicus.eu/realms/ocean/protocol/openid-connect/token'

def get_bearer_token():
    """Try to get a Bearer token. Returns token string or None."""
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
            log.debug('Bearer token fetch failed (will use Basic Auth): %s', e)
        return None


def cmems_request(params):
    """
    Make an authenticated GET request to the Copernicus Marine WMTS.
    Strategy:
      1. Try HTTP Basic Auth (always works if credentials are correct)
      2. If 401, try Bearer token once
      3. Return response
    """
    if not CMEMS_USER or not CMEMS_PASS:
        return None, 'CMEMS_USER/CMEMS_PASS not set in environment', 503

    headers = {
        'User-Agent': 'RoutePlannerPro/1.0',
        'Accept':     'image/png,image/jpeg,application/json,*/*',
    }

    # ── Attempt 1: HTTP Basic Auth ───────────────────────────────────────────
    try:
        r = req_lib.get(
            CMEMS_WMTS,
            params=params,
            headers=headers,
            auth=(CMEMS_USER, CMEMS_PASS),
            timeout=30,
        )
        log.debug('CMEMS Basic Auth response: %d', r.status_code)

        if r.status_code == 200:
            return r.content, r.headers.get('Content-Type','image/png'), 200

        if r.status_code == 401:
            log.warning('CMEMS Basic Auth 401 — trying Bearer token')
            # ── Attempt 2: Bearer token ──────────────────────────────────────
            token = get_bearer_token()
            if token:
                headers['Authorization'] = f'Bearer {token}'
                r2 = req_lib.get(
                    CMEMS_WMTS,
                    params=params,
                    headers=headers,
                    timeout=30,
                )
                log.debug('CMEMS Bearer response: %d', r2.status_code)
                return r2.content, r2.headers.get('Content-Type','image/png'), r2.status_code
            return r.content, r.headers.get('Content-Type','text/plain'), 401

        # Other HTTP errors (404 = layer not found, 400 = bad params, etc.)
        log.warning('CMEMS response %d for params: %s', r.status_code,
                    {k:v for k,v in params.items() if k not in ('password',)})
        return r.content, r.headers.get('Content-Type','text/plain'), r.status_code

    except req_lib.exceptions.Timeout:
        log.error('CMEMS request timed out')
        return b'', 'text/plain', 504
    except Exception as e:
        log.error('CMEMS request error: %s', e)
        return b'', 'text/plain', 503


def verify_cmems_auth():
    """
    Test auth by fetching a single known-good tile.
    Returns (ok: bool, method: str, message: str)
    """
    if not CMEMS_USER or not CMEMS_PASS:
        return False, 'none', 'CMEMS_USER or CMEMS_PASS not set'

    test_params = {
        'SERVICE':      'WMTS',
        'REQUEST':      'GetCapabilities',
        'VERSION':      '1.0.0',
    }
    try:
        content, ctype, status = cmems_request(test_params)
        if status == 200:
            method = 'bearer' if _bearer_token else 'basic'
            return True, method, 'Authentication successful'
        elif status == 401:
            return False, 'failed', f'Authentication failed (401) — check CMEMS_USER/CMEMS_PASS'
        else:
            return False, 'error', f'CMEMS returned HTTP {status}'
    except Exception as e:
        return False, 'error', str(e)


def cmems_tile_proxy(path, params):
    """Legacy wrapper — kept for compatibility."""
    content, ctype, status = cmems_request(params)
    return content, ctype, status


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


# ── COPERNICUS WMTS PROXY ENDPOINTS ──────────────────────────────────────────

@app.route("/api/cmems/tile")
def cmems_tile():
    """
    Proxy a single WMTS GetTile request to Copernicus Marine.
    The frontend passes all WMTS parameters; this endpoint injects auth.

    Query params forwarded: SERVICE, REQUEST, LAYER, FORMAT, TILEMATRIXSET,
    TILEMATRIX, TILEROW, TILECOL, TIME, STYLE, VERSION
    """
    # Forward all query params to CMEMS
    params = {k: v for k, v in request.args.items()}
    # Ensure mandatory params
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
    """
    Proxy WMTS GetCapabilities — used by frontend to discover available times.
    Optional param: layer=<PRODUCT>/<DATASET>
    """
    layer = request.args.get('layer', '')
    path  = layer if layer else ''
    params = {
        'SERVICE': 'WMTS',
        'REQUEST': 'GetCapabilities',
        'VERSION': '1.0.0',
    }
    content, ctype, status = cmems_tile_proxy(path, params)
    resp = Response(content, status=status, content_type=ctype)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Cache-Control'] = 'public, max-age=300'
    return resp


@app.route("/api/cmems/featureinfo")
def cmems_featureinfo():
    """
    Proxy WMTS GetFeatureInfo — returns weather values at a clicked point.
    Used for route waypoint weather lookup.
    Params: layer, tilematrix, tilerow, tilecol, i, j, time, infoformat
    """
    params = {k: v for k, v in request.args.items()}
    params.setdefault('SERVICE',    'WMTS')
    params.setdefault('REQUEST',    'GetFeatureInfo')
    params.setdefault('VERSION',    '1.0.0')
    params.setdefault('INFOFORMAT', 'application/json')
    params.setdefault('TILEMATRIXSET', 'EPSG:3857')

    content, ctype, status = cmems_request(params)
    resp = Response(content, status=status, content_type=ctype)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp


@app.route("/api/cmems/status")
def cmems_status():
    """Check auth status and return available layer info."""
    configured = bool(CMEMS_USER and CMEMS_PASS)
    auth_ok, auth_method, auth_msg = verify_cmems_auth() if configured else (False,'none','Not configured')
    r = jsonify({
        'configured':  configured,
        'user':        CMEMS_USER if configured else None,
        'auth_type':   auth_method,
        'token_valid': auth_ok,
        'message':     auth_msg,
        'layers': {
            'wind': {
                'product':  'WIND_GLO_PHY_L4_NRT_012_004',
                'dataset':  'cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H-r',
                'variable': 'wind_speed',
                'vector':   'wind_speed',
                'style':    'vectorStyle:solidAndVector,cmap:speed,range=0/20',
                'desc':     'Global Wind 0.125° hourly NRT',
            },
            'wave': {
                'product':  'GLOBAL_ANALYSISFORECAST_WAV_001_027',
                'dataset':  'cmems_mod_glo_wav_anfc_0.083deg_PT3H-i',
                'variable': 'VHM0',
                'style':    'cmap:matter,range=0/8',
                'desc':     'Global Wave Height 1/12° 3-hourly',
            },
            'current': {
                'product':  'GLOBAL_ANALYSISFORECAST_PHY_001_024',
                'dataset':  'cmems_mod_glo_phy-cur_anfc_0.083deg_PT6H-i_202406',
                'variable': 'sea_water_velocity',
                'style':    'vectorStyle:solidAndVector,cmap:thermal,range=0/2',
                'desc':     'Global Ocean Current 1/12° 6-hourly',
            },
        }
    })
    r.headers['Access-Control-Allow-Origin'] = '*'
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
    log.info("RoutePlannerPro — http://localhost:%d", port)
    log.info("CMEMS user: %s", CMEMS_USER or "NOT SET — add CMEMS_USER env var")
    app.run(host="0.0.0.0",port=port,debug=False)
