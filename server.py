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

# Auth token cache
_cmems_token      = None
_cmems_token_exp  = 0
_cmems_token_lock = threading.Lock()

CMEMS_TOKEN_URL = (
    'https://iam.marine.copernicus.eu/realms/ocean/protocol/openid-connect/token'
)

def get_cmems_token():
    """
    Obtain a short-lived Bearer token from Copernicus Marine IAM.
    Tokens last ~60 seconds; we cache and refresh automatically.
    Falls back to HTTP Basic Auth if token endpoint fails.
    """
    global _cmems_token, _cmems_token_exp
    with _cmems_token_lock:
        if _cmems_token and time.time() < _cmems_token_exp - 10:
            return _cmems_token, 'bearer'
        if not CMEMS_USER or not CMEMS_PASS:
            return None, 'none'
        try:
            resp = req_lib.post(
                CMEMS_TOKEN_URL,
                data={
                    'client_id':  'cmems-marine-public',
                    'username':   CMEMS_USER,
                    'password':   CMEMS_PASS,
                    'grant_type': 'password',
                },
                timeout=15,
            )
            if resp.status_code == 200:
                d = resp.json()
                _cmems_token     = d['access_token']
                _cmems_token_exp = time.time() + d.get('expires_in', 60)
                log.info('CMEMS token obtained, expires in %ds', d.get('expires_in', 60))
                return _cmems_token, 'bearer'
            else:
                log.warning('CMEMS token endpoint returned %d', resp.status_code)
        except Exception as e:
            log.warning('CMEMS token error: %s', e)
        # Fallback: Basic Auth
        _cmems_token = None
        return None, 'basic'


def cmems_tile_proxy(path, params):
    """
    Proxy a WMTS tile request to Copernicus Marine, injecting auth.
    Returns (content_bytes, content_type, status_code).
    """
    token, auth_type = get_cmems_token()

    headers = {'User-Agent': 'RoutePlannerPro/1.0'}
    if auth_type == 'bearer' and token:
        headers['Authorization'] = f'Bearer {token}'
        auth = None
    elif auth_type == 'basic' and CMEMS_USER and CMEMS_PASS:
        auth = (CMEMS_USER, CMEMS_PASS)
    else:
        auth = None

    url = f'{CMEMS_WMTS}/{path}' if path else CMEMS_WMTS
    try:
        r = req_lib.get(
            url,
            params=params,
            headers=headers,
            auth=auth,
            timeout=30,
            stream=True,
        )
        content_type = r.headers.get('Content-Type', 'image/png')
        return r.content, content_type, r.status_code
    except Exception as e:
        log.error('CMEMS proxy error: %s', e)
        return b'', 'text/plain', 503


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

    content, ctype, status = cmems_tile_proxy('', params)

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

    content, ctype, status = cmems_tile_proxy('', params)
    resp = Response(content, status=status, content_type=ctype)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp


@app.route("/api/cmems/status")
def cmems_status():
    """Check auth status and return available layer info."""
    token, auth_type = get_cmems_token()
    configured = bool(CMEMS_USER and CMEMS_PASS)
    r = jsonify({
        'configured':  configured,
        'user':        CMEMS_USER if configured else None,
        'auth_type':   auth_type,
        'token_valid': bool(token),
        'layers': {
            'wind': {
                'product':  'WIND_GLO_PHY_L4_NRT_012_004',
                'dataset':  'cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H-r',
                'variable': 'wind_speed',
                'vector':   'wind_velocity',
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
