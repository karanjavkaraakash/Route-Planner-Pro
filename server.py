#!/usr/bin/env python3
"""
SeaRoute Maritime Routing Server
Deploy to Render.com — pip install flask searoute gunicorn
"""
import math, os
from pathlib import Path
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__, static_folder=".")
ENGINE = None
GRAPH  = None

def haversine_km(lon1, lat1, lon2, lat2):
    R = 6371.0; r = math.pi / 180
    dlat=(lat2-lat1)*r; dlon=(lon2-lon1)*r
    a=math.sin(dlat/2)**2+math.cos(lat1*r)*math.cos(lat2*r)*math.sin(dlon/2)**2
    return R*2*math.asin(math.sqrt(max(0,a)))

def load_engine():
    global ENGINE, GRAPH
    try:
        from scgraph.geographs.marnet import marnet_geograph
        GRAPH=marnet_geograph; ENGINE='scgraph'
        print(f"  Engine: scgraph — {len(GRAPH.graph)} nodes"); return True
    except: pass
    try:
        import searoute as sr
        GRAPH=sr; ENGINE='searoute'
        print("  Engine: searoute-py"); return True
    except: pass
    print("  ERROR: pip install searoute"); return False

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
    """
    Returns True if the route must pass through Gulf of Aden
    (one endpoint west of 50°E above equator, other east of 55°E).
    In this case we force babalmandab passage which makes searoute
    use its built-in Gulf of Aden corridor — no manual waypoints needed.
    """
    west = (olon < 50 and olat > 0) or (dlon < 50 and dlat > 0)
    east = (olon > 55) or (dlon > 55)
    return west and east

def route_scgraph(olon,olat,dlon,dlat,restrictions):
    try:
        result=GRAPH.get_shortest_path(
            origin_node={"latitude":olat,"longitude":olon},
            destination_node={"latitude":dlat,"longitude":dlon},
            output_units='km',
            node_addition_type='quadrant',
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

    # Build restrictions list from user avoid preferences
    sr_r=[PASSAGE_MAP[r] for r in restrictions if r in PASSAGE_MAP]

    # If route goes through Gulf of Aden region, ensure babalmandab is NOT
    # in the restrictions list (we want to use it, not avoid it).
    # searoute's babalmandab passage uses the correct Gulf of Aden corridor.
    if needs_babalmandab(olon,olat,dlon,dlat):
        if 'babalmandab' in sr_r:
            sr_r.remove('babalmandab')

    try:
        route=sr.searoute(
            [olon,olat],[dlon,dlat],
            units="km",
            append_orig_dest=True,
            restrictions=sr_r,
            return_passages=True
        )
    except Exception as e:
        return {"error":str(e)}

    if not route: return {"error":"No route found"}

    geom=route.get("geometry",{}); coords=geom.get("coordinates",[])
    props=route.get("properties",{}); total_km=props.get("length",0)
    passages=props.get("passages",[])
    if isinstance(passages,str): passages=[passages] if passages else []

    return {
        "coordinates": coords,
        "distance_km": round(total_km,1),
        "distance_nm": round(total_km/1.852,1),
        "route_name":  name_from_passages(passages),
        "passages":    passages,
        "node_count":  len(coords),
        "warning":     None
    }

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
    print(f"\n  SeaRoute — open http://localhost:{port}\n")
    app.run(host="0.0.0.0",port=port,debug=False)
