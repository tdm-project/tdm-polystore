import requests
from datetime import datetime

SARDINIA_BBOX = "41.273966,8.034185;38.800759,9.836935"
HERE_PBT_FMT = '%Y-%m-%dT%H:%M:%SZ'


def swap(t):
    return (t[1], t[0])


def to_point(s):
    return(swap(list(map(float, s.split(',')))))


def to_points(s):
    s = s.strip()
    return [to_point(_) for _ in s.split()]


def merge_segments_helper(segments):
    # FIXME this is not very efficient...
    poly = segments[0]
    for i, s in enumerate(segments[1:]):
        if poly[-1] == s[0]:
            poly += s[1:]
        else:
            return poly, segments[i+1:]
    return poly, []


def merge_segments_fast(segments):
    polys = []
    while len(segments) > 0:
        poly, segments = merge_segments_helper(segments)
        polys.append(poly)
    return polys


def merge_segments_slow(segments):
    begins = {}
    ends = {}
    # Assumes no flips
    for i, s in enumerate(segments):
        begins.setdefault(tuple(s[0]), []).append(s)
        ends.setdefault(tuple(s[-1]), []).append(s)
    strain_begins = []
    for k in begins:
        if k not in ends:
            strain_begins.append(k)
    strains = []
    for b in strain_begins:
        assert len(begins[b]) == 1
        poly = begins[b][0]
        while poly[-1] in begins:
            poly += begins[poly[-1]][0][1:]
        strains.append(poly)
    return strains


def merge_segments(segments):
    polys = merge_segments_fast(segments)
    return merge_segments_slow(polys)


def get_shape(shp):
    shape = merge_segments([to_points(_['value'][0]) for _ in shp])
    assert len(shape) == 1
    return {"type": "LineString", "coordinates": shape[0]}


def analyze_flow_item(pbt, fi):
    m = []
    if len(fi) == 1:
        b = fi[0]
        m.append({
            'id': f'here/segment/{b["TMC"]["PC"]}',
            'pbt': pbt,
            'geojson': get_shape(b['SHP']),
            'TMC': [b['TMC']],
            'CF': b['CF'][0]})
    for b, e in zip(fi[:-1], fi[1:]):
        m.append({
            'id': f'here/segment/{b["TMC"]["PC"]}:{e["TMC"]["PC"]}',
            'pbt': pbt,
            'geojson': get_shape(b['SHP']),
            'TMC': [b['TMC'], e['TMC']],
            'CF': b['CF'][0]})
    return m


def analyze_roadway(rw):
    """
    Organize rw data by TMC location pairs.
    """
    pbt = datetime.strptime(rw['PBT'], HERE_PBT_FMT)
    seglists = []
    for fis in rw['FIS']:
        seglists.append(analyze_flow_item(pbt, fis['FI']))
    return sum(seglists, [])


def get_from_here(app_id, app_code, what, v='6.3', fmt='json'):
    args = {'bbox': "41.273966,8.034185;38.800759,9.836935",
            'app_id': app_id,
            'app_code': app_code,
            'responseattributes': 'sh,fc'}
    url = f"https://traffic.api.here.com/traffic/{v}/{what}.{fmt}"
    r = requests.get(url, args)
    r.raise_for_status()
    return r.json() if fmt == 'json' else r.content


def process_flow_data(data):
    return sum([analyze_roadway(rw) for rw in data['RWS'][0]['RW']], [])


def fetch_here_data(app_id, app_code, what):
    data = get_from_here(app_id, app_code, what)
    if what == 'flow':
        return process_flow_data(data)
    else:
        raise ValueError(f'{what} not implemented yet')


def get_description_of_src(here_desc):
    alias = ' '.join(tmc['DE'] for tmc in here_desc['TMC'])
    return {
        "id": here_desc['id'],
        "alias": alias,
        "entity_category": "Station",
        "entity_type": "TrafficObserver",
        "default_footprint": here_desc['geojson'],
        "stationary": True,
        "controlledProperties": ['SP', 'SU', 'FF', 'JF', 'CN'],
        "shape": [],
        "description": {
            "type": "trafficSensor",
            "brandName": "HERE",
            "modelName": "V6.2",
            "manufacturerName": "HERE",
            "category": ["sensor"],
            "function": ["sensing"],
            "acquisition_period": 300,
            "reference": "https://developer.here.com"
        },
    }
