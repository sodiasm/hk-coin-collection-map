import os, re, json, datetime
from typing import Dict, Any, List, Tuple
import requests
import pdfplumber

CSDI_WFS_URL = os.getenv('CSDI_WFS_URL', 'https://portal.csdi.gov.hk/server/services/common/had_rcd_1634523272907_75218/MapServer/WFSServer?service=wfs&request=GetFeature&typenames=DCD&outputFormat=geojson&count=200')
HKMA_PDF_URL = os.getenv('HKMA_PDF_URL', 'https://www.hkma.gov.hk/media/chi/doc/key-functions/monetary-stability/notes-and-coins/chi_coin_collection.pdf')

OUT_DIR = 'data'
OUT_GEOJSON = os.path.join(OUT_DIR, 'hk-districts.geojson')
OUT_CENTROIDS = os.path.join(OUT_DIR, 'district_centroids.json')
OUT_SCHEDULE = os.path.join(OUT_DIR, 'schedule.json')
OUT_LOCATION_COORDS = os.path.join(OUT_DIR, 'location_coords.json')

SERVICE_HOURS_DEFAULT = '10:00-19:00'

DATE_RANGE_RE = re.compile(r'(\d{1,2})\s*月\s*(\d{1,2})\s*日.*?至.*?(\d{1,2})\s*月\s*(\d{1,2})\s*日')
DATE_RE = re.compile(r'(\d{1,2})\s*月\s*(\d{1,2})\s*日')


def http_get(url: str) -> bytes:
    r = requests.get(url, timeout=60, headers={'User-Agent': 'Mozilla/5.0'})
    r.raise_for_status()
    return r.content


def fetch_geojson():
    os.makedirs(OUT_DIR, exist_ok=True)
    raw = http_get(CSDI_WFS_URL)
    try:
        data = json.loads(raw.decode('utf-8'))
    except Exception:
        data = json.loads(raw)
    with open(OUT_GEOJSON, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    return data


def extract_points(geom: Dict[str, Any]) -> List[Tuple[float, float]]:
    pts = []
    if not geom:
        return pts
    t = geom.get('type')
    coords = geom.get('coordinates')
    if t == 'Polygon':
        for ring in coords or []:
            for lon, lat in ring:
                pts.append((lon, lat))
    elif t == 'MultiPolygon':
        for poly in coords or []:
            for ring in poly:
                for lon, lat in ring:
                    pts.append((lon, lat))
    return pts


def compute_centroids(fc: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    res = {}
    for feat in fc.get('features', []):
        name = ((feat.get('properties') or {}).get('NAME_TC') or '').strip()
        pts = extract_points(feat.get('geometry') or {})
        if not name or not pts:
            continue
        lon = sum(p[0] for p in pts) / len(pts)
        lat = sum(p[1] for p in pts) / len(pts)
        res[name] = {'lat': lat, 'lng': lon}
    with open(OUT_CENTROIDS, 'w', encoding='utf-8') as f:
        json.dump(res, f, ensure_ascii=False, indent=2)
    return res


def normalize_text(s: str) -> str:
    if not s:
        return ''
    s = s.replace('*', '')
    s = s.replace('（', '(').replace('）', ')')
    s = re.sub(r'\s+', '', s)
    return s.strip()


def make_location_key(district: str, location: str) -> str:
    return f"{normalize_text(district)}|{normalize_text(location)}"


def load_location_coords() -> Dict[str, Any]:
    if not os.path.exists(OUT_LOCATION_COORDS):
        return {'schema_version': 1, 'updated_at': None, 'points': {}}
    with open(OUT_LOCATION_COORDS, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if 'points' not in data or not isinstance(data['points'], dict):
        data['points'] = {}
    return data


def parse_iso_date(year: int, month: str, day: str) -> str:
    return datetime.date(year, int(month), int(day)).isoformat()


def enrich_stop(stop: Dict[str, Any], truck_id: int, seq: int, coords_map: Dict[str, Any]) -> Dict[str, Any]:
    location_key = make_location_key(stop['district'], stop['location'])
    point = coords_map.get(location_key, {})
    stop['stop_id'] = f"t{truck_id}-{stop['start_date']}-{seq:02d}"
    stop['location_key'] = location_key
    stop['lat'] = point.get('lat')
    stop['lng'] = point.get('lng')
    stop['coord_status'] = point.get('status', 'pending')
    stop['coord_source'] = point.get('source')
    return stop


def process_cell(cell_text: str, outer_start: str, outer_end: str, year: int) -> List[Dict[str, Any]]:
    if not cell_text or '暫停服務' in cell_text:
        return []

    lines = [l.strip() for l in cell_text.split('\n') if l and l.strip()]
    if len(lines) < 2:
        return []

    district = lines[0].replace(' ', '')
    if not district.endswith('區'):
        return []

    sub_ranges = []
    for idx, line in enumerate(lines[1:], start=1):
        m = DATE_RANGE_RE.search(line.replace(' ', ''))
        if m:
            sub_ranges.append({
                'line_index': idx,
                'start_date': parse_iso_date(year, m.group(1), m.group(2)),
                'end_date': parse_iso_date(year, m.group(3), m.group(4))
            })

    suspension_dates = []
    for line in lines[1:]:
        compact = line.replace(' ', '')
        if '暫停' in compact:
            for m in DATE_RE.finditer(compact):
                try:
                    suspension_dates.append(parse_iso_date(year, m.group(1), m.group(2)))
                except ValueError:
                    pass
    suspension_dates = sorted(set(suspension_dates))

    def clean_location_parts(parts: List[str]) -> str:
        cleaned = []
        for p in parts:
            compact = p.replace(' ', '')
            if DATE_RANGE_RE.search(compact):
                continue
            if '暫停' in compact:
                continue
            cleaned.append(p.replace('*', '').strip())
        return re.sub(r'\s+', ' ', ' '.join([x for x in cleaned if x])).strip()

    stops = []

    if not sub_ranges:
        location = clean_location_parts(lines[1:])
        if location:
            valid_susp = [d for d in suspension_dates if outer_start <= d <= outer_end]
            stops.append({
                'district': district,
                'location': location,
                'raw_location': re.sub(r'\s+', ' ', ' '.join(lines[1:])).strip(),
                'start_date': outer_start,
                'end_date': outer_end,
                'suspended_dates': valid_susp,
                'service_hours': SERVICE_HOURS_DEFAULT
            })
        return stops

    prev = 1
    for sub in sub_ranges:
        loc_lines = lines[prev:sub['line_index']]
        location = clean_location_parts(loc_lines)
        if location:
            valid_susp = [d for d in suspension_dates if sub['start_date'] <= d <= sub['end_date']]
            stops.append({
                'district': district,
                'location': location,
                'raw_location': re.sub(r'\s+', ' ', ' '.join(lines[1:])).strip(),
                'start_date': sub['start_date'],
                'end_date': sub['end_date'],
                'suspended_dates': valid_susp,
                'service_hours': SERVICE_HOURS_DEFAULT
            })
        prev = sub['line_index'] + 1

    if not stops:
        location = clean_location_parts(lines[1:])
        if location:
            valid_susp = [d for d in suspension_dates if outer_start <= d <= outer_end]
            stops.append({
                'district': district,
                'location': location,
                'raw_location': re.sub(r'\s+', ' ', ' '.join(lines[1:])).strip(),
                'start_date': outer_start,
                'end_date': outer_end,
                'suspended_dates': valid_susp,
                'service_hours': SERVICE_HOURS_DEFAULT
            })

    return stops


def parse_pdf_to_schedule(pdf_bytes: bytes, coords_data: Dict[str, Any]) -> Dict[str, Any]:
    os.makedirs('tmp', exist_ok=True)
    pdf_path = os.path.join('tmp', 'hkma.pdf')
    with open(pdf_path, 'wb') as f:
        f.write(pdf_bytes)

    coords_map = (coords_data or {}).get('points', {})
    all_schedules = {1: [], 2: []}
    seqs = {1: 0, 2: 0}
    year = 2026

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            for row in table:
                if not row or len(row) < 5:
                    continue

                date1_col = row[0]
                truck1_col = row[2] if len(row) > 2 else None
                date2_col = row[3] if len(row) > 3 else None
                truck2_col = row[4] if len(row) > 4 else None

                if date1_col and truck1_col and isinstance(date1_col, str):
                    dates = DATE_RE.findall(date1_col.replace(' ', ''))
                    if len(dates) >= 2:
                        outer_start = parse_iso_date(year, dates[0][0], dates[0][1])
                        outer_end = parse_iso_date(year, dates[-1][0], dates[-1][1])
                        for stop in process_cell(truck1_col, outer_start, outer_end, year):
                            seqs[1] += 1
                            all_schedules[1].append(enrich_stop(stop, 1, seqs[1], coords_map))

                if date2_col and truck2_col and isinstance(date2_col, str):
                    dates = DATE_RE.findall(date2_col.replace(' ', ''))
                    if len(dates) >= 2:
                        outer_start = parse_iso_date(year, dates[0][0], dates[0][1])
                        outer_end = parse_iso_date(year, dates[-1][0], dates[-1][1])
                        for stop in process_cell(truck2_col, outer_start, outer_end, year):
                            seqs[2] += 1
                            all_schedules[2].append(enrich_stop(stop, 2, seqs[2], coords_map))

    return {
        'schema_version': 2,
        'last_updated': datetime.date.today().isoformat(),
        'source': HKMA_PDF_URL,
        'trucks': [
            {'id': 1, 'name': '收銀車1號', 'color': '#f0c040', 'schedules': all_schedules[1]},
            {'id': 2, 'name': '收銀車2號', 'color': '#4fc3f7', 'schedules': all_schedules[2]}
        ]
    }


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    fc = fetch_geojson()
    compute_centroids(fc)
    coords_data = load_location_coords()
    pdf_bytes = http_get(HKMA_PDF_URL)
    schedule = parse_pdf_to_schedule(pdf_bytes, coords_data)
    with open(OUT_SCHEDULE, 'w', encoding='utf-8') as f:
        json.dump(schedule, f, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    main()
