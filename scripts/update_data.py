import os, re, json, datetime
from typing import Dict, Any, List, Tuple
import requests
import pdfplumber

CSDI_WFS_URL = os.getenv('CSDI_WFS_URL', 'https://portal.csdi.gov.hk/server/services/common/had_rcd_1634523272907_75218/MapServer/WFSServer?service=wfs&request=GetFeature&typenames=DCD&outputFormat=geojson&count=200')
HKMA_PDF_URL_ZH = os.getenv('HKMA_PDF_URL_ZH', 'https://www.hkma.gov.hk/media/chi/doc/key-functions/monetary-stability/notes-and-coins/chi_coin_collection.pdf')
HKMA_PDF_URL_EN = os.getenv('HKMA_PDF_URL_EN', 'https://www.hkma.gov.hk/media/eng/doc/key-functions/monetary-stability/notes-and-coins/coin_collection.pdf')

OUT_DIR = 'data'
DEBUG_DIR = 'data/debug'
OUT_GEOJSON = os.path.join(OUT_DIR, 'hk-districts.geojson')
OUT_CENTROIDS = os.path.join(OUT_DIR, 'district_centroids.json')
OUT_SCHEDULE = os.path.join(OUT_DIR, 'schedule.json')
OUT_LOCATION_COORDS = os.path.join(OUT_DIR, 'location_coords.json')

SERVICE_HOURS_DEFAULT = '10:00-19:00'

DATE_RANGE_RE_ZH = re.compile(r'(\d{1,2})\s*月\s*(\d{1,2})\s*日.*?至.*?(\d{1,2})\s*月\s*(\d{1,2})\s*日')
DATE_RE_ZH = re.compile(r'(\d{1,2})\s*月\s*(\d{1,2})\s*日')
SUSP_NOTE_RE_ZH = re.compile(r'[（(][^()（）]*暫停[^()（）]*[）)]')
LEADING_WEEKDAY_RE_ZH = re.compile(r'^(?:\(\s*星期[一二三四五六日天]\s*\)\s*)+')

MONTHS = {
    'jan': 1, 'january': 1,
    'feb': 2, 'february': 2,
    'mar': 3, 'march': 3,
    'apr': 4, 'april': 4,
    'may': 5,
    'jun': 6, 'june': 6,
    'jul': 7, 'july': 7,
    'aug': 8, 'august': 8,
    'sep': 9, 'sept': 9, 'september': 9,
    'oct': 10, 'october': 10,
    'nov': 11, 'november': 11,
    'dec': 12, 'december': 12,
}

DATE_RE_EN = re.compile(r'(\d{1,2})\s+([A-Za-z]{3,9})', re.I)
EN_DATE_RANGE_RE = re.compile(
    r'(\d{1,2})\s+([A-Za-z]{3,9})\s*\([^)]*\)[\s\n]*to[\s\n]*(\d{1,2})\s+([A-Za-z]{3,9})\s*\([^)]*\)',
    re.I | re.MULTILINE
)

CORE_PREFIX_RE_EN = re.compile(
    r'^(?:'
    r'Lay-?by\s+outside\s+|'
    r'Lay-?by\s+opposite\s+to\s+|'
    r'Lay-?by\s+opposite\s+|'
    r'Lay-?by\s+near\s+|'
    r'Lay-?by\s+on\s+|'
    r'Outside\s+|'
    r'Opposite\s+to\s+|'
    r'Opposite\s+|'
    r'Adjacent\s+to\s+|'
    r'Adjacent\s+|'
    r'Nearby\s+|'
    r'Near\s+|'
    r'Open\s+area\s+adjacent\s+to\s+|'
    r'Open\s+parking\s+area\s+between\s+|'
    r'Open\s+area\s+between\s+|'
    r'Open\s+area\s+outside\s+|'
    r'Open\s+area\s+at\s+|'
    r'Open\s+area\s+near\s+|'
    r'Open\s+area\s+'
    r')',
    re.I,
)

def dbg(msg: str):
    print(f'[DEBUG] {msg}', flush=True)

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

def normalize_display_text(s: str) -> str:
    if not s:
        return ''
    s = s.replace('*', '')
    s = s.replace('（', '(').replace('）', ')')
    s = re.sub(r'\s+', ' ', s)
    s = LEADING_WEEKDAY_RE_ZH.sub('', s)
    return s.strip(' -、;；,，').strip()

def normalize_en_display_text(s: str) -> str:
    if not s:
        return ''
    s = s.replace('*', '')
    s = s.replace('（', '(').replace('）', ')')
    s = s.replace('–', '-').replace('—', '-')
    s = re.sub(r'\s+', ' ', s)
    return s.strip(' -;,.').strip()

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

def parse_iso_date(year: int, month: int, day: int) -> str:
    return datetime.date(year, int(month), int(day)).isoformat()

def month_to_number(month_name: str) -> int:
    return MONTHS[month_name.strip().lower()[:3]]

def extract_suspension_dates_zh(text: str, year: int) -> List[str]:
    normalized = text.replace('（', '(').replace('）', ')')
    dates = []
    for note in SUSP_NOTE_RE_ZH.findall(normalized):
        for m in DATE_RE_ZH.finditer(note):
            try:
                dates.append(parse_iso_date(year, int(m.group(1)), int(m.group(2))))
            except ValueError:
                pass
    return sorted(set(dates))

def strip_suspension_notes_zh(text: str) -> str:
    normalized = text.replace('（', '(').replace('）', ')')
    return SUSP_NOTE_RE_ZH.sub('', normalized)

def extract_suspension_dates_en(text: str, year: int) -> List[str]:
    dates = []
    text_normalized = text.replace('\n', ' ')
    for m in re.finditer(r'\(?Service suspended.*?(?:\)|$)', text_normalized, re.I):
        for day, month in re.findall(r'(\d{1,2})\s+([A-Za-z]{3,9})', m.group(0)):
            try:
                dates.append(parse_iso_date(year, month_to_number(month), int(day)))
            except Exception:
                pass
    return sorted(set(dates))

def clean_core_en_location(text: str) -> str:
    if not text: return ''
    s = normalize_en_display_text(text)
    if s.lower() in ['to']: return ''
    s = re.sub(r'\([^)]*\)', '', s)
    s = CORE_PREFIX_RE_EN.sub('', s)
    s = re.sub(r'\s*,\s*', ', ', s)
    s = re.sub(r'\s+', ' ', s)
    s = s.strip(' ,.-')
    if s.lower() in ['to']: return ''
    return s

def build_en_query_candidates(raw_location_en: str, location_en_core: str, district_en: str) -> List[str]:
    candidates: List[str] = []

    def add(value: str):
        if not value: return
        value = normalize_en_display_text(value)
        value = re.sub(r'\s*,\s*', ', ', value)
        value = re.sub(r'\s+', ' ', value).strip(' ,.-')
        if value and value.lower() not in ['to', ''] and value not in candidates:
            candidates.append(value)

    core = clean_core_en_location(location_en_core or raw_location_en)
    raw = normalize_en_display_text(raw_location_en)
    add(core)
    add(raw)

    parts = [p.strip(' .') for p in core.split(',') if p.strip(' .')]
    if len(parts) >= 2:
        add(', '.join(parts[:2]))
        add(', '.join(parts[-2:]))
        if len(parts) >= 3:
            add(', '.join(parts[:3]))

    for i, part in enumerate(parts):
        lower = part.lower()
        if any(token in lower for token in ['estate', 'court', 'plaza', 'mall', 'centre', 'center', 'garden', 'bay', 'mansion', 'building', 'tower', 'house']):
            if i + 1 < len(parts):
                add(f'{part}, {parts[i + 1]}')
            add(part)

    if parts and re.search(r'\d+.*(?:street|road|avenue|lane|path|drive|terrace)\b', parts[0], re.I):
        add(parts[0])
        if len(parts) >= 2:
            add(f'{parts[0]}, {parts[1]}')

    for value in list(candidates):
        add(f'{value}, Hong Kong')
        if district_en and district_en.lower() not in ['to']:
            add(f'{value}, {district_en}, Hong Kong')

    return candidates

def process_cell_zh(cell_text: str, outer_start: str, outer_end: str, year: int) -> List[Dict[str, Any]]:
    if not cell_text or '暫停服務' in cell_text:
        return []

    lines = [l.strip() for l in cell_text.split('\n') if l and l.strip()]
    if len(lines) < 2:
        return []

    district = lines[0].replace(' ', '')
    if not district.endswith('區'):
        return []

    body = re.sub(r'\s+', ' ', ' '.join(lines[1:])).strip()
    raw_location = body
    suspension_dates = extract_suspension_dates_zh(body, year)
    parse_body = normalize_display_text(strip_suspension_notes_zh(body))

    stops = []
    pos = 0
    while True:
        m = DATE_RANGE_RE_ZH.search(parse_body, pos)
        if not m:
            break
        location = normalize_display_text(parse_body[pos:m.start()])
        start_date = parse_iso_date(year, int(m.group(1)), int(m.group(2)))
        end_date = parse_iso_date(year, int(m.group(3)), int(m.group(4)))
        if location:
            valid_susp = [d for d in suspension_dates if start_date <= d <= end_date]
            stops.append({
                'district': district,
                'location': location,
                'raw_location': raw_location,
                'start_date': start_date,
                'end_date': end_date,
                'suspended_dates': valid_susp,
                'service_hours': SERVICE_HOURS_DEFAULT
            })
        pos = m.end()

    if stops:
        return stops

    location = normalize_display_text(parse_body)
    if location:
        valid_susp = [d for d in suspension_dates if outer_start <= d <= outer_end]
        return [{
            'district': district,
            'location': location,
            'raw_location': raw_location,
            'start_date': outer_start,
            'end_date': outer_end,
            'suspended_dates': valid_susp,
            'service_hours': SERVICE_HOURS_DEFAULT
        }]
    return []

def process_cell_en(cell_text: str, outer_start: str, outer_end: str, year: int) -> List[Dict[str, Any]]:
    if not cell_text:
        return []

    lines = [l.strip() for l in cell_text.split('\n') if l and l.strip()]
    if len(lines) < 2:
        return []

    district_en = lines[0]
    if not district_en.lower().endswith(' district'):
        return []

    body = '\n'.join(lines[1:]).strip()
    raw_location_en = normalize_en_display_text(body)
    suspension_dates = extract_suspension_dates_en(body, year)

    stops = []
    pos = 0
    while True:
        m = EN_DATE_RANGE_RE.search(body, pos)
        if not m:
            break
        loc_text = body[pos:m.start()].strip()
        loc_text = re.sub(r'(?i)\(?Service suspended[\s\S]*?(?:\)|$)', '', loc_text).strip()
        loc_text = normalize_en_display_text(loc_text)
        if loc_text.lower() == 'to':
            loc_text = ''
        
        start_date = parse_iso_date(year, month_to_number(m.group(2)), int(m.group(1)))
        end_date = parse_iso_date(year, month_to_number(m.group(4)), int(m.group(3)))
        
        if loc_text:
            valid_susp = [d for d in suspension_dates if start_date <= d <= end_date]
            core = clean_core_en_location(loc_text)
            stops.append({
                'district_en': district_en,
                'location_en': loc_text,
                'raw_location_en': raw_location_en,
                'location_en_core': core,
                'location_en_query_candidates': build_en_query_candidates(loc_text, core, district_en),
                'start_date': start_date,
                'end_date': end_date,
                'suspended_dates': valid_susp,
                'service_hours': SERVICE_HOURS_DEFAULT
            })
        pos = m.end()

    if stops:
        return stops

    loc_text = body.strip()
    loc_text = re.sub(r'(?i)\(?Service suspended[\s\S]*?(?:\)|$)', '', loc_text).strip()
    loc_text = normalize_en_display_text(loc_text)
    if loc_text.lower() == 'to':
        loc_text = ''
    
    if loc_text:
        valid_susp = [d for d in suspension_dates if outer_start <= d <= outer_end]
        core = clean_core_en_location(loc_text)
        return [{
            'district_en': district_en,
            'location_en': loc_text,
            'raw_location_en': raw_location_en,
            'location_en_core': core,
            'location_en_query_candidates': build_en_query_candidates(loc_text, core, district_en),
            'start_date': outer_start,
            'end_date': outer_end,
            'suspended_dates': valid_susp,
            'service_hours': SERVICE_HOURS_DEFAULT
        }]
    return []

def parse_english_pdf(pdf_bytes: bytes) -> Dict[int, List[Dict[str, Any]]]:
    os.makedirs('tmp', exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)
    pdf_path = os.path.join('tmp', 'hkma_en.pdf')
    with open(pdf_path, 'wb') as f:
        f.write(pdf_bytes)

    coords_placeholder = {1: [], 2: []}
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
                    dates = DATE_RE_EN.findall(date1_col.replace('\n', ' '))
                    if len(dates) >= 2:
                        outer_start = parse_iso_date(year, month_to_number(dates[0][1]), int(dates[0][0]))
                        outer_end = parse_iso_date(year, month_to_number(dates[-1][1]), int(dates[-1][0]))
                        coords_placeholder[1].extend(process_cell_en(truck1_col, outer_start, outer_end, year))
                
                if date2_col and truck2_col and isinstance(date2_col, str):
                    dates = DATE_RE_EN.findall(date2_col.replace('\n', ' '))
                    if len(dates) >= 2:
                        outer_start = parse_iso_date(year, month_to_number(dates[0][1]), int(dates[0][0]))
                        outer_end = parse_iso_date(year, month_to_number(dates[-1][1]), int(dates[-1][0]))
                        coords_placeholder[2].extend(process_cell_en(truck2_col, outer_start, outer_end, year))

    with open(os.path.join(DEBUG_DIR, 'en_schedules.json'), 'w', encoding='utf-8') as df:
        json.dump({str(k): v for k, v in coords_placeholder.items()}, df, ensure_ascii=False, indent=2)
    return coords_placeholder

def parse_chinese_pdf(pdf_bytes: bytes) -> Dict[int, List[Dict[str, Any]]]:
    os.makedirs('tmp', exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)
    pdf_path = os.path.join('tmp', 'hkma_zh.pdf')
    with open(pdf_path, 'wb') as f:
        f.write(pdf_bytes)

    coords_placeholder = {1: [], 2: []}
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
                    dates = DATE_RE_ZH.findall(date1_col.replace(' ', ''))
                    if len(dates) >= 2:
                        outer_start = parse_iso_date(year, int(dates[0][0]), int(dates[0][1]))
                        outer_end = parse_iso_date(year, int(dates[-1][0]), int(dates[-1][1]))
                        coords_placeholder[1].extend(process_cell_zh(truck1_col, outer_start, outer_end, year))
                if date2_col and truck2_col and isinstance(date2_col, str):
                    dates = DATE_RE_ZH.findall(date2_col.replace(' ', ''))
                    if len(dates) >= 2:
                        outer_start = parse_iso_date(year, int(dates[0][0]), int(dates[0][1]))
                        outer_end = parse_iso_date(year, int(dates[-1][0]), int(dates[-1][1]))
                        coords_placeholder[2].extend(process_cell_zh(truck2_col, outer_start, outer_end, year))

    with open(os.path.join(DEBUG_DIR, 'zh_schedules.json'), 'w', encoding='utf-8') as df:
        json.dump({str(k): v for k, v in coords_placeholder.items()}, df, ensure_ascii=False, indent=2)
    return coords_placeholder

def is_valid_hk_coord(lat, lng):
    if lat is None or lng is None: return False
    try:
        lat, lng = float(lat), float(lng)
        return 22.10 <= lat <= 22.60 and 113.80 <= lng <= 114.50
    except:
        return False

def get_clean_en(cached_val, new_val):
    v = new_val if new_val else cached_val
    if v and str(v).strip().lower() in ['to', '', 'null', 'none']:
        return None
    return v

def enrich_stop(stop: Dict[str, Any], truck_id: int, seq: int, coords_map: Dict[str, Any]) -> Dict[str, Any]:
    location_key = make_location_key(stop['district'], stop['location'])
    point = coords_map.get(location_key, {})
    stop['stop_id'] = f"t{truck_id}-{stop['start_date']}-{seq:02d}"
    stop['location_key'] = location_key
    
    lat = point.get('lat')
    lng = point.get('lng')
    if not is_valid_hk_coord(lat, lng):
        lat, lng = None, None
        stop['coord_status'] = 'pending'
    else:
        stop['coord_status'] = point.get('status', 'pending')
        
    stop['lat'] = lat
    stop['lng'] = lng
    stop['coord_source'] = point.get('source') if lat else None

    # Apply fallback logic, avoiding 'to'
    stop['location_en_raw'] = get_clean_en(point.get('location_en_raw'), stop.get('location_en_raw'))
    stop['location_en_core'] = get_clean_en(point.get('location_en_core'), stop.get('location_en_core'))
    stop['district_en'] = get_clean_en(point.get('district_en'), stop.get('district_en'))
    stop['location_en'] = get_clean_en(None, stop.get('location_en'))

    queries = stop.get('location_en_query_candidates', [])
    if not queries:
        queries = point.get('location_en_query_candidates', [])
        
    # filter out bad queries
    queries = [q for q in queries if q and str(q).strip().lower() not in ['to', '']]
    
    ch_query = stop.get('location', '')
    if ch_query and ch_query not in queries:
        queries.append(ch_query)
    stop['location_en_query_candidates'] = queries

    return stop

def parse_pdfs_to_schedule(pdf_zh: bytes, pdf_en: bytes, coords_data: Dict[str, Any]) -> Dict[str, Any]:
    coords_map = (coords_data or {}).get('points', {})
    zh_schedules = parse_chinese_pdf(pdf_zh)
    en_schedules = parse_english_pdf(pdf_en)

    dbg(f"Merging: zh cart1={len(zh_schedules[1])} cart2={len(zh_schedules[2])} | en cart1={len(en_schedules[1])} cart2={len(en_schedules[2])}")

    all_schedules = {1: [], 2: []}
    seqs = {1: 0, 2: 0}

    for truck_id in [1, 2]:
        zh_stops = zh_schedules.get(truck_id, [])
        en_stops = en_schedules.get(truck_id, [])
        
        if len(zh_stops) != len(en_stops):
            dbg(f"WARNING: mismatch stop count for truck {truck_id}: zh={len(zh_stops)}, en={len(en_stops)}")

        for idx, stop in enumerate(zh_stops):
            en_stop = en_stops[idx] if idx < len(en_stops) else {}
            # If en_stop doesn't exist, these will be None
            stop['district_en'] = en_stop.get('district_en')
            stop['location_en_raw'] = en_stop.get('raw_location_en')
            stop['location_en'] = en_stop.get('location_en')
            stop['location_en_core'] = en_stop.get('location_en_core')
            
            en_cands = en_stop.get('location_en_query_candidates', [])
            stop['location_en_query_candidates'] = en_cands

            seqs[truck_id] += 1
            all_schedules[truck_id].append(enrich_stop(stop, truck_id, seqs[truck_id], coords_map))

    return {
        'schema_version': 3,
        'last_updated': datetime.date.today().isoformat(),
        'source': {
            'zh': HKMA_PDF_URL_ZH,
            'en': HKMA_PDF_URL_EN,
        },
        'trucks': [
            {'id': 1, 'name': '收銀車1號', 'color': '#f0c040', 'schedules': all_schedules[1]},
            {'id': 2, 'name': '收銀車2號', 'color': '#4fc3f7', 'schedules': all_schedules[2]}
        ]
    }

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)
    dbg('=== update_data.py START ===')
    fc = fetch_geojson()
    compute_centroids(fc)
    coords_data = load_location_coords()
    dbg(f'Existing coords keys: {len(coords_data.get("points", {}))}')
    pdf_zh = http_get(HKMA_PDF_URL_ZH)
    dbg(f'Downloaded ZH PDF: {len(pdf_zh)} bytes')
    pdf_en = http_get(HKMA_PDF_URL_EN)
    dbg(f'Downloaded EN PDF: {len(pdf_en)} bytes')
    schedule = parse_pdfs_to_schedule(pdf_zh, pdf_en, coords_data)
    with open(OUT_SCHEDULE, 'w', encoding='utf-8') as f:
        json.dump(schedule, f, ensure_ascii=False, indent=2)
    dbg('=== update_data.py END ===')

if __name__ == '__main__':
    main()