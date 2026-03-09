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
EN_DATE_LINE_RE = re.compile(r'^(\d{1,2})\s+([A-Za-z]{3,9})\s*\([^)]*\)\s*(?:to)?$', re.I)
EN_DISTRICT_RE = re.compile(r'.+ District$', re.I)
EN_SUSP_LINE_RE = re.compile(r'^\(?Service suspended', re.I)
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
    return MONTHS[month_name.strip().lower()]


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


def clean_core_en_location(text: str) -> str:
    s = normalize_en_display_text(text)
    s = re.sub(r'\([^)]*\)', '', s)
    s = CORE_PREFIX_RE_EN.sub('', s)
    s = re.sub(r'\s*,\s*', ', ', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip(' ,.-')


def build_en_query_candidates(raw_location_en: str, location_en_core: str, district_en: str) -> List[str]:
    candidates: List[str] = []

    def add(value: str):
        value = normalize_en_display_text(value)
        value = re.sub(r'\s*,\s*', ', ', value)
        value = re.sub(r'\s+', ' ', value).strip(' ,.-')
        if value and value not in candidates:
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
        if district_en:
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


def parse_english_date_range(lines: List[str], index: int, year: int):
    if index + 1 >= len(lines):
        return None
    m1 = EN_DATE_LINE_RE.match(lines[index])
    m2 = EN_DATE_LINE_RE.match(lines[index + 1])
    if not (m1 and m2):
        return None
    start = parse_iso_date(year, month_to_number(m1.group(2)), int(m1.group(1)))
    end = parse_iso_date(year, month_to_number(m2.group(2)), int(m2.group(1)))
    return start, end, index + 2


def extract_suspension_dates_en(lines: List[str], year: int) -> List[str]:
    dates = []
    text = ' '.join(lines)
    for day, month in re.findall(r'(\d{1,2})\s+([A-Za-z]{3,9})', text):
        try:
            dates.append(parse_iso_date(year, month_to_number(month), int(day)))
        except Exception:
            pass
    return sorted(set(dates))


def clean_english_lines(lines: List[str]) -> List[str]:
    cleaned = []
    for line in lines:
        line = normalize_en_display_text(line)
        if not line:
            continue
        if line in {'2026', 'Date', 'Coin Cart No. 1', 'Coin Cart No. 2', 'to'}:
            continue
        if line.startswith('Coin Cart Schedule') or line.startswith('Service hours:') or line.startswith('(* denotes'):
            continue
        cleaned.append(line)
    return cleaned


def parse_english_block(district_en: str, outer_start: str, outer_end: str, body_lines: List[str], year: int) -> List[Dict[str, Any]]:
    if not body_lines:
        return []
    suspension_lines = [line for line in body_lines if EN_SUSP_LINE_RE.match(line)]
    suspension_dates = extract_suspension_dates_en(suspension_lines, year)
    work_lines = [line for line in body_lines if not EN_SUSP_LINE_RE.match(line)]

    stops = []
    current_loc: List[str] = []
    i = 0
    while i < len(work_lines):
        date_info = parse_english_date_range(work_lines, i, year)
        if date_info:
            start_date, end_date, next_index = date_info
            location_en = normalize_en_display_text(' '.join(current_loc))
            if location_en:
                core = clean_core_en_location(location_en)
                valid_susp = [d for d in suspension_dates if start_date <= d <= end_date]
                stops.append({
                    'district_en': district_en,
                    'location_en': location_en,
                    'raw_location_en': location_en,
                    'location_en_core': core,
                    'location_en_query_candidates': build_en_query_candidates(location_en, core, district_en),
                    'start_date': start_date,
                    'end_date': end_date,
                    'suspended_dates': valid_susp,
                    'service_hours': SERVICE_HOURS_DEFAULT
                })
            current_loc = []
            i = next_index
            continue
        current_loc.append(work_lines[i])
        i += 1

    if not stops and current_loc:
        location_en = normalize_en_display_text(' '.join(current_loc))
        if location_en:
            core = clean_core_en_location(location_en)
            valid_susp = [d for d in suspension_dates if outer_start <= d <= outer_end]
            stops.append({
                'district_en': district_en,
                'location_en': location_en,
                'raw_location_en': location_en,
                'location_en_core': core,
                'location_en_query_candidates': build_en_query_candidates(location_en, core, district_en),
                'start_date': outer_start,
                'end_date': outer_end,
                'suspended_dates': valid_susp,
                'service_hours': SERVICE_HOURS_DEFAULT
            })
    return stops


def parse_english_column_text(text: str, year: int, debug_label: str = '') -> List[Dict[str, Any]]:
    raw_lines = [line for line in (text or '').split('\n')]
    lines = clean_english_lines(raw_lines)
    dbg(f'parse_english_column_text [{debug_label}] raw_lines={len(raw_lines)} cleaned_lines={len(lines)}')
    # DEBUG: show first 30 cleaned lines
    for idx, ln in enumerate(lines[:30]):
        dbg(f'  cleaned[{idx}]: {repr(ln)}')
    schedules: List[Dict[str, Any]] = []
    i = 0
    while i < len(lines):
        date_info = parse_english_date_range(lines, i, year)
        if not date_info:
            i += 1
            continue
        outer_start, outer_end, i = date_info
        if i >= len(lines):
            break
        if EN_SUSP_LINE_RE.match(lines[i]):
            while i < len(lines):
                nxt = parse_english_date_range(lines, i, year)
                if nxt:
                    break
                i += 1
            continue
        district_en = lines[i]
        if not EN_DISTRICT_RE.match(district_en):
            dbg(f'  SKIP non-district line after date range: {repr(district_en)}')
            continue
        i += 1
        block_lines: List[str] = []
        while i < len(lines):
            nxt = parse_english_date_range(lines, i, year)
            if nxt:
                break
            block_lines.append(lines[i])
            i += 1
        schedules.extend(parse_english_block(district_en, outer_start, outer_end, block_lines, year))
    dbg(f'parse_english_column_text [{debug_label}] => {len(schedules)} stops')
    return schedules


def parse_english_pdf(pdf_bytes: bytes) -> Dict[int, List[Dict[str, Any]]]:
    os.makedirs('tmp', exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)
    pdf_path = os.path.join('tmp', 'hkma_en.pdf')
    with open(pdf_path, 'wb') as f:
        f.write(pdf_bytes)

    year = 2026
    all_schedules = {1: [], 2: []}
    with pdfplumber.open(pdf_path) as pdf:
        dbg(f'EN PDF total pages: {len(pdf.pages)}')
        for page in pdf.pages:
            pnum = page.page_number
            mid = page.width / 2
            left_text = page.crop((0, 0, mid, page.height)).extract_text(x_tolerance=2, y_tolerance=3) or ''
            right_text = page.crop((mid, 0, page.width, page.height)).extract_text(x_tolerance=2, y_tolerance=3) or ''
            # Save raw page text to debug files
            with open(os.path.join(DEBUG_DIR, f'en_page{pnum}_left.txt'), 'w', encoding='utf-8') as df:
                df.write(left_text)
            with open(os.path.join(DEBUG_DIR, f'en_page{pnum}_right.txt'), 'w', encoding='utf-8') as df:
                df.write(right_text)
            dbg(f'EN page {pnum}: left_chars={len(left_text)} right_chars={len(right_text)}')
            all_schedules[1].extend(parse_english_column_text(left_text, year, debug_label=f'p{pnum}_left'))
            all_schedules[2].extend(parse_english_column_text(right_text, year, debug_label=f'p{pnum}_right'))

    dbg(f'parse_english_pdf total => cart1={len(all_schedules[1])} cart2={len(all_schedules[2])}')
    # Save parsed EN schedules to debug
    with open(os.path.join(DEBUG_DIR, 'en_schedules.json'), 'w', encoding='utf-8') as df:
        json.dump({str(k): v for k, v in all_schedules.items()}, df, ensure_ascii=False, indent=2)
    return all_schedules


def parse_chinese_pdf(pdf_bytes: bytes) -> Dict[int, List[Dict[str, Any]]]:
    os.makedirs('tmp', exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)
    pdf_path = os.path.join('tmp', 'hkma_zh.pdf')
    with open(pdf_path, 'wb') as f:
        f.write(pdf_bytes)

    coords_placeholder = {1: [], 2: []}
    year = 2026
    with pdfplumber.open(pdf_path) as pdf:
        dbg(f'ZH PDF total pages: {len(pdf.pages)}')
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                dbg(f'ZH page {page.page_number}: no table found')
                continue
            dbg(f'ZH page {page.page_number}: table rows={len(table)}')
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

    dbg(f'parse_chinese_pdf total => cart1={len(coords_placeholder[1])} cart2={len(coords_placeholder[2])}')
    # Save parsed ZH schedules to debug
    with open(os.path.join(DEBUG_DIR, 'zh_schedules.json'), 'w', encoding='utf-8') as df:
        json.dump({str(k): v for k, v in coords_placeholder.items()}, df, ensure_ascii=False, indent=2)
    return coords_placeholder


def enrich_stop(stop: Dict[str, Any], truck_id: int, seq: int, coords_map: Dict[str, Any]) -> Dict[str, Any]:
    location_key = make_location_key(stop['district'], stop['location'])
    point = coords_map.get(location_key, {})
    stop['stop_id'] = f"t{truck_id}-{stop['start_date']}-{seq:02d}"
    stop['location_key'] = location_key
    stop['lat'] = point.get('lat')
    stop['lng'] = point.get('lng')
    stop['coord_status'] = point.get('status', 'pending')
    stop['coord_source'] = point.get('source')
    stop['location_en_raw'] = point.get('location_en_raw', stop.get('location_en_raw'))
    stop['location_en_core'] = point.get('location_en_core', stop.get('location_en_core'))
    stop['location_en_query_candidates'] = point.get('location_en_query_candidates', stop.get('location_en_query_candidates', []))
    stop['district_en'] = point.get('district_en', stop.get('district_en'))
    return stop


def parse_pdfs_to_schedule(pdf_zh: bytes, pdf_en: bytes, coords_data: Dict[str, Any]) -> Dict[str, Any]:
    coords_map = (coords_data or {}).get('points', {})
    zh_schedules = parse_chinese_pdf(pdf_zh)
    en_schedules = parse_english_pdf(pdf_en)

    dbg(f'Merging: zh cart1={len(zh_schedules[1])} cart2={len(zh_schedules[2])} | en cart1={len(en_schedules[1])} cart2={len(en_schedules[2])}')

    all_schedules = {1: [], 2: []}
    seqs = {1: 0, 2: 0}

    for truck_id in [1, 2]:
        zh_stops = zh_schedules.get(truck_id, [])
        en_stops = en_schedules.get(truck_id, [])
        for idx, stop in enumerate(zh_stops):
            en_stop = en_stops[idx] if idx < len(en_stops) else {}
            stop['district_en'] = en_stop.get('district_en')
            stop['location_en_raw'] = en_stop.get('raw_location_en')
            stop['location_en'] = en_stop.get('location_en')
            stop['location_en_core'] = en_stop.get('location_en_core')
            stop['location_en_query_candidates'] = en_stop.get('location_en_query_candidates', [])
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
