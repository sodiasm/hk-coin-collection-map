import os, re, json, math, datetime
from typing import Dict, Any, List, Tuple
import requests
import pdfplumber
from bs4 import BeautifulSoup

CSDI_WFS_URL = os.getenv('CSDI_WFS_URL', 'https://portal.csdi.gov.hk/server/services/common/had_rcd_1634523272907_75218/MapServer/WFSServer?service=wfs&request=GetFeature&typenames=DCD&outputFormat=geojson&count=200')
HKMA_PDF_URL = os.getenv('HKMA_PDF_URL', 'https://www.hkma.gov.hk/media/chi/doc/key-functions/monetary-stability/notes-and-coins/chi_coin_collection.pdf')

OUT_DIR = 'data'
OUT_GEOJSON = os.path.join(OUT_DIR, 'hk-districts.geojson')
OUT_CENTROIDS = os.path.join(OUT_DIR, 'district_centroids.json')
OUT_SCHEDULE = os.path.join(OUT_DIR, 'schedule.json')

SERVICE_HOURS_DEFAULT = '10:00-19:00'

def http_get(url: str) -> bytes:
    r = requests.get(url, timeout=60, headers={'User-Agent':'Mozilla/5.0'})
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

def extract_points(geom: Dict[str, Any]) -> List[Tuple[float,float]]:
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

def iso_date(year: int, month: int, day: int) -> str:
    return datetime.date(year, month, day).isoformat()

def parse_suspended_dates(text: str, year: int, current_month: int) -> List[str]:
    # Matches like: (3 月3 日星期二暫停) or (1 月26 日星期一及 1 月28 日星期三暫停)
    dates = []
    for m in re.finditer(r'(\d{1,2})\s*月\s*(\d{1,2})\s*日', text):
        mo = int(m.group(1))
        da = int(m.group(2))
        try:
            dates.append(iso_date(year, mo, da))
        except Exception:
            pass
    return sorted(set(dates))

def normalize_text(s: str) -> str:
    s = s.replace('\u00a0', ' ')
    s = re.sub(r'[ \t]+', ' ', s)
    s = re.sub(r'\n+', '\n', s)
    return s.strip()

def extract_pdf_text(pdf_path: str) -> str:
    chunks = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ''
            chunks.append(t)
    return normalize_text('\n'.join(chunks))

def parse_pdf_to_schedule(pdf_bytes: bytes) -> Dict[str, Any]:
    os.makedirs('tmp', exist_ok=True)
    pdf_path = os.path.join('tmp', 'hkma.pdf')
    with open(pdf_path, 'wb') as f:
        f.write(pdf_bytes)

    text = extract_pdf_text(pdf_path)

    # Determine year (default 2026; the PDF is "自2026年起")
    year = 2026

    # Split roughly by truck columns markers that sometimes appear in extracted text.
    # Strategy: find repeating blocks starting with a date-range then district then location.

    # Pattern for a date range: "3 月2 日（星期一）至 3 月8 日（星期日）"
    range_pat = re.compile(r'(\d{1,2})\s*月\s*(\d{1,2})\s*日（[^）]+）\s*至\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日（[^）]+）')

    # Heuristic: the PDF text includes both trucks; we parse by scanning and assigning to current truck section based on "收銀車1" / "收銀車2" markers.
    lines = [ln.strip() for ln in text.split('\n') if ln.strip()]

    current_truck = None
    trucks = {1: [], 2: []}

    i = 0
    while i < len(lines):
        ln = lines[i]
        if '收銀車1' in ln:
            current_truck = 1
            i += 1
            continue
        if '收銀車2' in ln:
            current_truck = 2
            i += 1
            continue

        m = range_pat.search(ln)
        if m and current_truck in (1,2):
            m1, d1, m2, d2 = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
            start = iso_date(year, m1, d1)
            end = iso_date(year, m2, d2)

            # Next non-empty line should be district or "暫停服務"
            district = None
            location_parts = []
            susp = []

            j = i + 1
            if j < len(lines):
                district = lines[j]
                j += 1

            if district and '暫停服務' in district:
                # Skip this block
                i = j
                continue

            # Collect location lines until next date range or truck marker
            while j < len(lines):
                if '收銀車1' in lines[j] or '收銀車2' in lines[j]:
                    break
                if range_pat.search(lines[j]):
                    break
                location_parts.append(lines[j])
                j += 1

            raw_loc = ' '.join(location_parts)
            raw_loc = re.sub(r'\(\*.*?\)', '', raw_loc)
            susp = parse_suspended_dates(raw_loc, year, m1)
            raw_loc = re.sub(r'\(.*?暫停.*?\)', '', raw_loc)
            raw_loc = raw_loc.replace('*','').strip(' -')

            if district and raw_loc:
                trucks[current_truck].append({
                    'district': district,
                    'location': raw_loc,
                    'start_date': start,
                    'end_date': end,
                    'service_hours': SERVICE_HOURS_DEFAULT,
                    'suspended_dates': susp
                })

            i = j
            continue

        i += 1

    schedule = {
        'last_updated': datetime.date.today().isoformat(),
        'source': HKMA_PDF_URL,
        'trucks': [
            {'id': 1, 'name': '收銀車1號', 'color': '#f0c040', 'schedules': trucks[1]},
            {'id': 2, 'name': '收銀車2號', 'color': '#4fc3f7', 'schedules': trucks[2]}
        ]
    }
    return schedule

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    fc = fetch_geojson()
    compute_centroids(fc)

    pdf_bytes = http_get(HKMA_PDF_URL)
    schedule = parse_pdf_to_schedule(pdf_bytes)

    with open(OUT_SCHEDULE, 'w', encoding='utf-8') as f:
        json.dump(schedule, f, ensure_ascii=False, indent=2)

    print(f"Updated: {OUT_GEOJSON}, {OUT_CENTROIDS}, {OUT_SCHEDULE}")
    print(f"Truck1 entries: {len(schedule['trucks'][0]['schedules'])}")
    print(f"Truck2 entries: {len(schedule['trucks'][1]['schedules'])}")

if __name__ == '__main__':
    main()