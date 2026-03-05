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
    if not geom: return pts
    t = geom.get('type')
    coords = geom.get('coordinates')
    if t == 'Polygon':
        for ring in coords or []:
            for lon, lat in ring: pts.append((lon, lat))
    elif t == 'MultiPolygon':
        for poly in coords or []:
            for ring in poly:
                for lon, lat in ring: pts.append((lon, lat))
    return pts

def compute_centroids(fc: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    res = {}
    for feat in fc.get('features', []):
        name = ((feat.get('properties') or {}).get('NAME_TC') or '').strip()
        pts = extract_points(feat.get('geometry') or {})
        if not name or not pts: continue
        lon = sum(p[0] for p in pts) / len(pts)
        lat = sum(p[1] for p in pts) / len(pts)
        res[name] = {'lat': lat, 'lng': lon}
    with open(OUT_CENTROIDS, 'w', encoding='utf-8') as f:
        json.dump(res, f, ensure_ascii=False, indent=2)
    return res

def parse_pdf_to_schedule(pdf_bytes: bytes) -> Dict[str, Any]:
    os.makedirs('tmp', exist_ok=True)
    pdf_path = os.path.join('tmp', 'hkma.pdf')
    with open(pdf_path, 'wb') as f:
        f.write(pdf_bytes)

    all_schedules = {1: [], 2: []}
    year = 2026
    
    def process_cell(cell_text, outer_start, outer_end):
        if not cell_text or "暫停服務" in cell_text: return []
        
        lines = [l.strip() for l in cell_text.split('\n') if l.strip()]
        if not lines: return []
        
        district = lines[0].replace(' ', '')
        if not district.endswith("區"): return []
        
        blocks = []
        current_lines = []
        
        for l in lines[1:]:
            l_clean = l.replace(' ', '')
            m_range = re.search(r'(\d{1,2})月(\d{1,2})日.*?至.*?(\d{1,2})月(\d{1,2})日', l_clean)
            if m_range:
                d_start = datetime.date(year, int(m_range.group(1)), int(m_range.group(2))).isoformat()
                d_end = datetime.date(year, int(m_range.group(3)), int(m_range.group(4))).isoformat()
                blocks.append({
                    'lines': current_lines,
                    'start_date': d_start,
                    'end_date': d_end
                })
                current_lines = []
            else:
                current_lines.append(l)
                
        if current_lines:
            blocks.append({
                'lines': current_lines,
                'start_date': outer_start,
                'end_date': outer_end
            })
            
        global_susp_dates = []
        for l in lines[1:]:
            l_clean = l.replace(' ', '')
            if "暫停" in l_clean or re.match(r'^\(\d{1,2}月\d{1,2}日', l_clean):
                for m in re.finditer(r'(\d{1,2})月(\d{1,2})日', l_clean):
                    try: global_susp_dates.append(datetime.date(year, int(m.group(1)), int(m.group(2))).isoformat())
                    except ValueError: pass
        global_susp_dates = sorted(list(set(global_susp_dates)))

        schedules = []
        for b in blocks:
            loc_str = []
            for l in b['lines']:
                l_clean = l.replace(' ', '')
                if "暫停" not in l_clean and not re.match(r'^\(\d{1,2}月\d{1,2}日', l_clean):
                    loc_str.append(l)
            
            loc = " ".join(loc_str).replace('*', '').strip()
            loc = re.sub(r'\s+', ' ', loc)
            
            if loc:
                valid_susp = [d for d in global_susp_dates if b['start_date'] <= d <= b['end_date']]
                schedules.append({
                    'district': district,
                    'location': loc,
                    'start_date': b['start_date'],
                    'end_date': b['end_date'],
                    'suspended_dates': valid_susp
                })
                
        return schedules

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table: continue
            
            for row in table:
                if not row or len(row) < 5: continue
                date1_col, truck1_col = row[0], row[2] if len(row) > 2 else None
                date2_col, truck2_col = row[3] if len(row) > 3 else None, row[4] if len(row) > 4 else None
                
                if date1_col and truck1_col and isinstance(date1_col, str):
                    dates = re.findall(r'(\d{1,2})月(\d{1,2})日', date1_col.replace(' ', ''))
                    if len(dates) >= 2:
                        d_start = datetime.date(year, int(dates[0][0]), int(dates[0][1])).isoformat()
                        d_end = datetime.date(year, int(dates[-1][0]), int(dates[-1][1])).isoformat()
                        for cell_data in process_cell(truck1_col, d_start, d_end):
                            cell_data['service_hours'] = SERVICE_HOURS_DEFAULT
                            all_schedules[1].append(cell_data)

                if date2_col and truck2_col and isinstance(date2_col, str):
                    dates = re.findall(r'(\d{1,2})月(\d{1,2})日', date2_col.replace(' ', ''))
                    if len(dates) >= 2:
                        d_start = datetime.date(year, int(dates[0][0]), int(dates[0][1])).isoformat()
                        d_end = datetime.date(year, int(dates[-1][0]), int(dates[-1][1])).isoformat()
                        for cell_data in process_cell(truck2_col, d_start, d_end):
                            cell_data['service_hours'] = SERVICE_HOURS_DEFAULT
                            all_schedules[2].append(cell_data)

    return {
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
    pdf_bytes = http_get(HKMA_PDF_URL)
    schedule = parse_pdf_to_schedule(pdf_bytes)
    with open(OUT_SCHEDULE, 'w', encoding='utf-8') as f:
        json.dump(schedule, f, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    main()