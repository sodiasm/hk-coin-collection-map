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
    year = 2026 # HKMA current schedule year
    
    def parse_cell(cell_text):
        if not cell_text or "暫停服務" in cell_text: return None
        lines = [l.strip() for l in cell_text.split('\n') if l.strip()]
        if len(lines) < 2: return None
        
        district = lines[0].replace(' ', '')
        if not district.endswith("區"): return None
        
        loc_lines = []
        susp = []
        
        for l in lines[1:]:
            l_clean = l.replace(' ', '')
            
            if "暫停" in l_clean:
                for m in re.finditer(r'(\d{1,2})月(\d{1,2})日', l_clean):
                    try:
                        susp.append(datetime.date(year, int(m.group(1)), int(m.group(2))).isoformat())
                    except ValueError: pass
                continue
                
            if re.search(r'\d{1,2}月\d{1,2}日至', l_clean):
                continue
                
            loc_lines.append(l)
            
        location = " ".join(loc_lines).replace('*', '').strip()
        location = re.sub(r'\s+', ' ', location)
        
        return {
            'district': district,
            'location': location,
            'suspended_dates': sorted(list(set(susp)))
        }

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table: continue
            
            for row in table:
                if not row or len(row) < 5: continue
                
                # Column mapping based on HKMA PDF structure
                date1_col = row[0]
                truck1_col = row[2] if len(row) > 2 else None
                date2_col = row[3] if len(row) > 3 else None
                truck2_col = row[4] if len(row) > 4 else None
                
                if date1_col and truck1_col and isinstance(date1_col, str):
                    d_clean = date1_col.replace(' ', '')
                    dates = re.findall(r'(\d{1,2})月(\d{1,2})日', d_clean)
                    if len(dates) >= 2:
                        d_start = datetime.date(year, int(dates[0][0]), int(dates[0][1])).isoformat()
                        d_end = datetime.date(year, int(dates[-1][0]), int(dates[-1][1])).isoformat()
                        cell_data = parse_cell(truck1_col)
                        if cell_data:
                            cell_data['start_date'] = d_start
                            cell_data['end_date'] = d_end
                            cell_data['service_hours'] = SERVICE_HOURS_DEFAULT
                            all_schedules[1].append(cell_data)

                if date2_col and truck2_col and isinstance(date2_col, str):
                    d_clean = date2_col.replace(' ', '')
                    dates = re.findall(r'(\d{1,2})月(\d{1,2})日', d_clean)
                    if len(dates) >= 2:
                        d_start = datetime.date(year, int(dates[0][0]), int(dates[0][1])).isoformat()
                        d_end = datetime.date(year, int(dates[-1][0]), int(dates[-1][1])).isoformat()
                        cell_data = parse_cell(truck2_col)
                        if cell_data:
                            cell_data['start_date'] = d_start
                            cell_data['end_date'] = d_end
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
    
    print("Fetching GeoJSON...")
    fc = fetch_geojson()
    compute_centroids(fc)

    print("Fetching PDF...")
    pdf_bytes = http_get(HKMA_PDF_URL)
    print("Parsing PDF...")
    schedule = parse_pdf_to_schedule(pdf_bytes)

    with open(OUT_SCHEDULE, 'w', encoding='utf-8') as f:
        json.dump(schedule, f, ensure_ascii=False, indent=2)

    print(f"✅ Truck1 entries: {len(schedule['trucks'][0]['schedules'])}")
    print(f"✅ Truck2 entries: {len(schedule['trucks'][1]['schedules'])}")

if __name__ == '__main__':
    main()