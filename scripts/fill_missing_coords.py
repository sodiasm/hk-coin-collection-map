import json
import time
from pathlib import Path
from datetime import date
import requests

SCHEDULE_PATH = Path('data/schedule.json')
COORDS_PATH = Path('data/location_coords.json')
NOMINATIM_URL = 'https://nominatim.openstreetmap.org/search'
MAX_PER_RUN = 20
SLEEP_SECONDS = 1.2
HEADERS = {
    'User-Agent': 'hk-coin-collection-map/1.0 (github-actions)'
}


def load_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding='utf-8'))


def save_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')


def geocode(query: str):
    r = requests.get(
        NOMINATIM_URL,
        params={
            'q': query,
            'format': 'jsonv2',
            'limit': 1,
            'accept-language': 'zh-HK'
        },
        headers=HEADERS,
        timeout=30
    )
    r.raise_for_status()
    rows = r.json()
    if not rows:
        return None
    return {
        'lat': float(rows[0]['lat']),
        'lng': float(rows[0]['lon'])
    }


def main():
    schedule = load_json(SCHEDULE_PATH, {})
    coords = load_json(COORDS_PATH, {
        'schema_version': 1,
        'updated_at': None,
        'points': {}
    })

    points = coords.setdefault('points', {})
    changed = False

    wanted = {}
    for truck in schedule.get('trucks', []):
        for stop in truck.get('schedules', []):
            key = stop.get('location_key')
            if not key:
                continue
            wanted[key] = {
                'district': stop.get('district', ''),
                'location': stop.get('location', '')
            }

    for key, meta in wanted.items():
        if key not in points:
            points[key] = {
                'lat': None,
                'lng': None,
                'label': meta['location'],
                'source': 'nominatim',
                'status': 'pending'
            }
            changed = True

    pending_keys = [
        key for key, item in points.items()
        if item.get('lat') is None or item.get('lng') is None
    ][:MAX_PER_RUN]

    for key in pending_keys:
        district, location = key.split('|', 1)
        query = f'{location}, {district}, Hong Kong'
        try:
            result = geocode(query)
            if result:
                points[key]['lat'] = result['lat']
                points[key]['lng'] = result['lng']
                points[key]['source'] = 'nominatim'
                points[key]['status'] = 'auto'
                changed = True
        except Exception:
            pass
        time.sleep(SLEEP_SECONDS)

    if changed:
        coords['updated_at'] = date.today().isoformat()
        save_json(COORDS_PATH, coords)


if __name__ == '__main__':
    main()
