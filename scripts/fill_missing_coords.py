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


def log(message: str):
    print(f'[fill_missing_coords] {message}', flush=True)


def load_json(path: Path, default):
    if not path.exists():
        log(f'File not found, using default: {path}')
        return default
    log(f'Loading JSON: {path}')
    return json.loads(path.read_text(encoding='utf-8'))


def save_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    log(f'Saved JSON: {path}')


def geocode(query: str):
    params = {
        'q': query,
        'format': 'jsonv2',
        'limit': 1,
        'accept-language': 'zh-HK'
    }
    r = requests.get(
        NOMINATIM_URL,
        params=params,
        headers=HEADERS,
        timeout=30
    )
    log(f'HTTP {r.status_code} | query={query} | url={r.url}')
    r.raise_for_status()
    rows = r.json()
    log(f'Result count={len(rows)} | query={query}')
    if not rows:
        return None
    top = rows[0]
    log(
        'Top result '
        f"display_name={top.get('display_name')} | lat={top.get('lat')} | lon={top.get('lon')}"
    )
    return {
        'lat': float(top['lat']),
        'lng': float(top['lon']),
        'display_name': top.get('display_name')
    }


def main():
    log('Start fill_missing_coords job')
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

    log(f'Schedule location keys found: {len(wanted)}')
    log(f'Existing coordinate keys before sync: {len(points)}')

    created_count = 0
    for key, meta in wanted.items():
        if key not in points:
            points[key] = {
                'lat': None,
                'lng': None,
                'label': meta['location'],
                'source': 'nominatim',
                'status': 'pending'
            }
            created_count += 1
            changed = True
            log(f'Added missing coord key: {key}')

    log(f'New coord keys added this run: {created_count}')

    pending_keys = [
        key for key, item in points.items()
        if item.get('lat') is None or item.get('lng') is None
    ][:MAX_PER_RUN]

    log(f'Pending keys selected this run: {len(pending_keys)} / max {MAX_PER_RUN}')

    success_count = 0
    empty_count = 0
    error_count = 0

    for index, key in enumerate(pending_keys, start=1):
        district, location = key.split('|', 1)
        query = f'{location}, {district}, Hong Kong'
        log(f'[{index}/{len(pending_keys)}] Processing key={key}')
        try:
            result = geocode(query)
            if result:
                points[key]['lat'] = result['lat']
                points[key]['lng'] = result['lng']
                points[key]['source'] = 'nominatim'
                points[key]['status'] = 'auto'
                points[key]['matched_address'] = result.get('display_name')
                changed = True
                success_count += 1
                log(f'SUCCESS key={key} | lat={result["lat"]} | lng={result["lng"]}')
            else:
                empty_count += 1
                log(f'NO RESULT key={key} | query={query}')
        except Exception as e:
            error_count += 1
            log(f'ERROR key={key} | type={type(e).__name__} | message={e}')
        time.sleep(SLEEP_SECONDS)

    log(
        f'Run summary | success={success_count} | no_result={empty_count} '
        f'| error={error_count} | changed={changed}'
    )

    if changed:
        coords['updated_at'] = date.today().isoformat()
        save_json(COORDS_PATH, coords)
    else:
        log('No JSON changes to save')

    log('End fill_missing_coords job')


if __name__ == '__main__':
    main()
