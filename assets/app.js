const COLORS = { t1: '#f0c040', t2: '#4fc3f7', both: '#b388ff', susp: '#ef5350', none: '#2b3554' };
const TODAY = new Date().toISOString().slice(0,10);
document.getElementById('today').textContent = '今日：' + TODAY.replaceAll('-', '/');

function inRange(s, d){ return d >= s.start_date && d <= s.end_date; }
function isSuspended(s, d){ return (s.suspended_dates || []).includes(d); }

function districtNameFromFeature(f){
  const p = f.properties || {};
  return p.NAME_TC || p.NAME_EN || p.ENAME || p.CNAME_C || '未知';
}

function buildIndex(schedule){
  const byDistrict = new Map();
  for (const t of schedule.trucks){
    for (const s of t.schedules){
      const key = s.district;
      if (!byDistrict.has(key)) byDistrict.set(key, []);
      byDistrict.get(key).push({ ...s, truck_id: t.id, truck_name: t.name, truck_color: t.color });
    }
  }
  return byDistrict;
}

function todayEntries(schedule){
  const res = [];
  for (const t of schedule.trucks){
    for (const s of t.schedules){
      if (inRange(s, TODAY)){
        res.push({ ...s, truck_id: t.id, truck_name: t.name, susp: isSuspended(s, TODAY) });
      }
    }
  }
  return res;
}

function renderToday(list){
  const el = document.getElementById('todayList');
  if (!list.length){ el.innerHTML = '<div class="muted">今日無服務</div>'; return; }
  el.innerHTML = list.map(s => {
    const badge = s.susp ? '<span class="badge susp">今日暫停</span>' : '<span class="badge ok">服務中</span>';
    return `
      <div class="card">
        <div class="name">${s.truck_name}</div>
        ${badge}
        <div class="meta">區：${s.district}<br>地點：${s.location}<br>時間：${s.service_hours || '10:00-19:00'}</div>
      </div>`;
  }).join('');
}

function districtStatus(entries){
  if (!entries || !entries.length) return 'none';
  const active = entries.filter(e => inRange(e, TODAY));
  if (!active.length) return 'none';
  if (active.some(e => isSuspended(e, TODAY))) return 'susp';
  const has1 = active.some(e => e.truck_id === 1);
  const has2 = active.some(e => e.truck_id === 2);
  if (has1 && has2) return 'both';
  if (has1) return 't1';
  if (has2) return 't2';
  return 'none';
}

(async function main(){
  const [schedule, districts, centroids] = await Promise.all([
    fetch('data/schedule.json', {cache:'no-store'}).then(r=>r.json()),
    fetch('data/hk-districts.geojson', {cache:'no-store'}).then(r=>r.json()),
    fetch('data/district_centroids.json', {cache:'no-store'}).then(r=>r.json()).catch(()=>({}))
  ]);

  const byDistrict = buildIndex(schedule);
  const todays = todayEntries(schedule);
  renderToday(todays);

  const map = L.map('map').setView([22.35, 114.15], 11);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: '© OpenStreetMap contributors' }).addTo(map);

  const markers = [];
  for (const e of todays){
    if (e.susp) continue;
    const c = centroids[e.district];
    if (!c) continue;
    const icon = L.divIcon({
      className:'',
      html:`<div style="background:${e.truck_id===1?COLORS.t1:COLORS.t2};color:#000;font-weight:700;border-radius:50%;width:26px;height:26px;display:flex;align-items:center;justify-content:center;border:2px solid #fff;box-shadow:0 2px 6px rgba(0,0,0,.5)">${e.truck_id}</div>`,
      iconSize:[26,26], iconAnchor:[13,13]
    });
    markers.push(L.marker([c.lat, c.lng], {icon}).addTo(map).bindPopup(`<b>${e.truck_name}</b><br>${e.district}<br>${e.location}<br>${e.service_hours || '10:00-19:00'}`));
  }

  const layer = L.geoJSON(districts, {
    style: (f) => {
      const name = districtNameFromFeature(f);
      const st = districtStatus(byDistrict.get(name));
      return { color:'#0e1630', weight:1, fillColor: COLORS[st], fillOpacity: 0.55 };
    },
    onEachFeature: (f, l) => {
      const name = districtNameFromFeature(f);
      l.on('click', () => {
        const items = (byDistrict.get(name) || []).slice().sort((a,b)=>a.start_date.localeCompare(b.start_date));
        const html = items.length
          ? items.map(x => {
              const susp = (x.suspended_dates && x.suspended_dates.length) ? `<div style="color:#ef9a9a">暫停：${x.suspended_dates.join(', ')}</div>` : '';
              return `<div style="margin:6px 0"><b>${x.truck_id===1?'收銀車1號':'收銀車2號'}</b> ${x.start_date} ~ ${x.end_date}<br>${x.location}${susp}</div>`;
            }).join('<hr style="border:0;border-top:1px solid #223055">')
          : '本期無服務';
        l.bindPopup(`<b>${name}</b><br>${html}`).openPopup();
      });
    }
  }).addTo(map);
})();