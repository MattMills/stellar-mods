#!/usr/bin/env python3
"""Render the Steam workshop ingest status page.

Run every few minutes by ingest-status.timer. Writes index.html and
status.json to OUT_DIR. Folder and zip sizes are cached in STATE_DIR, so
only the first run walks the whole backlog; later runs stat what is new.
Read-only towards the ingest: it never touches the lock (holders are read
from /proc/locks) and never writes under BASE.
"""
import bisect
import datetime as dt
import html
import json
import os
import re
import subprocess
import time
import zipfile

BASE = '/zpool0/share/stellar-mods'
APPID = '281990'
QUEUE_DIR = f'{BASE}/steam_workshop_data/{APPID}'
ARCHIVE_DIR = f'{BASE}/archive_steam_workshop_data/{APPID}'
LOCK_FILE = f'{BASE}/run_periodic_ingest.lock'
LOG_DIR = f'{BASE}/logs'
TERRA_QUEUE = '/zpool0/share/terra-mods/steam_workshop_data'
# First snapshot left unprocessed when the old lock went stale on 2024-02-12;
# every archive from it onward was made by the catch-up.
BACKLOG_START = '2024-02-12T15:05:01.772316'
DATASETS = ['zpool0/share/stellar-mods', 'zpool1/postgresql']
DB = 'stellar-mods'
OUT_DIR = '/var/www/html/ingest-status'
STATE_DIR = '/var/lib/ingest-status'
SETTLE = 120          # seconds unchanged before a folder or zip size is cached
RATE_WINDOW = 6 * 3600
HISTORY_KEEP = 8 * 86400

STAGES = [
    ('steam_workshop.py', 'Steam fetch', None),
    ('postgres_mod_metadata_ingest.py', 'Metadata ingest', 'postgres_mod_metadata_ingest.py'),
    ('zip', 'Archive snapshot', None),
    ('postgres_preview_download.py', 'Preview download', 'postgres_preview_download.py'),
    ('postgres_mod_download.py', 'Mod download', 'postgres_mod_download.py'),
    ('postgres_mod_file_checksum.py', 'File checksums', 'postgres_mod_file_checksum.py'),
    ('postgres_mod_creator_refresh.py', 'Creator refresh (daily)', 'postgres_mod_creator_refresh.py'),
]

SQL = r"""
select json_build_object(
  'db_size', pg_database_size(current_database()),
  'db', (select row_to_json(d) from (
      select numbackends, xact_commit, xact_rollback, blks_read, blks_hit,
             tup_inserted, tup_updated, tup_deleted, deadlocks, stats_reset
      from pg_stat_database where datname = current_database()) d),
  'tables', (select json_agg(t) from (
      select relname, n_live_tup, n_dead_tup, n_tup_ins, n_tup_upd,
             pg_total_relation_size(relid) as bytes,
             greatest(last_autovacuum, last_vacuum) as last_vacuum,
             greatest(last_autoanalyze, last_analyze) as last_analyze
      from pg_stat_user_tables
      order by n_tup_ins + n_tup_upd + n_tup_del > 0 desc, pg_total_relation_size(relid) desc
      limit 12) t),
  'queue', (select json_agg(q) from (
      select ft.type, count(*) as files, coalesce(sum(f.file_size::bigint), 0) as bytes
      from files f join file_types ft on ft.id = f.file_type
      where f.fetch_time is null
      group by ft.type order by 3 desc, 2 desc) q),
  'last_fetch', (select max(fetch_time) from files),
  'sessions', (select json_agg(s) from (
      select coalesce(state, 'unknown') as state, count(*) as n,
             max(extract(epoch from now() - query_start))::int as longest_s
      from pg_stat_activity
      where datname = current_database() and pid <> pg_backend_pid()
      group by 1 order by 2 desc) s)
)
"""


# ---------------------------------------------------------------- collection

def snap_ts(name):
    """Snapshot folder/zip names are UTC isoformat timestamps."""
    try:
        return dt.datetime.fromisoformat(name).replace(tzinfo=dt.timezone.utc).timestamp()
    except ValueError:
        return None


def tree_size(path):
    apparent = disk = files = 0
    with os.scandir(path) as it:
        for e in it:
            if e.is_dir(follow_symlinks=False):
                a, d, n = tree_size(e.path)
                apparent, disk, files = apparent + a, disk + d, files + n
            elif e.is_file(follow_symlinks=False):
                st = e.stat(follow_symlinks=False)
                apparent += st.st_size
                disk += st.st_blocks * 512
                files += 1
    return apparent, disk, files


def scan_queue(cache, now):
    """Every snapshot folder still waiting: {name: {mtime, apparent, disk, files}}."""
    current, keep = {}, {}
    with os.scandir(QUEUE_DIR) as it:
        for e in it:
            if not e.is_dir(follow_symlinks=False):
                continue
            try:
                mtime = e.stat(follow_symlinks=False).st_mtime
                rec = cache.get(e.name)
                if not rec or rec['mtime'] != mtime:
                    a, d, n = tree_size(e.path)
                    rec = {'mtime': mtime, 'apparent': a, 'disk': d, 'files': n}
            except FileNotFoundError:  # zipped away mid-scan
                continue
            current[e.name] = rec
            if now - mtime >= SETTLE:
                keep[e.name] = rec
    return current, keep


def scan_archive(cache, now):
    """Catch-up archives: {name: {ctime, size, uncompressed, files}}.

    The ingest zips with -o, which backdates mtime to the snapshot, so the
    time a snapshot was processed is the zip's ctime.
    """
    current, keep = {}, {}
    start = BACKLOG_START + '.zip'
    with os.scandir(ARCHIVE_DIR) as it:
        for e in it:
            if not e.name.endswith('.zip') or e.name < start:
                continue
            try:
                st = e.stat(follow_symlinks=False)
                rec = cache.get(e.name)
                if not rec or rec['size'] != st.st_size or rec['ctime'] != st.st_ctime:
                    with zipfile.ZipFile(e.path) as z:
                        infos = z.infolist()
                    rec = {'ctime': st.st_ctime, 'size': st.st_size,
                           'uncompressed': sum(i.file_size for i in infos),
                           'files': len(infos)}
            except (OSError, zipfile.BadZipFile):  # still being written
                continue
            current[e.name[:-4]] = rec
            if now - st.st_ctime >= SETTLE:
                keep[e.name] = rec
    return current, keep


def newest_snapshot(root):
    best = None
    try:
        for app in os.scandir(root):
            if app.is_dir():
                for e in os.scandir(app.path):
                    if e.is_dir() and snap_ts(e.name) and (best is None or e.name > best):
                        best = e.name
    except OSError:
        pass
    return best


def lock_held(path):
    """True if anyone holds a flock on path. /proc/locks names the process that
    took the lock (the short-lived flock(1)), not the script holding it now."""
    try:
        st = os.stat(path)
    except OSError:
        return False
    key = f'{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}:{st.st_ino}'
    with open('/proc/locks') as f:
        for line in f:
            parts = line.split()
            if '->' not in parts and len(parts) >= 6 and parts[5] == key:
                return True
    return False


def ingest_processes():
    """Ingest-pipeline processes running in BASE: [{stage, label, pid, start, arg}]."""
    with open('/proc/stat') as f:
        btime = next(int(l.split()[1]) for l in f if l.startswith('btime'))
    hz = os.sysconf('SC_CLK_TCK')
    labels = {s: l for s, l, _ in STAGES}
    out = []
    for pid in os.listdir('/proc'):
        if not pid.isdigit():
            continue
        try:
            if os.readlink(f'/proc/{pid}/cwd').rstrip('/') != BASE:
                continue
            with open(f'/proc/{pid}/cmdline', 'rb') as f:
                args = [a.decode(errors='replace') for a in f.read().split(b'\0') if a]
            with open(f'/proc/{pid}/stat') as f:
                start = btime + int(f.read().rsplit(')', 1)[1].split()[19]) / hz
        except (OSError, IndexError, ValueError):
            continue
        if not args:
            continue
        exe = os.path.basename(args[0])
        stage = os.path.basename(args[1]) if exe.startswith('python') and len(args) > 1 else exe
        if stage in labels:
            out.append({'stage': stage, 'label': labels[stage], 'pid': int(pid),
                        'start': start, 'arg': args[-1] if len(args) > 2 else ''})
    return sorted(out, key=lambda p: p['start'])


def stage_last_runs():
    out = []
    for stage, label, logdir in STAGES:
        if not logdir:
            continue
        newest = None
        try:
            with os.scandir(f'{LOG_DIR}/{logdir}') as it:
                for e in it:
                    if e.name.endswith('.log') and (newest is None or e.name > newest):
                        newest = e.name
        except OSError:
            pass
        t = None
        if newest:
            try:
                t = dt.datetime.strptime(newest[:-4], '%Y-%m-%d %H:%M:%S.%f').timestamp()
            except ValueError:
                pass
        out.append({'stage': stage, 'label': label, 'last_start': t})
    return out


def zfs_usage():
    out = []
    for ds in DATASETS:
        try:
            r = subprocess.run(['zfs', 'list', '-Hp', '-o', 'name,used,avail', ds],
                               capture_output=True, text=True, timeout=30, check=True)
            name, used, avail = r.stdout.split()
            out.append({'name': name, 'used': int(used), 'avail': int(avail)})
        except (OSError, subprocess.SubprocessError, ValueError):
            out.append({'name': ds, 'used': None, 'avail': None})
    return out


def postgres():
    try:
        r = subprocess.run(['runuser', '-u', 'postgres', '--', 'psql', '-d', DB, '-XAtq', '-c', SQL],
                           capture_output=True, text=True, timeout=120, cwd='/tmp')
    except (OSError, subprocess.SubprocessError) as e:
        return {'error': str(e)}
    if r.returncode != 0:
        return {'error': r.stderr.strip()[-500:]}
    return json.loads(r.stdout)


# ---------------------------------------------------------------- state

def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def write_atomic(path, text):
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        f.write(text)
    os.chmod(tmp, 0o644)
    os.replace(tmp, path)


def load_history(now):
    rows = []
    try:
        with open(f'{STATE_DIR}/history.jsonl') as f:
            for line in f:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if now - row['t'] <= HISTORY_KEEP:
                    rows.append(row)
    except OSError:
        pass
    return rows


# ---------------------------------------------------------------- analysis

def remaining_series(now, remaining_now, proc_times, arrival_times):
    """Snapshots waiting over time, rebuilt exactly from processing and arrival times.

    remaining(T) = remaining_now + processed after T - arrived after T.
    """
    if not proc_times:
        return [[now, remaining_now]]
    start = proc_times[0] - 300
    step = max(300, (now - start) / 400)
    proc, arr = sorted(proc_times), sorted(arrival_times)
    pts, t = [], start
    while t < now:
        done_after = len(proc) - bisect.bisect_right(proc, t)
        arr_after = len(arr) - bisect.bisect_right(arr, t)
        pts.append([round(t), remaining_now + done_after - arr_after])
        t += step
    pts.append([round(now), remaining_now])
    return pts


def health(now, remaining, holder, last_done):
    age = now - last_done if last_done else None
    if remaining <= 1:
        return 'good', 'Caught up', 'Nothing waiting beyond the snapshot being fetched.'
    if holder:
        if age is not None and age < 20 * 60:
            return 'good', 'Ingesting', f'Last snapshot finished {fmt_age(age)} ago.'
        if age is not None and age < 60 * 60:
            return 'warning', 'Slow', f'Lock held, but no snapshot finished for {fmt_age(age)}.'
        return 'critical', 'Stalled', ('Lock held, but no snapshot finished for '
                                       + (fmt_age(age) if age else 'the whole catch-up') + '.')
    if age is not None and age < 75 * 60:
        return 'warning', 'Waiting', 'Lock is free; the next :05 cron run should pick the backlog up.'
    return 'critical', 'Not running', 'Backlog waiting, lock free, nothing processed recently.'


def fetch_health(now, newest):
    ts = snap_ts(newest) if newest else None
    if ts is None:
        return 'critical', 'never', None
    age = now - ts
    level = 'good' if age < 75 * 60 else 'warning' if age < 3 * 3600 else 'critical'
    return level, fmt_age(age) + ' ago', ts


# ---------------------------------------------------------------- formatting

def fmt_bytes(n):
    if n is None:
        return '–'
    for unit, size in (('TB', 1e12), ('GB', 1e9), ('MB', 1e6), ('kB', 1e3)):
        if abs(n) >= size:
            v = n / size
            return f'{v:.2f} {unit}' if v < 10 else f'{v:.1f} {unit}' if v < 100 else f'{v:.0f} {unit}'
    return f'{n:.0f} B'


def fmt_int(n):
    return '–' if n is None else f'{int(n):,}'


def fmt_age(s):
    if s is None:
        return '–'
    s = int(max(s, 0))
    if s < 90:
        return f'{s} s'
    m = s // 60
    if m < 90:
        return f'{m} min'
    h, m = divmod(m, 60)
    if h < 48:
        return f'{h} h {m} min'
    d, h = divmod(h, 24)
    return f'{d} d {h} h'


def fmt_time(t, with_date=True):
    if t is None:
        return '–'
    return time.strftime('%b %-d %Y, %H:%M %Z' if with_date else '%H:%M %Z', time.localtime(t))


def fmt_pgtime(s):
    if not s:
        return '–'
    try:
        # Postgres trims trailing zeros from microseconds; 3.10's fromisoformat wants 3 or 6 digits.
        s6 = re.sub(r'\.(\d{1,6})', lambda g: '.' + g.group(1).ljust(6, '0'), s, count=1)
        return fmt_time(dt.datetime.fromisoformat(s6).timestamp())
    except ValueError:
        return html.escape(s)


def esc(s):
    return html.escape(str(s))


ICONS = {
    'good': '<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="7" fill="var(--good)"/>'
            '<path d="M4.5 8.2l2.3 2.3 4.7-4.8" fill="none" stroke="#fff" stroke-width="1.8" '
            'stroke-linecap="round" stroke-linejoin="round"/></svg>',
    'warning': '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M8 1.5l7 12.5H1z" fill="var(--warning)"/>'
               '<path d="M8 6v3.6" stroke="#0b0b0b" stroke-width="1.7" stroke-linecap="round"/>'
               '<circle cx="8" cy="11.8" r="1" fill="#0b0b0b"/></svg>',
    'critical': '<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="7" fill="var(--critical)"/>'
                '<path d="M5.5 5.5l5 5m0-5l-5 5" stroke="#fff" stroke-width="1.8" stroke-linecap="round"/></svg>',
}


def status_chip(level, label):
    return f'<span class="chip">{ICONS[level]}<span>{esc(label)}</span></span>'


def tile(label, value, sub=''):
    sub = f'<div class="sub">{sub}</div>' if sub else ''
    return f'<div class="tile card"><div class="label">{esc(label)}</div><div class="value">{value}</div>{sub}</div>'


# ---------------------------------------------------------------- page

CSS = """
:root{color-scheme:light;--page:#f9f9f7;--surface:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--muted:#898781;
--grid:#e1e0d9;--axis:#c3c2b7;--border:rgba(11,11,11,.10);--series:#2a78d6;--series-wash:rgba(42,120,214,.10);
--track:#cde2fb;--good:#0ca30c;--warning:#fab219;--critical:#d03b3b}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){color-scheme:dark;--page:#0d0d0d;--surface:#1a1a19;
--ink:#fff;--ink2:#c3c2b7;--muted:#898781;--grid:#2c2c2a;--axis:#383835;--border:rgba(255,255,255,.10);
--series:#3987e5;--series-wash:rgba(57,135,229,.12);--track:rgba(57,135,229,.18)}}
:root[data-theme="dark"]{color-scheme:dark;--page:#0d0d0d;--surface:#1a1a19;--ink:#fff;--ink2:#c3c2b7;--muted:#898781;
--grid:#2c2c2a;--axis:#383835;--border:rgba(255,255,255,.10);--series:#3987e5;--series-wash:rgba(57,135,229,.12);--track:rgba(57,135,229,.18)}
*{box-sizing:border-box}
body{margin:0;background:var(--page);color:var(--ink);font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:1120px;margin:0 auto;padding:24px 16px 48px}
header{display:flex;flex-wrap:wrap;align-items:baseline;justify-content:space-between;gap:8px 16px;margin-bottom:16px}
h1{font-size:20px;font-weight:600;margin:0}
h2{font-size:15px;font-weight:600;margin:28px 0 10px}
.muted{color:var(--muted)} .ink2{color:var(--ink2)}
.card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 18px}
.chip{display:inline-flex;align-items:center;gap:6px;font-weight:600}
.chip svg{width:16px;height:16px;flex:none}
.hero{display:grid;gap:14px}
.hero .top{display:flex;flex-wrap:wrap;justify-content:space-between;align-items:flex-start;gap:12px 24px}
.hero .big{font-size:52px;font-weight:600;line-height:1.05;letter-spacing:-.01em}
.hero .label{color:var(--ink2)}
.state{text-align:right;max-width:420px}
.state .detail{color:var(--ink2);margin-top:2px}
.meter{height:12px;background:var(--track);border-radius:6px;overflow:hidden}
.meter span{display:block;height:100%;background:var(--series);border-radius:6px 0 0 6px;min-width:4px}
.meter-legend{display:flex;justify-content:space-between;flex-wrap:wrap;gap:4px 16px;color:var(--ink2);font-size:13px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-top:12px}
.tile .label{color:var(--ink2);font-size:13px}
.tile .value{font-size:24px;font-weight:600;margin-top:2px}
.tile .sub{color:var(--muted);font-size:12.5px;margin-top:2px}
.chart-wrap{position:relative}
#chart{height:240px}
#chart svg{display:block;overflow:visible}
.tip{position:absolute;pointer-events:none;background:var(--surface);border:1px solid var(--border);border-radius:8px;
padding:6px 9px;font-size:12.5px;box-shadow:0 2px 10px rgba(0,0,0,.12);white-space:nowrap;display:none}
.tip b{font-weight:600}
.scroll{overflow-x:auto}
table{width:100%;border-collapse:collapse}
th{text-align:left;color:var(--ink2);font-weight:500;font-size:12.5px;border-bottom:1px solid var(--axis);padding:6px 8px;white-space:nowrap}
td{padding:6px 8px;border-bottom:1px solid var(--grid);white-space:nowrap}
tr:last-child td{border-bottom:0}
.num{text-align:right;font-variant-numeric:tabular-nums}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:12px}
.grid2 h3{font-size:13px;font-weight:600;margin:0 0 8px}
details{margin-top:10px} summary{cursor:pointer;color:var(--ink2);font-size:13px}
.err{color:var(--critical);white-space:pre-wrap}
footer{margin-top:28px;color:var(--muted);font-size:12.5px}
@media (max-width:520px){.hero .big{font-size:40px}.state{text-align:left}}
"""

JS = r"""
(function(){
const S = JSON.parse(document.getElementById('series').textContent);
const box = document.getElementById('chart'), tip = document.getElementById('tip');
if (S.length < 2) { box.innerHTML = '<p class="muted">Collecting data: the chart starts once a few snapshots have been processed.</p>'; box.style.height='auto'; return; }
const fmtN = v => Math.round(v).toLocaleString();
const fmtT = (t, d) => new Date(t*1000).toLocaleString([], d ? {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'} : {hour:'2-digit', minute:'2-digit'});
function niceStep(range, n){ const raw = range / n, mag = Math.pow(10, Math.floor(Math.log10(raw)));
  for (const m of [1, 2, 2.5, 5, 10]) if (m*mag >= raw) return m*mag; return 10*mag; }
const TSTEPS = [300,900,1800,3600,7200,10800,21600,43200,86400,172800,604800];
function draw(){
  const W = box.clientWidth, H = 240, L = 58, R = 70, T = 12, B = 28, pw = W-L-R, ph = H-T-B;
  if (pw < 60) return;  // not laid out yet (hidden tab); the ResizeObserver redraws
  const t0 = S[0][0], t1 = S[S.length-1][0];
  let vmax = Math.max(...S.map(p => p[1])), vmin = Math.min(...S.map(p => p[1]));
  const ys = Math.max(1, niceStep(Math.max(vmax - vmin, 1), 4));
  const y0 = Math.max(0, Math.floor(vmin / ys) * ys), y1 = Math.ceil(vmax / ys) * ys + (vmax % ys === 0 ? ys : 0);
  const x = t => L + (t - t0) / Math.max(t1 - t0, 1) * pw, y = v => T + ph - (v - y0) / (y1 - y0) * ph;
  let g = '';
  for (let v = y0; v <= y1 + 1e-9; v += ys)
    g += `<line x1="${L}" x2="${L+pw}" y1="${y(v)}" y2="${y(v)}" stroke="var(--grid)" stroke-width="1"/>` +
         `<text x="${L-8}" y="${y(v)+4}" text-anchor="end" fill="var(--muted)" font-size="11.5">${fmtN(v)}</text>`;
  const span = t1 - t0, ts = TSTEPS.find(s => span / s <= Math.max(2, Math.floor(pw / 110))) || 604800;
  const withDate = ts >= 21600 || new Date(t0*1000).toDateString() !== new Date(t1*1000).toDateString();
  const off = new Date().getTimezoneOffset() * 60;
  for (let t = Math.ceil((t0 - off) / ts) * ts + off; t <= t1; t += ts)
    g += `<text x="${x(t)}" y="${H-8}" text-anchor="middle" fill="var(--muted)" font-size="11.5">${fmtT(t, withDate && ts >= 21600 ? 1 : 0)}</text>`;
  g += `<line x1="${L}" x2="${L+pw}" y1="${T+ph}" y2="${T+ph}" stroke="var(--axis)" stroke-width="1"/>`;
  const line = S.map((p, i) => (i ? 'L' : 'M') + x(p[0]).toFixed(1) + ' ' + y(p[1]).toFixed(1)).join('');
  const area = line + `L${x(t1).toFixed(1)} ${T+ph}L${x(t0).toFixed(1)} ${T+ph}Z`;
  const last = S[S.length-1];
  // The wash reads as magnitude, so it only belongs on a zero-based axis.
  if (y0 === 0) g += `<path d="${area}" fill="var(--series-wash)"/>`;
  g += `<path d="${line}" fill="none" stroke="var(--series)" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>` +
       `<circle cx="${x(last[0])}" cy="${y(last[1])}" r="4.5" fill="var(--series)" stroke="var(--surface)" stroke-width="2"/>` +
       `<text x="${x(last[0])+10}" y="${y(last[1])+4}" fill="var(--ink)" font-size="12.5" font-weight="600">${fmtN(last[1])}</text>` +
       `<line id="xh" x1="0" x2="0" y1="${T}" y2="${T+ph}" stroke="var(--axis)" stroke-width="1" visibility="hidden"/>` +
       `<circle id="xd" r="4.5" fill="var(--series)" stroke="var(--surface)" stroke-width="2" visibility="hidden"/>` +
       `<rect x="${L}" y="${T}" width="${pw}" height="${ph}" fill="transparent" id="hit"/>`;
  box.innerHTML = `<svg width="${W}" height="${H}" role="img" aria-label="Snapshots remaining over time">${g}</svg>`;
  const svg = box.firstChild, xh = svg.querySelector('#xh'), xd = svg.querySelector('#xd');
  svg.querySelector('#hit').addEventListener('pointermove', ev => {
    const r = svg.getBoundingClientRect(), mx = ev.clientX - r.left;
    const t = t0 + (mx - L) / pw * (t1 - t0);
    let lo = 0, hi = S.length - 1;
    while (hi - lo > 1) { const mid = (lo + hi) >> 1; if (S[mid][0] < t) lo = mid; else hi = mid; }
    const p = Math.abs(S[lo][0] - t) < Math.abs(S[hi][0] - t) ? S[lo] : S[hi];
    const px = x(p[0]), py = y(p[1]);
    xh.setAttribute('x1', px); xh.setAttribute('x2', px); xh.setAttribute('visibility', 'visible');
    xd.setAttribute('cx', px); xd.setAttribute('cy', py); xd.setAttribute('visibility', 'visible');
    tip.innerHTML = `<span class="muted">${fmtT(p[0], 1)}</span><br><b>${fmtN(p[1])}</b> snapshots waiting`;
    tip.style.display = 'block';
    const tw = tip.offsetWidth;
    tip.style.left = Math.min(Math.max(px - tw / 2, 0), W - tw) + 'px';
    tip.style.top = Math.max(py - 58, 0) + 'px';
  });
  svg.querySelector('#hit').addEventListener('pointerleave', () => {
    tip.style.display = 'none'; xh.setAttribute('visibility', 'hidden'); xd.setAttribute('visibility', 'hidden');
  });
}
let rt, lastW = -1;
new ResizeObserver(() => { if (box.clientWidth === lastW) return; lastW = box.clientWidth;
  clearTimeout(rt); rt = setTimeout(draw, 50); }).observe(box);
})();
"""


def render(m):
    h = m['health']
    q, a = m['queue'], m['archive']
    total_bytes = q['apparent'] + a['uncompressed']
    pct = 100 * a['uncompressed'] / total_bytes if total_bytes else 100
    rate = m['rate']

    procs = ''.join(
        f'<tr><td>{esc(p["label"])}</td><td class="num">{p["pid"]}</td><td>{fmt_age(m["now"] - p["start"])}</td>'
        f'<td>{esc(os.path.basename(p["arg"].rstrip("/")))}</td></tr>' for p in m['processes']) \
        or '<tr><td colspan="4" class="muted">No ingest processes running</td></tr>'
    running = {p['stage'] for p in m['processes']}
    stages = ''.join(
        f'<tr><td>{esc(s["label"])}</td><td>{fmt_time(s["last_start"])}</td>'
        f'<td class="num">{fmt_age(m["now"] - s["last_start"]) if s["last_start"] else "–"}</td>'
        f'<td>{"running" if s["stage"] in running else ""}</td></tr>' for s in m['stages'])
    fetchers = ''.join(
        f'<tr><td>{esc(f["label"])}</td><td>{status_chip(f["level"], f["age"])}</td>'
        f'<td>{esc(f["newest"] or "–")}</td></tr>' for f in m['fetchers'])

    pg = m['pg']
    if 'error' in pg:
        pg_html = f'<div class="card err">Postgres query failed: {esc(pg["error"])}</div>'
    else:
        db = pg.get('db') or {}
        hits, reads = db.get('blks_hit') or 0, db.get('blks_read') or 0
        hit = f'{100 * hits / (hits + reads):.2f}%' if hits + reads else '–'
        pgds = next((d for d in m['zfs'] if d['name'] == 'zpool1/postgresql'), {})
        sessions = pg.get('sessions') or []
        pg_tiles = ''.join([
            tile('Database size', fmt_bytes(pg.get('db_size')),
                 f'{fmt_bytes(m["db_growth"])}/day' if m['db_growth'] is not None else 'growth: needs a day of history'),
            tile('Postgres pool free', fmt_bytes(pgds.get('avail')), f'zpool1/postgresql uses {fmt_bytes(pgds.get("used"))}'),
            tile('Commits per second', f'{m["tps"]:.1f}' if m['tps'] is not None else '–',
                 f'{fmt_int(db.get("xact_commit"))} since stats reset {fmt_pgtime(db.get("stats_reset"))}'),
            tile('Cache hit rate', hit, f'{fmt_int(db.get("deadlocks"))} deadlocks, {fmt_int(db.get("xact_rollback"))} rollbacks'),
            tile('Sessions', fmt_int(sum(s['n'] for s in sessions)),
                 ', '.join(f'{s["n"]} {esc(s["state"])}' for s in sessions) or 'none'),
        ])
        qrows = ''.join(
            f'<tr><td>{esc(r["type"])}</td><td class="num">{fmt_int(r["files"])}</td>'
            f'<td class="num">{fmt_bytes(r["bytes"])}</td></tr>' for r in pg.get('queue') or []) \
            or '<tr><td colspan="3" class="muted">Empty</td></tr>'
        qtot = pg.get('queue') or []
        qrows += (f'<tr><td><b>Total</b></td><td class="num"><b>{fmt_int(sum(r["files"] for r in qtot))}</b></td>'
                  f'<td class="num"><b>{fmt_bytes(sum(r["bytes"] for r in qtot))}</b></td></tr>')
        trows = ''
        for t in pg.get('tables') or []:
            live, dead = t['n_live_tup'] or 0, t['n_dead_tup'] or 0
            dpct = 100 * dead / (live + dead) if live + dead else 0
            dcell = f'{fmt_int(dead)} <span class="muted">({dpct:.0f}%)</span>' if dead else '0'
            if dpct >= 20 and dead > 10000:
                dcell = f'{ICONS["warning"].replace("<svg", "<svg width=12 height=12")} ' + dcell
            trows += (f'<tr><td>{esc(t["relname"])}</td><td class="num">{fmt_bytes(t["bytes"])}</td>'
                      f'<td class="num">{fmt_int(live)}</td><td class="num">{dcell}</td>'
                      f'<td class="num">{fmt_int(t["n_tup_ins"])}</td><td class="num">{fmt_int(t["n_tup_upd"])}</td>'
                      f'<td>{fmt_pgtime(t["last_vacuum"])}</td><td>{fmt_pgtime(t["last_analyze"])}</td></tr>')
        pg_html = f"""
<div class="tiles" style="margin-top:0">{pg_tiles}</div>
<div class="grid2" style="margin-top:12px">
  <div class="card"><h3>Download queue <span class="muted">(files with no fetch time)</span></h3>
    <div class="scroll"><table><tr><th>Type</th><th class="num">Files</th><th class="num">Size</th></tr>{qrows}</table></div>
    <div class="muted" style="margin-top:8px;font-size:12.5px">Last file fetched {fmt_pgtime(pg.get('last_fetch'))}. The downloads run after the backlog loop finishes.</div></div>
  <div class="card"><h3>Ingest processes</h3>
    <div class="scroll"><table><tr><th>Stage</th><th class="num">PID</th><th>Running for</th><th>Target</th></tr>{procs}</table></div></div>
</div>
<div class="card" style="margin-top:12px"><h3 style="font-size:13px;margin:0 0 8px">Tables <span class="muted">(active first, then largest; counters since stats reset)</span></h3>
  <div class="scroll"><table><tr><th>Table</th><th class="num">Size</th><th class="num">Live rows</th><th class="num">Dead rows</th>
  <th class="num">Inserted</th><th class="num">Updated</th><th>Last vacuum</th><th>Last analyze</th></tr>{trows}</table></div></div>"""

    data_rows = ''.join(f'<tr><td>{fmt_time(t)}</td><td class="num">{fmt_int(v)}</td></tr>'
                        for t, v in m['series'][::-1][:48])
    eta = (f'{fmt_age(rate["eta_s"])}' if rate['eta_s'] is not None else 'Not draining')
    eta_sub = (f'around {fmt_time(m["now"] + rate["eta_s"])}' if rate['eta_s'] is not None
               else 'arrivals keep pace with processing' if rate['processed_per_h'] else 'nothing processed recently')
    reached = m['data_reached']
    ds = next((d for d in m['zfs'] if d['name'] == 'zpool0/share/stellar-mods'), {})

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="120">
<title>Steam ingest status</title><style>{CSS}</style></head>
<body><main>
<header><h1>Steam workshop ingest <span class="muted" style="font-weight:400">· stellar-mods {APPID}</span></h1>
<div class="muted">Updated {fmt_time(m['now'])} · refreshes every 5 min</div></header>

<section class="card hero">
  <div class="top">
    <div><div class="label">Remaining backlog</div><div class="big">{fmt_bytes(q['apparent'])}</div>
      <div class="ink2">{fmt_int(q['count'])} snapshots · {fmt_bytes(q['disk'])} on disk · {fmt_int(q['files'])} files</div></div>
    <div class="state">{status_chip(h['level'], h['label'])}<div class="detail">{esc(h['detail'])}</div></div>
  </div>
  <div class="meter" role="img" aria-label="{pct:.1f}% of the backlog processed"><span style="width:{pct:.2f}%"></span></div>
  <div class="meter-legend"><span><b style="color:var(--ink)">{pct:.1f}%</b> processed: {fmt_bytes(a['uncompressed'])} in {fmt_int(a['count'])} snapshots</span>
    <span>{fmt_bytes(total_bytes)} total since the stall on Feb 12 2024</span></div>
</section>

<div class="tiles">
  {tile('Processed', fmt_bytes(a['uncompressed']), f"{fmt_int(a['count'])} snapshots, zipped to {fmt_bytes(a['size'])}")}
  {tile('Throughput', f"{rate['processed_per_h']:.1f}/h", f"{rate['arrived_per_h']:.1f}/h arriving · net {rate['net_per_h']:+.1f}/h (last {fmt_age(rate['window_s'])})")}
  {tile('Time to catch up', eta, eta_sub)}
  {tile('Data reached', esc(reached[:16].replace('T', ' ')) + ' UTC' if reached else '–', 'oldest snapshot still waiting')}
  {tile('stellar-mods dataset', fmt_bytes(ds.get('used')), f"{fmt_bytes(ds.get('avail'))} free in zpool0")}
</div>

<h2>Snapshots waiting</h2>
<div class="card"><div class="chart-wrap"><div id="chart"></div><div class="tip" id="tip"></div></div>
<details><summary>Data table (latest 48 points)</summary><div class="scroll"><table><tr><th>Time</th><th class="num">Snapshots waiting</th></tr>{data_rows}</table></div></details></div>

<h2>Pipeline</h2>
<div class="grid2">
  <div class="card"><h3>Stages <span class="muted">(newest log)</span></h3><div class="scroll"><table>
    <tr><th>Stage</th><th>Last started</th><th class="num">Age</th><th></th></tr>{stages}</table></div></div>
  <div class="card"><h3>Hourly Steam fetches</h3><div class="scroll"><table>
    <tr><th>Fetcher</th><th>Newest snapshot</th><th>Name (UTC)</th></tr>{fetchers}</table></div>
    <div class="muted" style="margin-top:8px;font-size:12.5px">Ingest lock: {'held' if m['holder'] else 'free'}</div></div>
</div>

<h2>Postgres <span class="muted" style="font-weight:400">· {DB}</span></h2>
{pg_html}

<footer>Sizes are decimal (1 GB = 10⁹ bytes). Backlog sizes are uncompressed file sizes; processed = catch-up archives under
archive_steam_workshop_data. Generated by {esc(BASE)}/status/ingest_status.py in {m['elapsed']:.1f} s. Raw data: <a href="status.json" style="color:inherit">status.json</a>.</footer>
</main>
<script type="application/json" id="series">{json.dumps(m['series'])}</script>
<script>{JS}</script>
</body></html>
"""


# ---------------------------------------------------------------- main

def main():
    t_start = time.time()
    now = t_start
    os.makedirs(STATE_DIR, exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)
    cache = load_json(f'{STATE_DIR}/cache.json', {})

    folders, keep_f = scan_queue(cache.get('folders', {}), now)
    zips, keep_z = scan_archive(cache.get('zips', {}), now)
    write_atomic(f'{STATE_DIR}/cache.json', json.dumps({'folders': keep_f, 'zips': keep_z}))

    proc_times = sorted(z['ctime'] for z in zips.values())
    arrivals = [t for t in (snap_ts(n) for n in list(folders) + list(zips)) if t]
    remaining = len(folders)

    window_start = max(now - RATE_WINDOW, proc_times[0] if proc_times else now - RATE_WINDOW)
    window = max(now - window_start, 1)
    done_w = len(proc_times) - bisect.bisect_right(proc_times, window_start)
    arrived_w = sum(1 for t in arrivals if t > window_start)
    p_h, a_h = done_w / window * 3600, arrived_w / window * 3600
    net = p_h - a_h
    rate = {'window_s': window, 'processed_per_h': p_h, 'arrived_per_h': a_h, 'net_per_h': net,
            'eta_s': remaining / net * 3600 if net > 0 and remaining > 1 else (0 if remaining <= 1 else None)}

    holder = lock_held(LOCK_FILE)
    level, label, detail = health(now, remaining, holder, proc_times[-1] if proc_times else None)

    fetchers = []
    for flabel, newest in (('stellar-mods', max(folders) if folders else newest_snapshot(f'{BASE}/steam_workshop_data')),
                           ('terra-mods', newest_snapshot(TERRA_QUEUE))):
        flevel, fage, _ = fetch_health(now, newest)
        fetchers.append({'label': flabel, 'level': flevel, 'age': fage, 'newest': newest})

    pg = postgres()
    history = load_history(now)
    tps = db_growth = None
    if 'error' not in pg:
        commits = (pg.get('db') or {}).get('xact_commit')
        if history and commits is not None and history[-1].get('commits') is not None:
            d = commits - history[-1]['commits']
            if d >= 0 and now > history[-1]['t']:
                tps = d / (now - history[-1]['t'])
        day = [r for r in history if r.get('db_size') and now - r['t'] >= 3600]
        if day and pg.get('db_size'):
            ref = min(day, key=lambda r: abs((now - r['t']) - 86400))
            db_growth = (pg['db_size'] - ref['db_size']) / (now - ref['t']) * 86400
    sample = {'t': round(now), 'remaining': remaining, 'processed': len(zips),
              'remaining_bytes': sum(f['apparent'] for f in folders.values()),
              'processed_bytes': sum(z['uncompressed'] for z in zips.values()),
              'db_size': pg.get('db_size'), 'commits': (pg.get('db') or {}).get('xact_commit')}
    history.append(sample)
    write_atomic(f'{STATE_DIR}/history.jsonl', ''.join(json.dumps(r) + '\n' for r in history))

    m = {
        'now': now,
        'queue': {'count': remaining, 'apparent': sample['remaining_bytes'],
                  'disk': sum(f['disk'] for f in folders.values()),
                  'files': sum(f['files'] for f in folders.values())},
        'archive': {'count': len(zips), 'uncompressed': sample['processed_bytes'],
                    'size': sum(z['size'] for z in zips.values())},
        'rate': rate,
        'health': {'level': level, 'label': label, 'detail': detail},
        'holder': holder,
        'data_reached': min(folders) if folders else None,
        'processes': ingest_processes(),
        'stages': stage_last_runs(),
        'fetchers': fetchers,
        'zfs': zfs_usage(),
        'pg': pg,
        'tps': tps,
        'db_growth': db_growth,
        'series': remaining_series(now, remaining, proc_times, arrivals),
    }
    m['elapsed'] = time.time() - t_start
    write_atomic(f'{OUT_DIR}/index.html', render(m))
    write_atomic(f'{OUT_DIR}/status.json', json.dumps(m, indent=1, default=str))


if __name__ == '__main__':
    main()
