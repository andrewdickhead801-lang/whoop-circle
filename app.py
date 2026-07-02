"""
WHOOP Circle — multi-user WHOOP tracker with private, permissioned circles.

One file = the whole website: Sign in with WHOOP, per-user private data,
sleep/recovery/strain analytics, a decision-rating journal, a peptide tracker,
and invite-only "circles" for comparing stats with friends. Circles are fully
isolated — you only ever see people inside a circle you share, and the owner
approves every member.

Run locally:  uvicorn app:app --port 8000
On Render:    uvicorn app:app --host 0.0.0.0 --port $PORT   (set DATABASE_URL for Postgres)
"""
import json, math, os, secrets, sqlite3, statistics, threading, time, urllib.parse
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone, date

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from itsdangerous import URLSafeSerializer, BadSignature

# ============================================================ CONFIG
WHOOP_AUTH_URL = "https://api.prod.whoop.com/oauth/oauth2/auth"
WHOOP_TOKEN_URL = "https://api.prod.whoop.com/oauth/oauth2/token"
WHOOP_API_BASE = "https://api.prod.whoop.com/developer"
WHOOP_SCOPES = ["offline", "read:profile", "read:recovery", "read:cycles",
                "read:sleep", "read:workout", "read:body_measurement"]

CLIENT_ID = os.getenv("WHOOP_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("WHOOP_CLIENT_SECRET", "")
APP_BASE_URL = os.getenv("APP_BASE_URL", "").rstrip("/")
APP_SECRET = os.getenv("APP_SECRET", "dev-insecure-change-me")
DB_PATH = os.getenv("DB_PATH", "whoop_circle.db")
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = "postgresql://" + DATABASE_URL[len("postgres://"):]
IS_PG = DATABASE_URL.startswith("postgresql")
if IS_PG:
    import psycopg
    from psycopg.rows import dict_row

MS_PER_HOUR = 3_600_000
DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
GREEN = 67  # WHOOP "green" recovery threshold
signer = URLSafeSerializer(APP_SECRET, salt="session")


def missing_credentials():
    return not CLIENT_ID or not CLIENT_SECRET or CLIENT_ID == "your_client_id_here"


def redirect_uri(request):
    if APP_BASE_URL:
        return APP_BASE_URL + "/callback"
    base = str(request.base_url)
    if base.startswith("http://") and not any(h in base for h in ("localhost", "127.0.0.1")):
        base = "https://" + base[len("http://"):]
    return base.rstrip("/") + "/callback"


def secure_cookies():
    return APP_BASE_URL.startswith("https") if APP_BASE_URL else False

# ============================================================ DATABASE ADAPTER
def _P(sql):
    return sql.replace("?", "%s") if IS_PG else sql


@contextmanager
def connect():
    if IS_PG:
        conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    else:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def rows(sql, params=()):
    with connect() as c:
        cur = c.cursor(); cur.execute(_P(sql), params)
        return [dict(r) for r in cur.fetchall()]


def one(sql, params=()):
    r = rows(sql, params)
    return r[0] if r else None


def run(sql, params=()):
    with connect() as c:
        cur = c.cursor(); cur.execute(_P(sql), params)


def many(sql, seq):
    seq = list(seq)
    if not seq:
        return
    with connect() as c:
        cur = c.cursor(); cur.executemany(_P(sql), seq)


def upsert(table, row, conflict):
    cols = list(row.keys())
    q = ",".join(f'"{c}"' for c in cols); ph = ",".join("?" for _ in cols)
    upd = ",".join(f'"{c}"=excluded."{c}"' for c in cols if c not in conflict)
    sql = f'INSERT INTO {table}({q}) VALUES({ph}) ON CONFLICT({",".join(conflict)}) DO UPDATE SET {upd}'
    run(sql, [row[c] for c in cols])


def upsert_many(table, rowlist, conflict):
    rowlist = list(rowlist)
    if not rowlist:
        return
    cols = list(rowlist[0].keys())
    q = ",".join(f'"{c}"' for c in cols); ph = ",".join("?" for _ in cols)
    upd = ",".join(f'"{c}"=excluded."{c}"' for c in cols if c not in conflict)
    sql = f'INSERT INTO {table}({q}) VALUES({ph}) ON CONFLICT({",".join(conflict)}) DO UPDATE SET {upd}'
    many(sql, [[r[c] for c in cols] for r in rowlist])


SCHEMA = [
    """CREATE TABLE IF NOT EXISTS users(user_id BIGINT PRIMARY KEY, first_name TEXT,
        last_name TEXT, display_name TEXT, created_at REAL)""",
    """CREATE TABLE IF NOT EXISTS tokens(user_id BIGINT PRIMARY KEY, access_token TEXT,
        refresh_token TEXT, expires_at REAL, scope TEXT, updated_at REAL)""",
    """CREATE TABLE IF NOT EXISTS cycles(user_id BIGINT, id BIGINT, start TEXT, "end" TEXT,
        tz_offset TEXT, score_state TEXT, strain REAL, avg_hr INTEGER, max_hr INTEGER,
        kilojoules REAL, raw_json TEXT, PRIMARY KEY(user_id,id))""",
    """CREATE TABLE IF NOT EXISTS sleeps(user_id BIGINT, id TEXT, cycle_id BIGINT, start TEXT,
        "end" TEXT, tz_offset TEXT, nap INTEGER, score_state TEXT, performance_pct REAL,
        efficiency_pct REAL, consistency_pct REAL, respiratory_rate REAL, total_in_bed_ms BIGINT,
        total_awake_ms BIGINT, total_light_ms BIGINT, total_sws_ms BIGINT, total_rem_ms BIGINT,
        disturbance_count INTEGER, sleep_need_ms BIGINT, raw_json TEXT, PRIMARY KEY(user_id,id))""",
    """CREATE TABLE IF NOT EXISTS recoveries(user_id BIGINT, sleep_id TEXT, cycle_id BIGINT,
        created_at TEXT, score_state TEXT, recovery_pct REAL, resting_hr REAL, hrv_rmssd_ms REAL,
        spo2_pct REAL, skin_temp_c REAL, user_calibrating INTEGER, raw_json TEXT,
        PRIMARY KEY(user_id,sleep_id))""",
    """CREATE TABLE IF NOT EXISTS workouts(user_id BIGINT, id TEXT, start TEXT, "end" TEXT,
        tz_offset TEXT, sport_name TEXT, score_state TEXT, strain REAL, avg_hr INTEGER,
        max_hr INTEGER, kilojoules REAL, distance_m REAL, raw_json TEXT, PRIMARY KEY(user_id,id))""",
    """CREATE TABLE IF NOT EXISTS journal(user_id BIGINT, day TEXT, mood INTEGER, notes TEXT,
        tags_json TEXT, updated_at REAL, PRIMARY KEY(user_id,day))""",
    """CREATE TABLE IF NOT EXISTS peptides(peptide_id TEXT PRIMARY KEY, user_id BIGINT, name TEXT,
        dose TEXT, days_json TEXT, active INTEGER DEFAULT 1, created_at REAL)""",
    """CREATE TABLE IF NOT EXISTS peptide_log(user_id BIGINT, peptide_id TEXT, day TEXT,
        taken INTEGER, updated_at REAL, PRIMARY KEY(user_id,peptide_id,day))""",
    """CREATE TABLE IF NOT EXISTS peptide_notes(user_id BIGINT, peptide_id TEXT, week_start TEXT,
        note TEXT, updated_at REAL, PRIMARY KEY(user_id,peptide_id,week_start))""",
    """CREATE TABLE IF NOT EXISTS circles(circle_id TEXT PRIMARY KEY, name TEXT, owner_id BIGINT,
        invite_code TEXT, created_at REAL)""",
    """CREATE TABLE IF NOT EXISTS memberships(circle_id TEXT, user_id BIGINT, status TEXT, role TEXT,
        share_recovery INTEGER DEFAULT 1, share_sleep INTEGER DEFAULT 1, share_strain INTEGER DEFAULT 1,
        share_hrv INTEGER DEFAULT 1, share_peptides INTEGER DEFAULT 0, joined_at REAL,
        PRIMARY KEY(circle_id,user_id))""",
    """CREATE TABLE IF NOT EXISTS sync_log(user_id BIGINT, resource TEXT, finished_at REAL,
        records INTEGER, ok INTEGER, message TEXT)""",
    """CREATE TABLE IF NOT EXISTS goals(user_id BIGINT, metric TEXT, target REAL, direction TEXT,
        created_at REAL, PRIMARY KEY(user_id, metric))""",
    """CREATE TABLE IF NOT EXISTS assets(name TEXT PRIMARY KEY, data TEXT, ctype TEXT, created_at REAL)""",
]


def init_db():
    for stmt in SCHEMA:
        run(stmt)
    # migrations: add age/sex to existing users tables (ignore if already present)
    for col, typ in (("age", "INTEGER"), ("sex", "TEXT")):
        try:
            run(f"ALTER TABLE users ADD COLUMN {col} {typ}")
        except Exception:
            pass

# ============================================================ USERS / SESSION
def upsert_user(profile):
    uid = profile.get("user_id")
    fn = profile.get("first_name") or "WHOOP"
    ln = profile.get("last_name") or ""
    existing = one("SELECT display_name FROM users WHERE user_id=?", (uid,))
    disp = existing["display_name"] if existing else (fn + (" " + ln[0] + "." if ln else ""))
    upsert("users", {"user_id": uid, "first_name": fn, "last_name": ln,
                     "display_name": disp, "created_at": time.time()}, ["user_id"])
    return uid


def get_user(uid):
    return one("SELECT * FROM users WHERE user_id=?", (uid,))


def current_uid(request):
    c = request.cookies.get("sid")
    if not c:
        return None
    try:
        return signer.loads(c)
    except BadSignature:
        return None


def require(request):
    uid = current_uid(request)
    if uid is None:
        raise HTTPException(401, "Not signed in")
    return uid

# ============================================================ TOKENS / WHOOP CLIENT
class NotConnected(Exception):
    pass


def save_tokens(uid, access, refresh, expires_in, scope=""):
    if refresh is None:
        ex = one("SELECT refresh_token FROM tokens WHERE user_id=?", (uid,))
        refresh = ex["refresh_token"] if ex else None
    upsert("tokens", {"user_id": uid, "access_token": access, "refresh_token": refresh,
                      "expires_at": time.time() + float(expires_in) - 60, "scope": scope,
                      "updated_at": time.time()}, ["user_id"])


def token_row(uid):
    return one("SELECT * FROM tokens WHERE user_id=?", (uid,))


def build_authorize_url(state, redir):
    p = {"response_type": "code", "client_id": CLIENT_ID, "redirect_uri": redir,
         "scope": " ".join(WHOOP_SCOPES), "state": state}
    return f"{WHOOP_AUTH_URL}?{urllib.parse.urlencode(p)}"


def exchange_code(code, redir):
    r = httpx.post(WHOOP_TOKEN_URL, data={"grant_type": "authorization_code", "code": code,
                   "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "redirect_uri": redir}, timeout=30)
    r.raise_for_status()
    return r.json()


def refresh_token(uid):
    row = token_row(uid)
    if not row or not row["refresh_token"]:
        raise NotConnected("No refresh token — reconnect WHOOP.")
    r = httpx.post(WHOOP_TOKEN_URL, data={"grant_type": "refresh_token",
                   "refresh_token": row["refresh_token"], "client_id": CLIENT_ID,
                   "client_secret": CLIENT_SECRET, "scope": "offline"}, timeout=30)
    r.raise_for_status()
    t = r.json()
    save_tokens(uid, t["access_token"], t.get("refresh_token"), t.get("expires_in", 3600), t.get("scope", ""))
    return t["access_token"]


def valid_token(uid):
    row = token_row(uid)
    if not row:
        raise NotConnected("WHOOP not connected.")
    if time.time() >= row["expires_at"]:
        return refresh_token(uid)
    return row["access_token"]


def _api(access, method, path, params=None):
    return httpx.request(method, f"{WHOOP_API_BASE}{path}", params=params,
                         headers={"Authorization": f"Bearer {access}"}, timeout=30)


def _req(uid, method, path, params=None):
    tok = valid_token(uid); resp = None
    for attempt in range(4):
        resp = _api(tok, method, path, params)
        if resp.status_code == 401:
            tok = refresh_token(uid); continue
        if resp.status_code == 429:
            time.sleep(min(float(resp.headers.get("Retry-After", 2 ** attempt)), 30)); continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return resp.json()


def fetch_profile(access):
    r = _api(access, "GET", "/v2/user/profile/basic")
    r.raise_for_status()
    return r.json()


def _paginate(uid, path, start=None):
    nxt = None
    while True:
        params = {"limit": 25}
        if start: params["start"] = start
        if nxt: params["nextToken"] = nxt
        payload = _req(uid, "GET", path, params)
        for rec in payload.get("records", []):
            yield rec
        nxt = payload.get("next_token")
        if not nxt:
            break
        time.sleep(0.25)


def _g(d, *path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
        if cur is None:
            return default
    return cur


def parse_cycle(uid, r):
    return {"user_id": uid, "id": r.get("id"), "start": r.get("start"), "end": r.get("end"),
            "tz_offset": r.get("timezone_offset"), "score_state": r.get("score_state"),
            "strain": _g(r, "score", "strain"), "avg_hr": _g(r, "score", "average_heart_rate"),
            "max_hr": _g(r, "score", "max_heart_rate"), "kilojoules": _g(r, "score", "kilojoule"),
            "raw_json": json.dumps(r)}


def parse_sleep(uid, r):
    st = ("score", "stage_summary")
    return {"user_id": uid, "id": r.get("id"), "cycle_id": r.get("cycle_id"), "start": r.get("start"),
            "end": r.get("end"), "tz_offset": r.get("timezone_offset"), "nap": 1 if r.get("nap") else 0,
            "score_state": r.get("score_state"),
            "performance_pct": _g(r, "score", "sleep_performance_percentage"),
            "efficiency_pct": _g(r, "score", "sleep_efficiency_percentage"),
            "consistency_pct": _g(r, "score", "sleep_consistency_percentage"),
            "respiratory_rate": _g(r, "score", "respiratory_rate"),
            "total_in_bed_ms": _g(r, *st, "total_in_bed_time_milli"),
            "total_awake_ms": _g(r, *st, "total_awake_time_milli"),
            "total_light_ms": _g(r, *st, "total_light_sleep_time_milli"),
            "total_sws_ms": _g(r, *st, "total_slow_wave_sleep_time_milli"),
            "total_rem_ms": _g(r, *st, "total_rem_sleep_time_milli"),
            "disturbance_count": _g(r, *st, "disturbance_count"),
            "sleep_need_ms": _g(r, "score", "sleep_needed", "need_from_sleep_debt_milli"),
            "raw_json": json.dumps(r)}


def parse_recovery(uid, r):
    return {"user_id": uid, "sleep_id": r.get("sleep_id"), "cycle_id": r.get("cycle_id"),
            "created_at": r.get("created_at"), "score_state": r.get("score_state"),
            "recovery_pct": _g(r, "score", "recovery_score"),
            "resting_hr": _g(r, "score", "resting_heart_rate"),
            "hrv_rmssd_ms": _g(r, "score", "hrv_rmssd_milli"),
            "spo2_pct": _g(r, "score", "spo2_percentage"),
            "skin_temp_c": _g(r, "score", "skin_temp_celsius"),
            "user_calibrating": 1 if _g(r, "score", "user_calibrating") else 0, "raw_json": json.dumps(r)}


def parse_workout(uid, r):
    return {"user_id": uid, "id": r.get("id"), "start": r.get("start"), "end": r.get("end"),
            "tz_offset": r.get("timezone_offset"), "sport_name": r.get("sport_name") or r.get("sport_id"),
            "score_state": r.get("score_state"), "strain": _g(r, "score", "strain"),
            "avg_hr": _g(r, "score", "average_heart_rate"), "max_hr": _g(r, "score", "max_heart_rate"),
            "kilojoules": _g(r, "score", "kilojoule"), "distance_m": _g(r, "score", "distance_meter"),
            "raw_json": json.dumps(r)}

# ============================================================ SYNC (per user)
_sync = {}
_locks = defaultdict(threading.Lock)


def sync_status(uid):
    return _sync.get(uid, {"running": False, "phase": "", "counts": {}, "error": None})


def _iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _run_sync(uid, start_iso, label):
    with _locks[uid]:
        if _sync.get(uid, {}).get("running"):
            return
        _sync[uid] = {"running": True, "phase": "starting", "counts": {}, "error": None}
    try:
        jobs = [("cycles", "/v2/cycle", parse_cycle, ["user_id", "id"]),
                ("sleeps", "/v2/activity/sleep", parse_sleep, ["user_id", "id"]),
                ("recoveries", "/v2/recovery", parse_recovery, ["user_id", "sleep_id"]),
                ("workouts", "/v2/activity/workout", parse_workout, ["user_id", "id"])]
        for name, path, parse, conflict in jobs:
            _sync[uid]["phase"] = f"{label}: {name}"
            batch, total = [], 0
            try:
                for rec in _paginate(uid, path, start_iso):
                    batch.append(parse(uid, rec))
                    if len(batch) >= 200:
                        upsert_many(name, batch, conflict); total += len(batch); batch = []
                        _sync[uid]["counts"][name] = total
                if batch:
                    upsert_many(name, batch, conflict); total += len(batch)
                _sync[uid]["counts"][name] = total
                run("INSERT INTO sync_log(user_id,resource,finished_at,records,ok,message) VALUES(?,?,?,?,?,?)",
                    (uid, name, time.time(), total, 1, ""))
            except Exception as e:
                run("INSERT INTO sync_log(user_id,resource,finished_at,records,ok,message) VALUES(?,?,?,?,?,?)",
                    (uid, name, time.time(), total, 0, str(e)))
                raise
        _sync[uid]["phase"] = "done"
    except Exception as e:
        _sync[uid]["error"] = str(e)
    finally:
        _sync[uid]["running"] = False
        bust_cache(uid)


def full_sync_async(uid):
    threading.Thread(target=_run_sync, args=(uid, None, "lifetime"), daemon=True).start()


def recent_sync(uid, days=14):
    _run_sync(uid, _iso(datetime.now(timezone.utc) - timedelta(days=days)), f"last {days}d")
    return sync_status(uid)

# ============================================================ ANALYSIS (per user)
def _mean(xs):
    xs = [x for x in xs if x is not None]
    return statistics.fmean(xs) if xs else None


def _pearson(xs, ys):
    pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    if len(pairs) < 4:
        return None
    a, b = zip(*pairs)
    try:
        return statistics.correlation(a, b)
    except (statistics.StatisticsError, ValueError):
        return None


def _date(iso):
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _round(v, n=1):
    return round(v, n) if isinstance(v, (int, float)) else v


def sleep_quality_score(row):
    perf, eff, cons = row.get("performance_pct"), row.get("efficiency_pct"), row.get("consistency_pct")
    rem, sws = row.get("total_rem_ms") or 0, row.get("total_sws_ms") or 0
    in_bed, awake = row.get("total_in_bed_ms") or 0, row.get("total_awake_ms") or 0
    asleep = max(in_bed - awake, 1)
    rest = (rem + sws) / asleep * 100 if asleep else None
    rest_score = min(rest / 0.5, 100) if rest is not None else None
    parts, w = [], []
    for val, wt in ((perf, .40), (eff, .20), (cons, .15), (rest_score, .25)):
        if val is not None:
            parts.append(val * wt); w.append(wt)
    if not w:
        return None
    score = sum(parts) / sum(w)
    if row.get("disturbance_count"):
        score -= min(row["disturbance_count"] * 0.4, 8)
    return max(0, min(100, round(score, 1)))


def _verdict(s):
    if s is None: return "no data"
    if s >= 85: return "elite"
    if s >= 70: return "solid"
    if s >= 55: return "fair"
    return "poor"


# --- lightweight per-user cache so a tab's many calls don't re-read the DB each time ---
_DCACHE = {}
_DCACHE_TTL = 90


def _cached(uid, key, fn):
    now = time.time()
    e = _DCACHE.get((uid, key))
    if e and now - e[0] < _DCACHE_TTL:
        return e[1]
    v = fn()
    _DCACHE[(uid, key)] = (now, v)
    return v


def bust_cache(uid=None):
    if uid is None:
        _DCACHE.clear()
    else:
        for k in [k for k in _DCACHE if k[0] == uid]:
            _DCACHE.pop(k, None)


def _sleeps(uid):
    def build():
        r = rows('SELECT * FROM sleeps WHERE user_id=? AND nap=0 AND score_state=? ORDER BY start ASC', (uid, "SCORED"))
        for x in r:
            x["day"] = str(_date(x["start"])); x["quality"] = sleep_quality_score(x)
            x["hours"] = round((x.get("total_in_bed_ms") or 0) / MS_PER_HOUR, 2)
        return r
    return _cached(uid, "sleeps", build)


def _recs(uid):
    def build():
        r = rows('SELECT * FROM recoveries WHERE user_id=? AND score_state=? ORDER BY created_at ASC', (uid, "SCORED"))
        for x in r:
            x["day"] = str(_date(x["created_at"]))
        return r
    return _cached(uid, "recs", build)


def _cycles(uid):
    def build():
        r = rows('SELECT * FROM cycles WHERE user_id=? AND score_state=? ORDER BY start ASC', (uid, "SCORED"))
        for x in r:
            x["day"] = str(_date(x["start"]))
        return r
    return _cached(uid, "cycles", build)


def counts(uid):
    out = {}
    for t in ("cycles", "sleeps", "recoveries", "workouts", "journal"):
        out[t] = one(f"SELECT COUNT(*) AS n FROM {t} WHERE user_id=?", (uid,))["n"]
    return out


def overview(uid):
    sleeps, recs, cycles = _sleeps(uid), _recs(uid), _cycles(uid)

    def latest(rws, k):
        for r in reversed(rws):
            if r.get(k) is not None:
                return r
        return None
    lr, ls, lc = latest(recs, "recovery_pct"), latest(sleeps, "quality"), latest(cycles, "strain")
    span = None
    if cycles:
        d0, d1 = _date(cycles[0]["start"]), _date(cycles[-1]["start"])
        if d0 and d1: span = (d1 - d0).days + 1
    c = counts(uid)
    return {"counts": c, "span_days": span,
            "records_total": c["cycles"] + c["sleeps"] + c["recoveries"] + c["workouts"],
            "latest": {"recovery_pct": _round(lr["recovery_pct"]) if lr else None,
                       "recovery_day": lr["day"] if lr else None,
                       "sleep_quality": ls["quality"] if ls else None,
                       "sleep_verdict": _verdict(ls["quality"]) if ls else None,
                       "strain": _round(lc["strain"]) if lc else None},
            "lifetime": {"avg_recovery": _round(_mean([r["recovery_pct"] for r in recs])),
                         "avg_sleep_quality": _round(_mean([s["quality"] for s in sleeps if s["quality"] is not None])),
                         "avg_sleep_hours": _round(_mean([s["hours"] for s in sleeps])),
                         "avg_strain": _round(_mean([cc["strain"] for cc in cycles])),
                         "avg_resting_hr": _round(_mean([r["resting_hr"] for r in recs])),
                         "avg_hrv": _round(_mean([r["hrv_rmssd_ms"] for r in recs]))}}


def sleep_report(uid, window=120):
    sleeps = _sleeps(uid)
    series = [{"day": s["day"], "quality": s["quality"], "hours": s["hours"]}
              for s in sleeps if s["quality"] is not None][-window:]
    qs = [p["quality"] for p in series]
    for i, p in enumerate(series):
        w = qs[max(0, i - 6):i + 1]; p["rolling7"] = round(sum(w) / len(w), 1)

    def am(k): return _mean([s.get(k) for s in sleeps])
    light, rem, sws = am("total_light_ms"), am("total_rem_ms"), am("total_sws_ms")
    tot = (light or 0) + (rem or 0) + (sws or 0)
    stages = {}
    if tot:
        stages = {"light_pct": round((light or 0) / tot * 100, 1), "rem_pct": round((rem or 0) / tot * 100, 1),
                  "sws_pct": round((sws or 0) / tot * 100, 1)}
    ranked = sorted([s for s in sleeps if s["quality"] is not None], key=lambda s: s["quality"])
    return {"series": series, "stages": stages,
            "best": [{"day": s["day"], "quality": s["quality"], "hours": s["hours"]} for s in ranked[-5:][::-1]],
            "worst": [{"day": s["day"], "quality": s["quality"], "hours": s["hours"]} for s in ranked[:5]],
            "verdict": _verdict(_mean([s["quality"] for s in sleeps]))}


def recovery_report(uid, window=120):
    recs, cycles = _recs(uid), _cycles(uid)
    rb, cb = {r["day"]: r for r in recs}, {c["day"]: c for c in cycles}
    days = sorted(set(rb) | set(cb))[-window:]
    series = [{"day": d, "recovery": _round(rb[d]["recovery_pct"]) if d in rb else None,
               "strain": _round(cb[d]["strain"]) if d in cb else None,
               "hrv": _round(rb[d]["hrv_rmssd_ms"]) if d in rb else None} for d in days]
    st, rt = [], []
    for d in sorted(set(cb) | set(rb)):
        nx = str((datetime.fromisoformat(d) + timedelta(days=1)).date())
        if d in cb and nx in rb:
            st.append(cb[d]["strain"]); rt.append(rb[nx]["recovery_pct"])
    strain_next = _pearson(st, rt)
    dr, ds = defaultdict(list), defaultdict(list)
    for d, r in rb.items(): dr[datetime.fromisoformat(d).weekday()].append(r["recovery_pct"])
    for d, cc in cb.items(): ds[datetime.fromisoformat(d).weekday()].append(cc["strain"])
    by_dow = [{"day": DOW[i], "avg_recovery": _round(_mean(dr.get(i, []))),
               "avg_strain": _round(_mean(ds.get(i, [])))} for i in range(7)]
    ins = []
    if strain_next is not None:
        verb = "lowers" if strain_next < 0 else "doesn't clearly lower"
        ins.append(f"Higher strain {verb} your next-day recovery (r = {strain_next:+.2f}).")
    valid = [x for x in by_dow if x["avg_recovery"] is not None]
    if valid:
        b = max(valid, key=lambda x: x["avg_recovery"]); w = min(valid, key=lambda x: x["avg_recovery"])
        ins.append(f"You recover best on {b['day']} ({b['avg_recovery']}%) and worst on {w['day']} ({w['avg_recovery']}%).")
    return {"series": series, "by_dow": by_dow,
            "correlations": {"strain_to_next_recovery": _round(strain_next, 2)}, "insights": ins}


def decision_report(uid):
    journal = get_journal(uid)
    recs = {r["day"]: r for r in _recs(uid)}
    sleeps_by = {s["day"]: s for s in _sleeps(uid)}
    b_rec = _mean([r["recovery_pct"] for r in recs.values()])
    b_hrv = _mean([r["hrv_rmssd_ms"] for r in recs.values()])
    b_sleep = _mean([s["quality"] for s in sleeps_by.values() if s["quality"] is not None])
    t_rec, t_hrv, t_sleep = defaultdict(list), defaultdict(list), defaultdict(list)
    for e in journal:
        d = e["day"]
        try:
            nx = str((datetime.fromisoformat(d) + timedelta(days=1)).date())
        except ValueError:
            continue
        night, morning = sleeps_by.get(d), recs.get(nx) or recs.get(d)
        for tag in e.get("tags", []):
            if morning:
                if morning.get("recovery_pct") is not None: t_rec[tag].append(morning["recovery_pct"])
                if morning.get("hrv_rmssd_ms") is not None: t_hrv[tag].append(morning["hrv_rmssd_ms"])
            if night and night.get("quality") is not None: t_sleep[tag].append(night["quality"])
    ratings = []
    for tag in sorted(set(t_rec) | set(t_sleep)):
        rv = t_rec.get(tag, []); n = len(rv)
        d_rec = (_mean(rv) - b_rec) if (rv and b_rec is not None) else None
        d_hrv = (_mean(t_hrv.get(tag, [])) - b_hrv) if (t_hrv.get(tag) and b_hrv is not None) else None
        d_sleep = (_mean(t_sleep.get(tag, [])) - b_sleep) if (t_sleep.get(tag) and b_sleep is not None) else None
        sig = d_rec if d_rec is not None else d_sleep
        if sig is None or n < 2:
            rating, emoji = "need more data", "·"
        elif sig >= 3:
            rating, emoji = "good call", "✅"
        elif sig <= -3:
            rating, emoji = "costly", "⚠️"
        else:
            rating, emoji = "neutral", "➖"
        ratings.append({"tag": tag, "n": n, "rating": rating, "emoji": emoji,
                        "recovery_delta": _round(d_rec), "hrv_delta": _round(d_hrv), "sleep_delta": _round(d_sleep)})
    ratings.sort(key=lambda x: (x["recovery_delta"] if x["recovery_delta"] is not None else -999), reverse=True)
    return {"baseline": {"recovery": _round(b_rec), "hrv": _round(b_hrv), "sleep_quality": _round(b_sleep)},
            "ratings": ratings, "journal_days": len(journal), "has_data": bool(ratings)}


def get_journal(uid):
    r = rows("SELECT * FROM journal WHERE user_id=? ORDER BY day DESC", (uid,))
    for x in r:
        x["tags"] = json.loads(x.get("tags_json") or "[]")
    return r

# ============================================================ CIRCLE METRICS
def user_metrics(uid):
    recs, sleeps, cycles = _recs(uid), _sleeps(uid), _cycles(uid)
    last_recovery = recs[-1]["recovery_pct"] if recs else None
    r30 = [r["recovery_pct"] for r in recs[-30:]]
    s30 = [s["quality"] for s in sleeps[-30:] if s["quality"] is not None]
    hrv30 = [r["hrv_rmssd_ms"] for r in recs[-30:]]
    today = date.today()
    wk = [c["strain"] for c in cycles if _date(c["start"]) and (today - _date(c["start"])).days < 7]
    return {"recovery_latest": _round(last_recovery), "recovery_avg30": _round(_mean(r30)),
            "sleep_avg30": _round(_mean(s30)), "hrv_avg30": _round(_mean(hrv30)),
            "strain_week": _round(sum(wk), 1) if wk else None}


def green_streak(uid):
    recs = {r["day"]: r["recovery_pct"] for r in _recs(uid) if r.get("recovery_pct") is not None}
    if not recs:
        return 0
    d = max(datetime.fromisoformat(x).date() for x in recs)
    streak = 0
    while True:
        key = str(d)
        if key in recs and recs[key] >= GREEN:
            streak += 1; d = d - timedelta(days=1)
        else:
            break
    return streak


def sleep_streak(uid):
    days = {s["day"] for s in _sleeps(uid)}
    if not days:
        return 0
    d = max(datetime.fromisoformat(x).date() for x in days)
    streak = 0
    while str(d) in days:
        streak += 1; d = d - timedelta(days=1)
    return streak


def user_badges(uid):
    recs, sleeps, cycles = _recs(uid), _sleeps(uid), _cycles(uid)
    workouts = one("SELECT COUNT(*) AS n FROM workouts WHERE user_id=?", (uid,))["n"]
    badges = []

    def add(cond, label):
        if cond: badges.append(label)
    add(any((r.get("recovery_pct") or 0) >= 90 for r in recs), "🟢 90+ Recovery")
    add(green_streak(uid) >= 7, "🔥 Green Week")
    add(sleep_streak(uid) >= 30, "🛏️ 30-Night Streak")
    add(any((c.get("strain") or 0) >= 14 for c in cycles), "💪 14 Strain Club")
    rr = _mean([s.get("respiratory_rate") for s in sleeps])
    add(rr is not None and rr < 14 and len(sleeps) >= 10, "🫁 Iron Lungs")
    add(workouts >= 20, "🏋️ Habit Builder")
    cons = _mean([s.get("consistency_pct") for s in sleeps])
    add(cons is not None and cons >= 70, "⏰ Consistent Sleeper")
    return badges


def peptide_week_summary(uid):
    """Active peptides + this-week adherence for sharing to a circle."""
    ws = week_start(date.today())
    peps = rows("SELECT * FROM peptides WHERE user_id=? AND active=1", (uid,))
    out = []
    for p in peps:
        days = json.loads(p.get("days_json") or "[]")
        scheduled = len(days)
        taken = 0
        for i in days:
            d = str(datetime.fromisoformat(ws).date() + timedelta(days=i))
            row = one("SELECT taken FROM peptide_log WHERE user_id=? AND peptide_id=? AND day=?",
                      (uid, p["peptide_id"], d))
            if row and row["taken"]:
                taken += 1
        out.append({"name": p["name"], "dose": p.get("dose") or "",
                    "per_week": scheduled, "taken": taken})
    return out

# ============================================================ PEPTIDES
def week_start(d):
    monday = d - timedelta(days=d.weekday())
    return str(monday)


def peptides_view(uid, ws):
    peps = rows("SELECT * FROM peptides WHERE user_id=? ORDER BY active DESC, name ASC", (uid,))
    logs = {}
    notes = {}
    for p in peps:
        p["days"] = json.loads(p.get("days_json") or "[]")
        lr = rows("SELECT day,taken FROM peptide_log WHERE user_id=? AND peptide_id=? AND day>=? AND day<=?",
                  (uid, p["peptide_id"], ws, str(datetime.fromisoformat(ws).date() + timedelta(days=6))))
        logs[p["peptide_id"]] = {x["day"]: x["taken"] for x in lr}
        nr = one("SELECT note FROM peptide_notes WHERE user_id=? AND peptide_id=? AND week_start=?",
                 (uid, p["peptide_id"], ws))
        notes[p["peptide_id"]] = nr["note"] if nr else ""
    return {"week_start": ws, "peptides": peps, "logs": logs, "notes": notes}

# ============================================================ CIRCLES
def my_circles(uid):
    r = rows("""SELECT c.circle_id, c.name, c.owner_id, c.invite_code, m.status, m.role
                FROM memberships m JOIN circles c ON c.circle_id=m.circle_id
                WHERE m.user_id=? ORDER BY c.created_at ASC""", (uid,))
    for x in r:
        x["is_owner"] = (x["owner_id"] == uid)
        x["members"] = one("SELECT COUNT(*) AS n FROM memberships WHERE circle_id=? AND status=?",
                           (x["circle_id"], "active"))["n"]
        x["pending"] = one("SELECT COUNT(*) AS n FROM memberships WHERE circle_id=? AND status=?",
                          (x["circle_id"], "pending"))["n"] if x["is_owner"] else 0
        if not x["is_owner"]:
            x["invite_code"] = None  # only owner sees the code
    return r


def circle_detail(uid, cid):
    mem = one("SELECT * FROM memberships WHERE circle_id=? AND user_id=?", (cid, uid))
    if not mem or mem["status"] != "active":
        raise HTTPException(403, "You are not a member of this circle.")
    circle = one("SELECT * FROM circles WHERE circle_id=?", (cid,))
    is_owner = circle["owner_id"] == uid
    active = rows("""SELECT m.*, u.display_name FROM memberships m JOIN users u ON u.user_id=m.user_id
                     WHERE m.circle_id=? AND m.status=?""", (cid, "active"))
    pending = []
    if is_owner:
        pending = rows("""SELECT m.user_id, u.display_name FROM memberships m JOIN users u ON u.user_id=m.user_id
                          WHERE m.circle_id=? AND m.status=?""", (cid, "pending"))

    # --- leaderboards (only members who share that metric) ---
    def board(flag, key, label, reverse=True):
        entries = []
        for m in active:
            if not m.get(flag):
                continue
            val = user_metrics(m["user_id"]).get(key)
            if val is not None:
                entries.append({"name": m["display_name"], "value": val,
                                "me": m["user_id"] == uid})
        entries.sort(key=lambda e: e["value"], reverse=reverse)
        return {"label": label, "entries": entries}

    leaderboards = [
        board("share_recovery", "recovery_avg30", "Recovery (30-day avg %)"),
        board("share_sleep", "sleep_avg30", "Sleep quality (30-day avg)"),
        board("share_strain", "strain_week", "Strain (7-day total)"),
        board("share_hrv", "hrv_avg30", "HRV (30-day avg ms)"),
    ]
    # --- challenges / streaks ---
    streaks = []
    for m in active:
        if m.get("share_recovery"):
            streaks.append({"name": m["display_name"], "green_streak": green_streak(m["user_id"]),
                            "sleep_streak": sleep_streak(m["user_id"]), "me": m["user_id"] == uid})
    streaks.sort(key=lambda e: e["green_streak"], reverse=True)
    # --- achievements wall ---
    achievements = [{"name": m["display_name"], "badges": user_badges(m["user_id"]),
                     "me": m["user_id"] == uid} for m in active]
    # --- shared peptides ---
    peptides = []
    for m in active:
        if m.get("share_peptides"):
            peptides.append({"name": m["display_name"], "items": peptide_week_summary(m["user_id"]),
                             "me": m["user_id"] == uid})
    return {"circle": {"circle_id": cid, "name": circle["name"], "is_owner": is_owner,
                       "invite_code": circle["invite_code"] if is_owner else None},
            "my_sharing": {k: mem[k] for k in ("share_recovery", "share_sleep", "share_strain",
                                               "share_hrv", "share_peptides")},
            "members": [{"name": m["display_name"], "me": m["user_id"] == uid} for m in active],
            "pending": pending, "leaderboards": leaderboards, "streaks": streaks,
            "achievements": achievements, "peptides": peptides}

# ============================================================ HEALTH ANALYTICS
# All modules compare you to YOUR OWN rolling baseline (not population averages),
# using personal-baseline z-scores, coefficient of variation, EWMA load, and
# published clinical/sports-science thresholds. Pure statistics — deterministic.

def _pstats(vals):
    v = [x for x in vals if x is not None]
    if len(v) < 3:
        return None
    m = statistics.fmean(v)
    sd = statistics.pstdev(v) if len(v) > 1 else 0.0
    return {"mean": m, "sd": sd, "cv": (sd / m * 100 if m else None), "n": len(v)}


def vitals_panel(uid):
    recs, sleeps = _recs(uid), _sleeps(uid)

    def metric(name, pairs, unit, higher_better, popref=None, use_log=False):
        raw = [v for _, v in pairs if v is not None and (v > 0 or not use_log)]
        if len(raw) < 3:
            return None
        latest = raw[-1]
        tv = [math.log(v) for v in raw if v > 0] if use_log else raw
        base = tv[-60:]
        st = _pstats(base)
        if not st:
            return None
        tlatest = tv[-1]
        z = (tlatest - st["mean"]) / st["sd"] if st["sd"] else 0.0
        adverse = (-z if higher_better else z)
        status = "flag" if adverse >= 2 else "watch" if adverse >= 1 else "optimal"
        if use_log:
            low, high = math.exp(st["mean"] - st["sd"]), math.exp(st["mean"] + st["sd"])
            mean_disp = math.exp(st["mean"])
        else:
            low, high, mean_disp = st["mean"] - st["sd"], st["mean"] + st["sd"], st["mean"]
        return {"name": name, "unit": unit, "latest": round(latest, 1), "mean": round(mean_disp, 1),
                "cv": round(st["cv"], 1) if st["cv"] else None, "low": round(low, 1), "high": round(high, 1),
                "z": round(z, 2), "status": status, "higher_better": higher_better, "popref": popref,
                "trend": [round(v, 1) for v in raw[-30:]]}
    out = [
        metric("HRV (rMSSD)", [(r["day"], r["hrv_rmssd_ms"]) for r in recs], "ms", True, use_log=True),
        metric("Resting HR", [(r["day"], r["resting_hr"]) for r in recs], "bpm", False, "40–100"),
        metric("Respiratory rate", [(s["day"], s["respiratory_rate"]) for s in sleeps], "br/min", False, "12–20"),
        metric("SpO₂", [(r["day"], r["spo2_pct"]) for r in recs], "%", True, "95–100"),
        metric("Skin temp", [(r["day"], r["skin_temp_c"]) for r in recs], "°C", False),
    ]
    out = [v for v in out if v]
    # add plain-language verdict + age/sex population average
    u = get_user(uid) or {}
    nv = norm_values(u.get("age"), u.get("sex")) if (u.get("age") and u.get("sex")) else None
    POPKEY = {"HRV (rMSSD)": "hrv", "Resting HR": "rhr", "Respiratory rate": "resp", "SpO₂": "spo2"}
    for v in out:
        v["rating"] = {"optimal": "good", "watch": "watch", "flag": "off"}[v["status"]]
        plain = {"optimal": "In your healthy range.", "watch": "Drifting from your usual — keep an eye on it.",
                 "flag": "Well outside your usual — worth attention."}[v["status"]]
        avg = nv.get(POPKEY[v["name"]]) if (nv and v["name"] in POPKEY) else None
        if avg is not None:
            better = (v["latest"] > avg) if v["higher_better"] else (v["latest"] < avg)
            pct = abs(v["latest"] - avg) / avg * 100 if avg else 0
            cmp = "about average" if pct < 6 else ("better than average 👍" if better else "below average")
            plain += f" You're {cmp} for your age & sex (~{avg} {v['unit']})."
            v["avg"] = avg
        v["plain"] = plain
    return {"vitals": out}


def early_warning(uid):
    vp = {v["name"]: v for v in vitals_panel(uid)["vitals"]}
    score, drivers = 0, []
    for nm in ("Resting HR", "HRV (rMSSD)", "Respiratory rate", "Skin temp"):
        v = vp.get(nm)
        if not v:
            continue
        adverse = (-v["z"]) if v["higher_better"] else v["z"]
        if adverse >= 2:
            score += 2; drivers.append(f"{nm} strongly off baseline (z={v['z']:+.1f})")
        elif adverse >= 1:
            score += 1; drivers.append(f"{nm} drifting (z={v['z']:+.1f})")
    # classic pre-illness combo: elevated RHR + suppressed HRV
    rhr, hrv = vp.get("Resting HR"), vp.get("HRV (rMSSD)")
    if rhr and hrv and rhr["z"] >= 1 and hrv["z"] <= -1:
        score += 1; drivers.append("elevated resting HR + suppressed HRV (classic strain/illness pattern)")
    level = "alert" if score >= 4 else "watch" if score >= 2 else "ok"
    conf = min(95, 40 + score * 12)
    msg = {"ok": "Your vitals look steady — no early-warning signals right now.",
           "watch": "Some vitals are drifting from your baseline. Ease off and prioritise recovery; this can precede illness or overreaching by 1–3 days.",
           "alert": "Multiple vitals are off your baseline together — a pattern that often precedes illness or heavy strain. Consider rest, hydration, and monitoring; see a clinician if you feel unwell."}[level]
    return {"level": level, "score": score, "confidence": conf, "drivers": drivers, "message": msg}


def sleep_debt(uid, days=7, need_h=8.0):
    sl = _sleeps(uid)[-days:]
    if not sl:
        return None
    return round(sum(max(0, need_h - s["hours"]) for s in sl), 1)


def readiness(uid):
    recs, sleeps = _recs(uid), _sleeps(uid)
    if not recs:
        return {"score": None, "band": None, "recommendation": "Not enough data yet — keep syncing."}

    def comp(vals, higher_better, use_log=False):
        v = [x for x in vals if x is not None and (x > 0 or not use_log)]
        if len(v) < 8:
            return None
        tv = [math.log(x) for x in v] if use_log else v
        b = tv[-14:]; m = statistics.fmean(b); sd = statistics.pstdev(b) or 1e-9
        z = (tv[-1] - m) / sd
        adj = z if higher_better else -z
        return max(0, min(100, 50 + adj * 18))
    hrv_s = comp([r["hrv_rmssd_ms"] for r in recs], True, use_log=True)
    rhr_s = comp([r["resting_hr"] for r in recs], False)
    sleep_s = sleeps[-1]["quality"] if sleeps and sleeps[-1]["quality"] is not None else None
    recovery = recs[-1]["recovery_pct"]
    parts, wsum = [], 0
    for val, w in ((hrv_s, .40), (rhr_s, .20), (sleep_s, .25), (recovery, .15)):
        if val is not None:
            parts.append(val * w); wsum += w
    if not wsum:
        return {"score": None, "band": None, "recommendation": "Not enough data yet."}
    score = round(sum(parts) / wsum)
    debt = sleep_debt(uid)
    if debt and debt >= 6:
        score = max(0, score - 6)
    band = "prime" if score >= 67 else "moderate" if score >= 34 else "recover"
    rec = {"prime": "Green light — your body can handle high strain today.",
           "moderate": "Amber — train, but keep it controlled and fuel/hydrate well.",
           "recover": "Red — prioritise rest, mobility and sleep today."}[band]
    return {"score": score, "band": band, "recommendation": rec,
            "factors": {"hrv": _round(hrv_s), "rhr": _round(rhr_s), "sleep": _round(sleep_s),
                        "whoop_recovery": _round(recovery), "sleep_debt_h": debt}}


def load_acwr(uid):
    cycles = _cycles(uid)
    by_day = {c["day"]: c["strain"] for c in cycles if c.get("strain") is not None}
    if len(by_day) < 14:
        return {"enough": False}
    days_sorted = sorted(by_day)
    d0, d1 = datetime.fromisoformat(days_sorted[0]).date(), datetime.fromisoformat(days_sorted[-1]).date()
    series, day = [], d0
    while day <= d1:
        series.append(by_day.get(str(day), 0.0)); day += timedelta(days=1)
    la, lc = 2 / (7 + 1), 2 / (28 + 1)
    ewa = ewc = series[0]; acwr_series = []
    for i, x in enumerate(series):
        if i:
            ewa = x * la + (1 - la) * ewa
            ewc = x * lc + (1 - lc) * ewc
        acwr_series.append(round(ewa / ewc, 2) if ewc else None)
    acwr = round(ewa / ewc, 2) if ewc else None
    zone = ("detraining" if acwr < 0.8 else "optimal" if acwr <= 1.3 else "caution"
            if acwr <= 1.5 else "high risk" if acwr <= 2.0 else "danger")
    msg = {"detraining": "Load is low relative to your norm — room to build, but ramp gradually.",
           "optimal": "You're in the sweet spot (0.8–1.3) — well-balanced load.",
           "caution": "Load is climbing faster than your body has adapted to — watch it.",
           "high risk": "Sharp spike vs your chronic load — elevated injury/illness risk. Back off.",
           "danger": "Very large spike — high injury risk. Deload now."}[zone]
    return {"enough": True, "acute": round(ewa, 1), "chronic": round(ewc, 1), "acwr": acwr, "zone": zone,
            "message": msg, "series": [{"day": str(d0 + timedelta(days=i)), "strain": round(series[i], 1),
                                        "acwr": acwr_series[i]} for i in range(len(series))][-60:]}


def sleep_regularity(uid, nights=14):
    sl = [s for s in _sleeps(uid) if s.get("start") and s.get("end")][-nights:]
    if len(sl) < 5:
        return None

    def mod(iso):
        t = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        # shift so 6pm=0, avoiding midnight wraparound for typical bedtimes
        return ((t.hour * 60 + t.minute) - 1080 + 1440) % 1440
    onsets, wakes = [mod(s["start"]) for s in sl], [mod(s["end"]) for s in sl]
    sd = (statistics.pstdev(onsets) + statistics.pstdev(wakes)) / 2
    return max(0, min(100, round(100 - sd / 3.2)))


def sleep_medicine(uid):
    sleeps = _sleeps(uid)
    if len(sleeps) < 5:
        return {"enough": False}

    def am(k):
        return _mean([s.get(k) for s in sleeps[-30:]])
    light, rem, sws = am("total_light_ms"), am("total_rem_ms"), am("total_sws_ms")
    tot = (light or 0) + (rem or 0) + (sws or 0)
    arch = []
    if tot:
        def band(pct, lo, hi, label):
            status = "optimal" if lo <= pct <= hi else ("watch" if (lo - 8) <= pct <= (hi + 8) else "flag")
            return {"stage": label, "pct": round(pct, 1), "norm": f"{lo}–{hi}%", "status": status}
        arch = [band(light / tot * 100, 45, 55, "Light"), band(sws / tot * 100, 10, 20, "Deep"),
                band(rem / tot * 100, 20, 25, "REM")]
    recs = _recs(uid)[-30:]
    spo2 = _mean([r["spo2_pct"] for r in recs])
    dist = _mean([s.get("disturbance_count") for s in sleeps[-30:]])
    rr = _mean([s.get("respiratory_rate") for s in sleeps[-30:]])
    flags = []
    if spo2 is not None and spo2 < 95:
        flags.append("Average SpO₂ below 95% — worth noting; can relate to disrupted breathing.")
    if dist is not None and dist >= 5:
        flags.append("Frequent nightly disturbances — fragmented sleep.")
    reg = sleep_regularity(uid)
    avg_h = _round(_mean([s["hours"] for s in sleeps[-30:]]))
    debt = sleep_debt(uid)

    # today / 7-day / 30-day windows for each sleep metric
    def _w3(vals):
        v = [x for x in vals if x is not None]
        if not v:
            return {"d1": None, "d7": None, "d30": None}
        return {"d1": _round(v[-1]), "d7": _round(_mean(v[-7:])), "d30": _round(_mean(v[-30:]))}
    dur_trio = _w3([s["hours"] for s in sleeps]); dur_trio["unit"] = "h"
    spo2_trio = _w3([r["spo2_pct"] for r in _recs(uid)]); spo2_trio["unit"] = "%"
    reg_trio = {"d1": None, "d7": sleep_regularity(uid, 7), "d30": sleep_regularity(uid, 30), "unit": ""}
    debt_trio = {"d1": sleep_debt(uid, 1), "d7": sleep_debt(uid, 7), "d30": sleep_debt(uid, 30), "unit": "h"}
    for a in arch:
        a["plain"] = {"optimal": "healthy amount", "watch": "a little off the ideal range",
                      "flag": "outside the healthy range"}[a["status"]]

    def _rate(v, good, ok, higher=True):
        if v is None:
            return ("unknown", "neutral", "not enough data yet")
        ok_hit = (v >= ok) if higher else (v <= ok)
        good_hit = (v >= good) if higher else (v <= good)
        return ("great", "good", "") if good_hit else (("okay", "watch", "") if ok_hit else ("poor", "bad", ""))
    reg_r = _rate(reg, 85, 70, True)
    dur_r = _rate(avg_h, 7.5, 6.5, True)
    debt_r = _rate(debt, 3, 6, False)
    dur_risk = metric_risk("sleep_hours", avg_h) or {}
    reg_risk = "concern" if (reg is not None and reg < 50) else ("watch" if (reg is not None and reg < 70) else "ok")
    debt_risk = "concern" if (debt is not None and debt >= 10) else ("watch" if (debt is not None and debt >= 6) else "ok")
    plains = {
        "regularity": {"label": reg_r[0], "color": reg_r[1], "risk": reg_risk, "trio": reg_trio,
                       "text": "how consistent your sleep/wake times are. 85+ is excellent; under 50 means very irregular timing.",
                       "why": "An irregular body clock is linked to worse recovery, mood and even long-term metabolic health.",
                       "causes": "changing bedtimes, late weekends, shift work, travel, or screens late at night",
                       "danger": "Not dangerous short-term, but a chronically irregular schedule is worth fixing." if reg_risk != "ok" else "Healthy, consistent timing."},
        "duration": {"label": dur_r[0], "color": dur_r[1], "risk": dur_risk.get("level", "ok"), "trio": dur_trio,
                     "text": f"you average {avg_h}h/night. 7–9h is the healthy target for adults.",
                     "why": dur_risk.get("why", ""), "causes": dur_risk.get("causes", ""), "danger": dur_risk.get("note", "")},
        "debt": {"label": debt_r[0], "color": debt_r[1], "risk": debt_risk, "trio": debt_trio,
                 "text": "sleep you owe from short nights this week. Under 3h is great; over 6h means you're running low.",
                 "why": "Big sleep debt lowers recovery, focus and immunity until you pay it back.",
                 "causes": "a run of short nights — late bedtimes, early alarms, or disrupted sleep",
                 "danger": "High debt this week — prioritise a few longer nights." if debt_risk != "ok" else "Well managed."},
    }
    breathe_risk = metric_risk("spo2", spo2) or {}
    return {"enough": True, "architecture": arch, "debt_h": debt,
            "regularity": reg, "avg_hours": avg_h,
            "spo2": _round(spo2), "disturbances": _round(dist), "respiratory_rate": _round(rr),
            "breathing_flags": flags, "plains": plains,
            "breathing": {"risk": breathe_risk.get("level", "ok"), "note": breathe_risk.get("note", ""),
                          "causes": breathe_risk.get("causes", ""), "why": breathe_risk.get("why", ""), "trio": spo2_trio}}


def peptide_outcomes(uid, before_days=21):
    recs = {r["day"]: r for r in _recs(uid)}
    sleeps = {s["day"]: s for s in _sleeps(uid)}
    peps = rows("SELECT * FROM peptides WHERE user_id=?", (uid,))
    out = []
    for p in peps:
        taken = [x["day"] for x in rows(
            "SELECT day FROM peptide_log WHERE user_id=? AND peptide_id=? AND taken=1 ORDER BY day ASC",
            (uid, p["peptide_id"]))]
        note = one("SELECT note FROM peptide_notes WHERE user_id=? AND peptide_id=? ORDER BY week_start DESC LIMIT 1",
                   (uid, p["peptide_id"]))
        note_txt = note["note"] if note else ""
        if not taken:
            out.append({"name": p["name"], "dose": p.get("dose"), "status": "no doses logged yet", "note": note_txt})
            continue
        start = datetime.fromisoformat(taken[0]).date()

        def window(dfrom, dto, src, key):
            vals, d = [], dfrom
            while d < dto:
                r = src.get(str(d))
                if r and r.get(key) is not None:
                    vals.append(r[key])
                d += timedelta(days=1)
            return _mean(vals), len(vals)
        metrics = []
        for label, src, key in (("Recovery %", recs, "recovery_pct"), ("HRV ms", recs, "hrv_rmssd_ms"),
                                ("Sleep quality", sleeps, "quality"), ("Resting HR", recs, "resting_hr")):
            b, bn = window(start - timedelta(days=before_days), start, src, key)
            a, an = window(start, date.today() + timedelta(days=1), src, key)
            metrics.append({"label": label, "before": _round(b), "during": _round(a),
                            "delta": _round(a - b) if (a is not None and b is not None) else None,
                            "n_before": bn, "n_during": an})
        conf = "low" if min(metrics[0]["n_before"], metrics[0]["n_during"]) < 7 else "moderate"
        out.append({"name": p["name"], "dose": p.get("dose"), "start": str(start),
                    "doses_logged": len(taken), "metrics": metrics, "note": note_txt, "confidence": conf})
    return {"peptides": out}


def weekly_narrative(uid):
    rd, vp, ew = readiness(uid), vitals_panel(uid), early_warning(uid)
    ld, sm, dec = load_acwr(uid), sleep_medicine(uid), decision_report(uid)
    L = []
    if rd.get("score") is not None:
        L.append(f"Your readiness is {rd['score']}/100 ({rd['band']}). {rd['recommendation']}")
    flags = [v for v in vp["vitals"] if v["status"] != "optimal"]
    L.append("Core vitals are all within your normal ranges." if not flags
             else "Vitals to watch: " + ", ".join(f"{v['name']} ({v['status']})" for v in flags) + ".")
    if ew["level"] != "ok":
        L.append(ew["message"])
    if ld.get("enough"):
        L.append(f"Training load (ACWR {ld['acwr']}) is {ld['zone']} — {ld['message']}")
    if sm.get("enough") and sm.get("debt_h") is not None:
        L.append(f"You're carrying ~{sm['debt_h']}h of sleep debt this week"
                 + (f", and your sleep regularity is {sm['regularity']}/100." if sm.get("regularity") is not None else "."))
    if dec.get("has_data") and dec["ratings"]:
        bad = [r for r in dec["ratings"] if r["rating"] == "costly"]
        good = [r for r in dec["ratings"] if r["rating"] == "good call"]
        if bad:
            L.append(f"'{bad[0]['tag']}' is costing you (~{bad[0]['recovery_delta']} recovery on the days after).")
        if good:
            L.append(f"'{good[0]['tag']}' is paying off (+{good[0]['recovery_delta']} recovery).")
    return {"narrative": " ".join(L), "generated": str(date.today())}


# ============================================================ DEEP ANALYTICS
def _onset_min(iso):
    if not iso:
        return None
    try:
        t = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return t.hour * 60 + t.minute
    except ValueError:
        return None


def _linreg(xs, ys):
    pts = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    if len(pts) < 4:
        return None
    n = len(pts)
    sx = sum(p[0] for p in pts); sy = sum(p[1] for p in pts)
    mx = sx / n; my = sy / n
    var = sum((p[0] - mx) ** 2 for p in pts)
    if var == 0:
        return None
    cov = sum((p[0] - mx) * (p[1] - my) for p in pts)
    slope = cov / var
    intercept = my - slope * mx
    resid = [p[1] - (slope * p[0] + intercept) for p in pts]
    rstd = statistics.pstdev(resid) if len(resid) > 1 else 0.0
    return {"slope": slope, "intercept": intercept, "mean_x": mx, "mean_y": my, "rstd": rstd, "n": n}


def _percentile(vals, v):
    vals = [x for x in vals if x is not None]
    if not vals or v is None:
        return None
    below = sum(1 for x in vals if x < v)
    return round(below / len(vals) * 100)


def feature_frame(uid):
    cached = _DCACHE.get((uid, "frame"))
    if cached and time.time() - cached[0] < _DCACHE_TTL:
        return cached[1]
    recs = {r["day"]: r for r in _recs(uid)}
    sleeps = {s["day"]: s for s in _sleeps(uid)}
    cycles = {c["day"]: c for c in _cycles(uid)}
    out = []
    for d, r in recs.items():
        try:
            dd = datetime.fromisoformat(d).date()
        except ValueError:
            continue
        prev = str(dd - timedelta(days=1))
        s = sleeps.get(prev) or sleeps.get(d)
        c = cycles.get(prev) or cycles.get(d)
        out.append({
            "day": d, "recovery": r.get("recovery_pct"), "hrv": r.get("hrv_rmssd_ms"),
            "rhr": r.get("resting_hr"), "spo2": r.get("spo2_pct"), "skin_temp": r.get("skin_temp_c"),
            "resp": s.get("respiratory_rate") if s else None,
            "sleep_quality": s.get("quality") if s else None,
            "sleep_hours": s.get("hours") if s else None,
            "bedtime": _onset_min(s.get("start")) if s else None,
            "strain": c.get("strain") if c else None,
        })
    out.sort(key=lambda x: x["day"])
    _DCACHE[(uid, "frame")] = (time.time(), out)
    return out


METRIC_LABELS = {"recovery": "Recovery %", "hrv": "HRV (ms)", "rhr": "Resting HR",
                 "sleep_quality": "Sleep quality", "sleep_hours": "Sleep hours",
                 "strain": "Strain", "resp": "Respiratory rate", "bedtime": "Bedtime consistency"}
HIGHER_BETTER = {"recovery": True, "hrv": True, "rhr": False, "sleep_quality": True,
                 "sleep_hours": True, "strain": None, "resp": False, "bedtime": None}


def driver_analysis(uid):
    fr = feature_frame(uid)
    rec = [r["recovery"] for r in fr]
    drivers = []
    for key in ("sleep_quality", "strain", "hrv", "rhr", "sleep_hours", "resp", "bedtime"):
        xs = [r[key] for r in fr]
        r = _pearson(xs, rec)
        if r is not None:
            drivers.append({"factor": METRIC_LABELS.get(key, key), "r": round(r, 2),
                            "direction": "raises" if r > 0 else "lowers",
                            "strength": "strong" if abs(r) >= .5 else "moderate" if abs(r) >= .3 else "weak"})
    # journal habits from decision engine
    dec = decision_report(uid)
    for rt in dec.get("ratings", []):
        if rt.get("recovery_delta") is not None and rt["n"] >= 3:
            drivers.append({"factor": "habit: " + rt["tag"], "r": None,
                            "direction": "raises" if rt["recovery_delta"] > 0 else "lowers",
                            "strength": "n=" + str(rt["n"]), "delta": rt["recovery_delta"]})
    drivers.sort(key=lambda d: abs(d["r"]) if d["r"] is not None else abs(d.get("delta", 0)) / 30, reverse=True)
    return {"drivers": drivers, "target": "next-day recovery"}


def why_day(uid):
    fr = feature_frame(uid)
    if not fr:
        return {"has_data": False}
    latest = fr[-1]
    reasons = []
    for key in ("sleep_quality", "strain", "hrv", "rhr", "resp", "sleep_hours"):
        series = [r[key] for r in fr[:-1] if r[key] is not None]
        v = latest.get(key)
        if v is None or len(series) < 8:
            continue
        m = statistics.fmean(series); sd = statistics.pstdev(series) or 1e-9
        z = (v - m) / sd
        if abs(z) < 0.8:
            continue
        hb = HIGHER_BETTER.get(key)
        good = (z > 0) if hb else (z < 0) if hb is False else None
        verb = "high" if z > 0 else "low"
        tag = "helped" if good else "hurt" if good is False else "notable"
        reasons.append({"factor": METRIC_LABELS.get(key, key), "value": _round(v),
                        "z": round(z, 1), "note": verb, "impact": tag})
    j = one("SELECT tags_json FROM journal WHERE user_id=? AND day=?",
            (uid, str((datetime.fromisoformat(latest['day']) - timedelta(days=1)).date())))
    tags = json.loads(j["tags_json"]) if j and j.get("tags_json") else []
    reasons.sort(key=lambda x: abs(x["z"]), reverse=True)
    return {"has_data": True, "day": latest["day"], "recovery": _round(latest["recovery"]),
            "reasons": reasons, "habits": tags}


def trends(uid):
    fr = feature_frame(uid)
    out = []
    for key in ("recovery", "hrv", "rhr", "sleep_quality", "sleep_hours", "strain"):
        series = [(r["day"], r[key]) for r in fr if r[key] is not None]
        vals = [v for _, v in series]
        if len(vals) < 7:
            continue

        def avg(n):
            w = vals[-n:]
            return round(statistics.fmean(w), 1) if w else None

        def window(a, b):
            w = vals[a:b]
            return statistics.fmean(w) if w else None
        last7, prev7 = window(-7, None), window(-14, -7)
        last30, prev30 = window(-30, None), window(-60, -30)
        wow = round(last7 - prev7, 1) if (last7 is not None and prev7 is not None) else None
        mom = round(last30 - prev30, 1) if (last30 is not None and prev30 is not None) else None
        out.append({"metric": METRIC_LABELS[key], "key": key, "latest": round(vals[-1], 1),
                    "avg7": avg(7), "avg30": avg(30), "avg90": avg(90),
                    "wow": wow, "mom": mom, "percentile": _percentile(vals, vals[-1]),
                    "higher_better": HIGHER_BETTER.get(key), "spark": [round(v, 1) for v in vals[-30:]]})
    return {"metrics": out}


def forecast(uid):
    fr = [r for r in feature_frame(uid) if r["recovery"] is not None]
    if len(fr) < 10:
        return {"has_data": False}
    # additive single-variable model: predicted = base + Σ slope_i*(x_today - mean_i)
    base = statistics.fmean([r["recovery"] for r in fr])
    latest = fr[-1]
    contribs = []
    pred = base
    total_resid = 0.0
    for key in ("sleep_quality", "strain", "hrv"):
        xs = [r[key] for r in fr]; ys = [r["recovery"] for r in fr]
        lr = _linreg(xs, ys)
        xt = latest.get(key)
        if lr and xt is not None:
            delta = lr["slope"] * (xt - lr["mean_x"])
            pred += delta
            total_resid += lr["rstd"] ** 2
            contribs.append({"factor": METRIC_LABELS[key], "effect": round(delta, 1)})
    pred = max(1, min(99, round(pred)))
    band = round((total_resid ** 0.5)) if total_resid else 8
    ew = early_warning(uid)
    return {"has_data": True, "predicted_recovery": pred, "range": [max(1, pred - band), min(99, pred + band)],
            "contributors": contribs, "illness_risk": ew["level"], "illness_conf": ew["confidence"],
            "note": "Estimated from how your recovery has historically responded to sleep, strain and HRV."}


def workout_analysis(uid):
    ws = rows("SELECT * FROM workouts WHERE user_id=? AND score_state=?", (uid, "SCORED"))
    by_sport = defaultdict(lambda: {"count": 0, "strain": [], "avg_hr": [], "kj": 0.0, "dist": 0.0})
    zones = [0, 0, 0, 0, 0, 0]
    for w in ws:
        sp = w.get("sport_name") or "workout"
        b = by_sport[sp]
        b["count"] += 1
        if w.get("strain") is not None: b["strain"].append(w["strain"])
        if w.get("avg_hr") is not None: b["avg_hr"].append(w["avg_hr"])
        b["kj"] += w.get("kilojoules") or 0
        b["dist"] += w.get("distance_m") or 0
        try:
            zs = _g(json.loads(w["raw_json"]), "score", "zone_durations") or {}
            keys = ["zone_zero_milli", "zone_one_milli", "zone_two_milli", "zone_three_milli",
                    "zone_four_milli", "zone_five_milli"]
            for i, k in enumerate(keys):
                zones[i] += zs.get(k) or 0
        except Exception:
            pass
    sports = [{"sport": sp, "count": b["count"], "avg_strain": _round(_mean(b["strain"])),
               "avg_hr": _round(_mean(b["avg_hr"])), "total_kj": round(b["kj"]),
               "km": round(b["dist"] / 1000, 1)} for sp, b in by_sport.items()]
    sports.sort(key=lambda s: s["count"], reverse=True)
    ztot = sum(zones) or 1
    zone_pct = [round(z / ztot * 100) for z in zones]
    # Foster monotony & strain on daily cycle strain (last 7 days)
    cyc = _cycles(uid)
    today = date.today()
    daily = [c["strain"] for c in cyc if c.get("strain") is not None
             and _date(c["start"]) and (today - _date(c["start"])).days < 7]
    monotony = strain_score = None
    if len(daily) >= 3:
        m = statistics.fmean(daily); sd = statistics.pstdev(daily) or 1e-9
        monotony = round(m / sd, 2)
        strain_score = round(sum(daily) * monotony)
    return {"sports": sports, "zone_pct": zone_pct, "has_zones": sum(zones) > 0,
            "monotony": monotony, "weekly_strain": strain_score,
            "monotony_note": ("High monotony (>2) with high load raises overtraining risk — vary your days."
                              if monotony and monotony > 2 else "Healthy training variation.")}


def circadian(uid):
    sl = [s for s in _sleeps(uid) if s.get("start") and s.get("end")][-60:]
    pts = []
    weekday_mid, weekend_mid = [], []
    for s in sl:
        on = _onset_min(s["start"]); wk = _onset_min(s["end"])
        if on is None or wk is None:
            continue
        dur = (s.get("total_in_bed_ms") or 0) / 60000
        mid = (on + dur / 2) % 1440
        d = datetime.fromisoformat(s["start"].replace("Z", "+00:00"))
        (weekend_mid if d.weekday() >= 5 else weekday_mid).append(mid)
        pts.append({"day": s["day"], "onset": on, "wake": wk})
    jetlag = None
    if weekday_mid and weekend_mid:
        jetlag = round(abs(statistics.fmean(weekend_mid) - statistics.fmean(weekday_mid)) / 60, 1)
    return {"points": pts, "regularity": sleep_regularity(uid), "social_jetlag_h": jetlag,
            "onset_std_min": round(statistics.pstdev([p["onset"] for p in pts])) if len(pts) > 2 else None}


def anomalies(uid):
    fr = feature_frame(uid)
    events = []
    for key in ("recovery", "hrv", "rhr", "resp", "skin_temp", "strain", "sleep_quality"):
        series = [(r["day"], r[key]) for r in fr if r[key] is not None]
        if len(series) < 15:
            continue
        vals = [v for _, v in series]
        for i in range(10, len(series)):
            base = vals[max(0, i - 30):i]
            if len(base) < 8:
                continue
            m = statistics.fmean(base); sd = statistics.pstdev(base) or 1e-9
            z = (vals[i] - m) / sd
            if abs(z) >= 2.2:
                events.append({"day": series[i][0], "metric": METRIC_LABELS.get(key, key),
                               "z": round(z, 1), "value": round(vals[i], 1),
                               "direction": "high" if z > 0 else "low"})
    events.sort(key=lambda e: e["day"], reverse=True)
    return {"events": events[:40]}


def daily_frame_rows(uid):
    return feature_frame(uid)


def period_compare(uid, a_from, a_to, b_from, b_to):
    fr = feature_frame(uid)

    def stats_for(f, t):
        sub = [r for r in fr if f <= r["day"] <= t]
        out = {"n": len(sub)}
        for key in ("recovery", "hrv", "rhr", "sleep_quality", "sleep_hours", "strain"):
            out[key] = _round(_mean([r[key] for r in sub]))
        return out
    A, B = stats_for(a_from, a_to), stats_for(b_from, b_to)
    deltas = {}
    for key in ("recovery", "hrv", "rhr", "sleep_quality", "sleep_hours", "strain"):
        if A.get(key) is not None and B.get(key) is not None:
            deltas[key] = round(B[key] - A[key], 1)
    return {"a": A, "b": B, "deltas": deltas, "labels": METRIC_LABELS}


# ---- goals ----
def set_goal(uid, metric, target, direction):
    upsert("goals", {"user_id": uid, "metric": metric, "target": target,
                     "direction": direction, "created_at": time.time()}, ["user_id", "metric"])


def goals_status(uid):
    gs = rows("SELECT * FROM goals WHERE user_id=?", (uid,))
    fr = feature_frame(uid)[-30:]
    out = []
    for g in gs:
        key = g["metric"]
        vals = [r.get(key) for r in fr if r.get(key) is not None]
        if not vals:
            out.append({**g, "adherence": None, "recent": None}); continue
        hit = sum(1 for v in vals if (v >= g["target"] if g["direction"] == "min" else v <= g["target"]))
        out.append({"metric": key, "label": METRIC_LABELS.get(key, key), "target": g["target"],
                    "direction": g["direction"], "adherence": round(hit / len(vals) * 100),
                    "recent": _round(statistics.fmean(vals))})
    return {"goals": out, "available": METRIC_LABELS}


# ---- peptide correlations ----
def peptide_correlations(uid):
    recs = {r["day"]: r for r in _recs(uid)}
    sleeps = {s["day"]: s for s in _sleeps(uid)}
    cycles = {c["day"]: c for c in _cycles(uid)}
    peps = rows("SELECT * FROM peptides WHERE user_id=?", (uid,))
    # (label, source, key, higher_is_better) ; None = neutral (not scored in verdict)
    METRIC_SPEC = [("Recovery %", recs, "recovery_pct", True), ("HRV ms", recs, "hrv_rmssd_ms", True),
                   ("Sleep performance %", sleeps, "performance_pct", True),
                   ("Sleep quality", sleeps, "quality", True), ("Resting HR", recs, "resting_hr", False),
                   ("Strain", cycles, "strain", None)]
    results = []
    for p in peps:
        taken = [x["day"] for x in rows(
            "SELECT day FROM peptide_log WHERE user_id=? AND peptide_id=? AND taken=1 ORDER BY day ASC",
            (uid, p["peptide_id"]))]
        if not taken:
            results.append({"name": p["name"], "status": "no doses logged yet"})
            continue
        taken_set = set(taken)
        metrics = []
        good = bad = 0
        for label, src, key, hb in METRIC_SPEC:
            on = [src[d][key] for d in src if d in taken_set and src[d].get(key) is not None]
            off = [src[d][key] for d in src if d not in taken_set and src[d].get(key) is not None]
            if len(on) >= 3 and len(off) >= 3:
                mon, moff = statistics.fmean(on), statistics.fmean(off)
                pooled = (statistics.pstdev(on) + statistics.pstdev(off)) / 2 or 1e-9
                eff = round((mon - moff) / pooled, 2)
                metrics.append({"label": label, "on": round(mon, 1), "off": round(moff, 1),
                                "delta": round(mon - moff, 1), "effect": eff,
                                "n_on": len(on), "n_off": len(off)})
                if hb is not None and abs(eff) >= 0.3:
                    helps = (eff > 0) == hb
                    good += 1 if helps else 0
                    bad += 1 if not helps else 0
        # weekly adherence vs weekly recovery correlation
        wk_adh, wk_rec = {}, defaultdict(list)
        days_sched = json.loads(p.get("days_json") or "[]")
        for d, r in recs.items():
            try:
                monday = week_start(datetime.fromisoformat(d).date())
            except ValueError:
                continue
            if r.get("recovery_pct") is not None:
                wk_rec[monday].append(r["recovery_pct"])
        for wk in wk_rec:
            wd = datetime.fromisoformat(wk).date()
            sched = len(days_sched) or 7
            got = sum(1 for i in range(7) if str(wd + timedelta(days=i)) in taken_set)
            wk_adh[wk] = got / sched * 100 if sched else 0
        pairs_x, pairs_y = [], []
        for wk in wk_rec:
            pairs_x.append(wk_adh.get(wk, 0)); pairs_y.append(statistics.fmean(wk_rec[wk]))
        adh_corr = _pearson(pairs_x, pairs_y)
        conf = "low" if (not metrics or min((m["n_on"] for m in metrics), default=0) < 7) else "moderate"
        if good > bad and good:
            verdict, vcolor = "helping", "good"
        elif bad > good and bad:
            verdict, vcolor = "possible adverse", "bad"
        else:
            verdict, vcolor = "neutral", "neutral"
        results.append({"name": p["name"], "dose": p.get("dose"), "doses": len(taken),
                        "metrics": metrics, "adherence_corr": _round(adh_corr, 2), "confidence": conf,
                        "verdict": verdict, "vcolor": vcolor})
    return {"peptides": results,
            "disclaimer": "Personal within-subject tracking only — correlation is not causation, "
                          "confounders exist, and small samples are noisy. Not medical advice."}


# ============================================================ POPULATION NORMS
# Study-based reference values. HRV rMSSD medians decline with age; women lower &
# resting HR higher than men. Sources: population HRV/RHR studies, NSF sleep guidance.
HRV_NORM = {(18, 25): (75, 60), (25, 35): (62, 50), (35, 45): (48, 40),
            (45, 55): (38, 32), (55, 65): (30, 26), (65, 120): (24, 22)}  # (male, female) ms


def _age_band(age):
    for lo, hi in ((18, 25), (25, 35), (35, 45), (45, 55), (55, 65), (65, 120)):
        if lo <= age < hi:
            return (lo, hi)
    return (25, 35)


def norm_values(age, sex):
    fem = (sex or "male").lower().startswith("f")
    hrv = HRV_NORM.get(_age_band(age or 30), (50, 42))[1 if fem else 0]
    return {"hrv": hrv, "rhr": 79 if fem else 74,
            "sleep_hours": 7.5 if (age or 30) >= 65 else 8.0, "resp": 15.0, "spo2": 97.0}


def metric_risk(key, v):
    """Plain-language safety read per metric: is it dangerous, why, and common causes.
    Informational only — thresholds from general clinical reference ranges."""
    if v is None:
        return None
    if key == "hrv":
        return {"level": "ok", "note": "Not dangerous on its own — HRV is a recovery signal, not a health alarm.",
                "causes": "stress, short or poor sleep, alcohol, hard training, illness — and it naturally falls with age",
                "why": "HRV shows how well your nervous system bounces back; higher means better recovery."}
    if key == "rhr":
        lvl = "concern" if v > 100 else ("watch" if v >= 90 else "ok")
        note = {"concern": "A resting heart rate over 100 bpm at rest is on the high side — pay attention to it. If it stays high or comes with palpitations or dizziness, it's worth mentioning to a doctor.",
                "watch": "A little elevated — usually stress, caffeine or fitness related. Worth keeping an eye on.",
                "ok": "Sits in the healthy range — nothing to worry about."}[lvl]
        return {"level": lvl, "note": note,
                "causes": "stress, dehydration, caffeine, alcohol, illness, poor sleep, or lower fitness",
                "why": "Resting heart rate reflects how hard your heart works at rest — lower is generally fitter."}
    if key == "resp":
        lvl = "concern" if v > 20 else ("watch" if v >= 18 else "ok")
        note = {"concern": "Consistently over 20 breaths/min is worth paying attention to — it's often an early sign of illness. Rest up; if it persists or you feel unwell, mention it to a doctor.",
                "watch": "Slightly high — often an early sign of illness or stress.",
                "ok": "In the healthy range — nothing to worry about."}[lvl]
        return {"level": lvl, "note": note,
                "causes": "an oncoming illness or fever, stress, alcohol, or a warm/stuffy room",
                "why": "Breathing rate during sleep is normally very steady, so a rise often shows up before you feel sick."}
    if key == "sleep_hours":
        lvl = "concern" if v < 6 else ("watch" if v < 7 else "ok")
        note = {"concern": "Regularly under 6h is linked to higher long-term risk for heart, metabolic and immune health — worth prioritising.",
                "watch": "A bit short of the 7–9h target.",
                "ok": "In the healthy 7–9h range."}[lvl]
        return {"level": lvl, "note": note,
                "causes": "late bedtimes, screens or caffeine too late, stress, or a packed schedule",
                "why": "Adults need 7–9h — sleep is when your body repairs and your brain consolidates."}
    if key == "spo2":
        lvl = "concern" if v < 90 else ("watch" if v < 95 else "ok")
        note = {"concern": "Below 90% overnight is worth paying attention to — if it keeps happening it's worth mentioning to a doctor, as it can relate to disrupted breathing such as sleep apnea.",
                "watch": "Slightly below the 95–100% norm — keep an eye on it.",
                "ok": "In the normal 95–100% range — nothing to worry about."}[lvl]
        return {"level": lvl, "note": note,
                "causes": "disrupted breathing / sleep apnea, congestion or illness, or high altitude",
                "why": "Blood oxygen shows how well you're oxygenating overnight; 95–100% is normal."}
    return None


def population_compare(uid):
    u = get_user(uid) or {}
    age, sex = u.get("age"), u.get("sex")
    if not age or not sex:
        return {"have_profile": False}
    recs, sleeps = _recs(uid), _sleeps(uid)
    SER = {"hrv": [r["hrv_rmssd_ms"] for r in recs], "rhr": [r["resting_hr"] for r in recs],
           "sleep_hours": [s["hours"] for s in sleeps], "resp": [s["respiratory_rate"] for s in sleeps],
           "spo2": [r["spo2_pct"] for r in recs]}

    def _win(vals):
        v = [x for x in vals if x is not None]
        if not v:
            return (None, None, None)
        return (v[-1], _mean(v[-7:]), _mean(v[-30:]))
    nv = norm_values(age, sex)
    META = {
        "hrv": ("HRV", "ms", True, "how recovered your nervous system is — higher means better recovery"),
        "rhr": ("Resting heart rate", "bpm", False, "how hard your heart works at rest — lower is fitter"),
        "sleep_hours": ("Sleep duration", "h", True, "hours slept per night — 7–9h is the healthy target"),
        "resp": ("Breathing rate", "/min", False, "breaths per minute while asleep — steady and low is good"),
        "spo2": ("Blood oxygen", "%", True, "oxygen in your blood — 95–100% is normal"),
    }
    sexword = "women" if sex.lower().startswith("f") else "men"
    rows = []
    for k, (label, unit, hb, explain) in META.items():
        d1, d7, d30 = _win(SER[k])
        yv, avg = d7, nv[k]
        if yv is None:
            continue
        better = (yv > avg) if hb else (yv < avg)
        pct = round((yv - avg) / avg * 100) if avg else 0
        status = "typical" if abs(pct) < 6 else ("better" if better else "below")
        word = {"better": "better than average 👍", "below": "below average", "typical": "about average"}[status]
        plain = f"Your {label} averages {round(yv,1)} {unit} over the last 7 days — {word} for {sexword} aged {age} (average is ~{avg} {unit})."
        ra = metric_risk(k, yv) or {}
        rows.append({"metric": label, "unit": unit, "you": _round(yv), "avg": avg, "status": status,
                     "d1": _round(d1), "d7": _round(d7), "d30": _round(d30),
                     "higher_better": hb, "explain": explain, "plain": plain,
                     "risk": ra.get("level", "ok"), "risk_note": ra.get("note", ""),
                     "causes": ra.get("causes", ""), "why": ra.get("why", "")})
    return {"have_profile": True, "age": age, "sex": sex, "rows": rows,
            "note": "Compared to published population averages for your age & sex. General reference, not a diagnosis."}


# ============================================================ OVERALL SUMMARY
REASONS = {
    "HRV (rMSSD)": ["short or poor sleep", "alcohol the night before", "high stress or hard training",
                    "fighting off something", "dehydration", "and it's naturally lower with age & genetics"],
    "Resting HR": ["incomplete recovery or overtraining", "alcohol or late caffeine", "stress or poor sleep",
                   "an oncoming illness", "dehydration", "lower fitness (this one's very trainable)"],
    "Respiratory rate": ["an oncoming cold/illness", "alcohol", "stress", "a warm or stuffy room"],
    "SpO₂": ["disrupted breathing during sleep", "congestion or illness", "high altitude",
             "and baseline varies a lot person-to-person"],
    "Skin temp": ["an oncoming fever/illness", "alcohol", "a warm room", "hormonal cycle"],
    "Sleep duration": ["late bedtimes", "screens or caffeine too late", "stress", "a packed schedule"],
}


def overall_health(uid):
    vitals = vitals_panel(uid)["vitals"]
    sm = sleep_medicine(uid)
    items = []  # (name, status good/watch/off, higher_better, plain)
    for v in vitals:
        items.append({"name": v["name"], "rating": v["rating"], "plain": v.get("plain", "")})
    if sm.get("enough"):
        dur = sm.get("plains", {}).get("duration", {})
        rate = {"great": "good", "okay": "watch", "poor": "off"}.get(dur.get("label"), "watch")
        items.append({"name": "Sleep duration", "rating": rate,
                      "plain": f"You average {sm.get('avg_hours')}h/night (7–9h is the target)."})
    good = [i for i in items if i["rating"] == "good"]
    watch = [i for i in items if i["rating"] == "watch"]
    off = [i for i in items if i["rating"] == "off"]
    if off:
        level, color = "Worth attention", "bad"
        verb = "is" if len(off) == 1 else "are"
        headline = f"Most signals look fine, but your {', '.join(i['name'] for i in off)} {verb} well outside your normal range — worth a closer look."
    elif watch:
        level, color = "Mostly good", "watch"
        headline = f"Solid overall — {len(good)} of your {len(items)} core signals are healthy. A couple ({', '.join(i['name'] for i in watch)}) are drifting a little."
    else:
        level, color = "Strong", "good"
        headline = "All your core health signals are in a healthy range right now — nice work."
    reasons = [{"name": i["name"], "reasons": REASONS.get(i["name"], [])}
               for i in (off + watch) if REASONS.get(i["name"])]
    return {"have_data": bool(items), "level": level, "color": color, "headline": headline,
            "good": [i["name"] for i in good], "watch": [i["name"] for i in watch],
            "off": [i["name"] for i in off], "reasons": reasons,
            "disclaimer": "A plain-language read of your data — not a medical diagnosis. See a clinician for anything concerning."}


def period_summary(uid):
    fr = feature_frame(uid)
    if len(fr) < 7:
        return {"enough": False}
    KEYS = [("recovery", "Recovery", "%", True), ("hrv", "HRV", "ms", True), ("rhr", "Resting HR", "bpm", False),
            ("sleep_hours", "Sleep", "h", True), ("sleep_quality", "Sleep quality", "", True),
            ("strain", "Strain", "", None)]
    rows, wins, losses = [], [], []
    for key, label, unit, hb in KEYS:
        vals = [r[key] for r in fr if r[key] is not None]
        if len(vals) < 7:
            continue
        a7 = statistics.fmean(vals[-7:])
        a30 = statistics.fmean(vals[-30:]) if len(vals) >= 8 else a7
        delta = a7 - a30
        if hb is None:
            direction = "steady"
        elif abs(delta) < (a30 * 0.03):
            direction = "steady"
        else:
            improving = (delta > 0) == hb
            direction = "improving" if improving else "declining"
            (wins if improving else losses).append(label)
        rows.append({"metric": label, "unit": unit, "avg7": round(a7, 1), "avg30": round(a30, 1),
                     "delta": round(delta, 1), "direction": direction, "higher_better": hb})
    bits = []
    if wins:
        bits.append("up vs your monthly average: " + ", ".join(wins))
    if losses:
        bits.append("down a bit: " + ", ".join(losses))
    narrative = ("This week " + "; ".join(bits) + ".") if bits else "This week is right in line with your monthly averages — steady and consistent."
    return {"enough": True, "rows": rows, "narrative": narrative}


# ============================================================ WEB APP
app = FastAPI(title="WHOOP Circle")


@app.on_event("startup")
def _startup():
    init_db()


def _set_session(resp, uid):
    resp.set_cookie("sid", signer.dumps(uid), httponly=True, max_age=31536000,
                    samesite="lax", secure=secure_cookies())
    return resp


_oauth_states = set()


@app.get("/login")
def login(request: Request):
    if missing_credentials():
        raise HTTPException(400, "Server has no WHOOP credentials configured.")
    state = secrets.token_urlsafe(16)
    _oauth_states.add(state)
    return RedirectResponse(build_authorize_url(state, redirect_uri(request)))


@app.get("/callback")
def callback(request: Request, code: str = "", state: str = "", error: str = ""):
    if error:
        return HTMLResponse(f"<h2>WHOOP error: {error}</h2><a href='/'>Back</a>", status_code=400)
    if state not in _oauth_states:
        return HTMLResponse("<h2>Invalid state. <a href='/login'>Retry</a></h2>", status_code=400)
    _oauth_states.discard(state)
    tok = exchange_code(code, redirect_uri(request))
    profile = fetch_profile(tok["access_token"])
    uid = upsert_user(profile)
    save_tokens(uid, tok["access_token"], tok.get("refresh_token"), tok.get("expires_in", 3600), tok.get("scope", ""))
    full_sync_async(uid)
    return _set_session(RedirectResponse("/?connected=1"), uid)


@app.post("/api/logout")
def logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("sid")
    return resp


@app.get("/api/me")
def api_me(request: Request):
    uid = current_uid(request)
    if uid is None:
        return {"signed_in": False, "has_credentials": not missing_credentials()}
    u = get_user(uid)
    return {"signed_in": True, "user_id": uid, "name": u["display_name"] if u else "You",
            "first_name": u["first_name"] if u else "", "connected": token_row(uid) is not None,
            "age": (u or {}).get("age"), "sex": (u or {}).get("sex")}


@app.post("/api/profile")
async def api_profile(request: Request):
    uid = require(request); body = await request.json()
    name = (body.get("display_name") or "").strip()
    if name:
        run("UPDATE users SET display_name=? WHERE user_id=?", (name, uid))
    if body.get("age") not in (None, ""):
        try:
            run("UPDATE users SET age=? WHERE user_id=?", (int(body["age"]), uid))
        except (ValueError, TypeError):
            pass
    if body.get("sex"):
        run("UPDATE users SET sex=? WHERE user_id=?", (body["sex"], uid))
    return {"ok": True}


@app.get("/api/health/population")
def api_population(request: Request):
    return population_compare(require(request))


# ---- Asset hosting (organ illustrations stored in DB, served as images) ----
ASSET_UPLOAD_TOKEN = "wc-organ-upload-7g2k"


@app.post("/api/asset/{name}")
async def api_asset_put(name: str, request: Request):
    if request.query_params.get("token") != ASSET_UPLOAD_TOKEN:
        raise HTTPException(403, "bad token")
    body = await request.json()
    data = (body.get("data") or "").split(",")[-1]  # strip data: prefix if present
    if not data:
        raise HTTPException(400, "no data")
    upsert("assets", {"name": name, "data": data,
                      "ctype": body.get("ctype", "image/jpeg"), "created_at": time.time()},
           ["name"])
    return {"ok": True, "name": name, "bytes": len(data)}


@app.get("/asset/{name}")
def api_asset_get(name: str):
    import base64 as _b64
    row = one("SELECT data, ctype FROM assets WHERE name=?", (name,))
    if not row:
        raise HTTPException(404, "not found")
    try:
        raw = _b64.b64decode(row["data"])
    except Exception:
        raise HTTPException(500, "decode error")
    return Response(content=raw, media_type=row["ctype"] or "image/jpeg",
                    headers={"Cache-Control": "public, max-age=86400"})


@app.post("/api/sync/recent")
def api_recent(request: Request):
    uid = require(request)
    return recent_sync(uid, 14)


@app.get("/api/sync/status")
def api_sync_status(request: Request):
    uid = require(request)
    return sync_status(uid)


@app.get("/api/overview")
def api_overview(request: Request):
    return overview(require(request))


@app.get("/api/dashboard")
def api_dashboard(request: Request):
    uid = require(request)
    rc = recovery_report(uid)
    return {"readiness": readiness(uid), "early_warning": early_warning(uid),
            "vitals": vitals_panel(uid)["vitals"], "load": load_acwr(uid),
            "sleep": sleep_medicine(uid), "overview": overview(uid),
            "narrative": weekly_narrative(uid)["narrative"],
            "recovery_series": rc["series"][-30:], "population": population_compare(uid),
            "overall": overall_health(uid)}


@app.get("/api/sleep")
def api_sleep(request: Request, window: int = 120):
    return sleep_report(require(request), window)


@app.get("/api/recovery")
def api_recovery(request: Request, window: int = 120):
    return recovery_report(require(request), window)


@app.get("/api/decisions")
def api_decisions(request: Request):
    return decision_report(require(request))


@app.get("/api/health/vitals")
def api_vitals(request: Request):
    return vitals_panel(require(request))


@app.get("/api/health/early-warning")
def api_earlywarning(request: Request):
    return early_warning(require(request))


@app.get("/api/health/readiness")
def api_readiness(request: Request):
    return readiness(require(request))


@app.get("/api/health/load")
def api_load(request: Request):
    return load_acwr(require(request))


@app.get("/api/health/sleepmed")
def api_sleepmed(request: Request):
    return sleep_medicine(require(request))


@app.get("/api/health/peptide-outcomes")
def api_peptide_outcomes(request: Request):
    return peptide_outcomes(require(request))


@app.get("/api/health/narrative")
def api_narrative(request: Request):
    return weekly_narrative(require(request))


@app.get("/api/health/summary")
def api_summary(request: Request):
    return overall_health(require(request))


@app.get("/api/insights/summary")
def api_insights_summary(request: Request):
    return period_summary(require(request))


@app.get("/api/insights/drivers")
def api_drivers(request: Request):
    return driver_analysis(require(request))


@app.get("/api/insights/why")
def api_why(request: Request):
    return why_day(require(request))


@app.get("/api/insights/trends")
def api_trends(request: Request):
    return trends(require(request))


@app.get("/api/insights/forecast")
def api_forecast(request: Request):
    return forecast(require(request))


@app.get("/api/insights/anomalies")
def api_anomalies(request: Request):
    return anomalies(require(request))


@app.get("/api/workouts")
def api_workouts(request: Request):
    return workout_analysis(require(request))


@app.get("/api/circadian")
def api_circadian(request: Request):
    return circadian(require(request))


@app.get("/api/explore")
def api_explore(request: Request):
    return {"rows": daily_frame_rows(require(request)), "labels": METRIC_LABELS}


@app.get("/api/explore/csv")
def api_explore_csv(request: Request):
    uid = require(request)
    fr = daily_frame_rows(uid)
    cols = ["day", "recovery", "hrv", "rhr", "spo2", "skin_temp", "resp",
            "sleep_quality", "sleep_hours", "bedtime", "strain"]
    lines = [",".join(cols)]
    for r in fr:
        lines.append(",".join("" if r.get(c) is None else str(r.get(c)) for c in cols))
    return Response("\n".join(lines), media_type="text/csv",
                    headers={"Content-Disposition": "attachment; filename=whoop_circle_data.csv"})


@app.get("/api/compare")
def api_compare(request: Request, a_from: str, a_to: str, b_from: str, b_to: str):
    return period_compare(require(request), a_from, a_to, b_from, b_to)


@app.get("/api/goals")
def api_goals(request: Request):
    return goals_status(require(request))


@app.post("/api/goals")
async def api_goals_save(request: Request):
    uid = require(request); b = await request.json()
    set_goal(uid, b["metric"], float(b["target"]), b.get("direction", "min"))
    return {"ok": True}


@app.post("/api/goals/delete")
async def api_goals_delete(request: Request):
    uid = require(request); b = await request.json()
    run("DELETE FROM goals WHERE user_id=? AND metric=?", (uid, b["metric"]))
    return {"ok": True}


@app.get("/api/peptides/correlations")
def api_peptide_corr(request: Request):
    return peptide_correlations(require(request))


@app.get("/api/journal")
def api_journal(request: Request):
    return get_journal(require(request))


@app.post("/api/journal")
async def api_journal_save(request: Request):
    uid = require(request); body = await request.json()
    day = body.get("day")
    if not day:
        raise HTTPException(400, "day required")
    tags = [t.strip() for t in body.get("tags", []) if t and t.strip()]
    upsert("journal", {"user_id": uid, "day": day, "mood": body.get("mood"),
                       "notes": body.get("notes", ""), "tags_json": json.dumps(tags),
                       "updated_at": time.time()}, ["user_id", "day"])
    return {"ok": True}


# ---- peptides ----
@app.get("/api/peptides")
def api_peptides(request: Request, week: str = ""):
    uid = require(request)
    ws = week or week_start(date.today())
    return peptides_view(uid, ws)


@app.post("/api/peptides")
async def api_peptides_save(request: Request):
    uid = require(request); b = await request.json()
    pid = b.get("peptide_id") or secrets.token_hex(6)
    upsert("peptides", {"peptide_id": pid, "user_id": uid, "name": b.get("name", "Peptide"),
                        "dose": b.get("dose", ""), "days_json": json.dumps(b.get("days", [])),
                        "active": 1 if b.get("active", True) else 0, "created_at": time.time()}, ["peptide_id"])
    return {"ok": True, "peptide_id": pid}


@app.post("/api/peptides/delete")
async def api_peptides_delete(request: Request):
    uid = require(request); b = await request.json()
    run("DELETE FROM peptides WHERE peptide_id=? AND user_id=?", (b.get("peptide_id"), uid))
    run("DELETE FROM peptide_log WHERE peptide_id=? AND user_id=?", (b.get("peptide_id"), uid))
    return {"ok": True}


@app.post("/api/peptides/toggle")
async def api_peptides_toggle(request: Request):
    uid = require(request); b = await request.json()
    upsert("peptide_log", {"user_id": uid, "peptide_id": b["peptide_id"], "day": b["day"],
                           "taken": 1 if b.get("taken") else 0, "updated_at": time.time()},
           ["user_id", "peptide_id", "day"])
    return {"ok": True}


@app.post("/api/peptides/note")
async def api_peptides_note(request: Request):
    uid = require(request); b = await request.json()
    upsert("peptide_notes", {"user_id": uid, "peptide_id": b["peptide_id"], "week_start": b["week_start"],
                             "note": b.get("note", ""), "updated_at": time.time()},
           ["user_id", "peptide_id", "week_start"])
    return {"ok": True}


# ---- circles ----
@app.get("/api/circles")
def api_circles(request: Request):
    return my_circles(require(request))


@app.post("/api/circles")
async def api_circles_create(request: Request):
    uid = require(request); b = await request.json()
    cid = secrets.token_hex(4)
    code = secrets.token_urlsafe(6)
    run("INSERT INTO circles(circle_id,name,owner_id,invite_code,created_at) VALUES(?,?,?,?,?)",
        (cid, b.get("name", "My Circle"), uid, code, time.time()))
    upsert("memberships", {"circle_id": cid, "user_id": uid, "status": "active", "role": "owner",
                           "joined_at": time.time()}, ["circle_id", "user_id"])
    return {"ok": True, "circle_id": cid, "invite_code": code}


@app.post("/api/circles/join")
async def api_circles_join(request: Request):
    uid = require(request); b = await request.json()
    code = (b.get("code") or "").strip()
    circle = one("SELECT * FROM circles WHERE invite_code=?", (code,))
    if not circle:
        raise HTTPException(404, "Invalid invite code.")
    existing = one("SELECT status FROM memberships WHERE circle_id=? AND user_id=?", (circle["circle_id"], uid))
    if existing:
        return {"ok": True, "status": existing["status"], "circle_id": circle["circle_id"]}
    upsert("memberships", {"circle_id": circle["circle_id"], "user_id": uid, "status": "pending",
                           "role": "member", "joined_at": time.time()}, ["circle_id", "user_id"])
    return {"ok": True, "status": "pending", "circle_id": circle["circle_id"], "name": circle["name"]}


@app.post("/api/circles/approve")
async def api_circles_approve(request: Request):
    uid = require(request); b = await request.json()
    cid = b.get("circle_id")
    if not one("SELECT 1 AS x FROM circles WHERE circle_id=? AND owner_id=?", (cid, uid)):
        raise HTTPException(403, "Only the owner can approve.")
    run("UPDATE memberships SET status=? WHERE circle_id=? AND user_id=?", ("active", cid, b.get("user_id")))
    return {"ok": True}


@app.post("/api/circles/decline")
async def api_circles_decline(request: Request):
    uid = require(request); b = await request.json()
    cid = b.get("circle_id")
    if not one("SELECT 1 AS x FROM circles WHERE circle_id=? AND owner_id=?", (cid, uid)):
        raise HTTPException(403, "Only the owner can remove members.")
    run("DELETE FROM memberships WHERE circle_id=? AND user_id=?", (cid, b.get("user_id")))
    return {"ok": True}


@app.post("/api/circles/leave")
async def api_circles_leave(request: Request):
    uid = require(request); b = await request.json()
    cid = b.get("circle_id")
    owner = one("SELECT owner_id FROM circles WHERE circle_id=?", (cid,))
    if owner and owner["owner_id"] == uid:
        run("DELETE FROM memberships WHERE circle_id=?", (cid,))
        run("DELETE FROM circles WHERE circle_id=?", (cid,))
    else:
        run("DELETE FROM memberships WHERE circle_id=? AND user_id=?", (cid, uid))
    return {"ok": True}


@app.post("/api/circles/revoke")
async def api_circles_revoke(request: Request):
    uid = require(request); b = await request.json()
    cid = b.get("circle_id")
    if not one("SELECT 1 AS x FROM circles WHERE circle_id=? AND owner_id=?", (cid, uid)):
        raise HTTPException(403, "Only the owner can revoke.")
    code = secrets.token_urlsafe(6)
    run("UPDATE circles SET invite_code=? WHERE circle_id=?", (code, cid))
    return {"ok": True, "invite_code": code}


@app.post("/api/circles/sharing")
async def api_circles_sharing(request: Request):
    uid = require(request); b = await request.json()
    cid = b.get("circle_id")
    if not one("SELECT 1 AS x FROM memberships WHERE circle_id=? AND user_id=? AND status=?", (cid, uid, "active")):
        raise HTTPException(403, "Not a member.")
    for k in ("share_recovery", "share_sleep", "share_strain", "share_hrv", "share_peptides"):
        if k in b:
            run(f"UPDATE memberships SET {k}=? WHERE circle_id=? AND user_id=?",
                (1 if b[k] else 0, cid, uid))
    return {"ok": True}


@app.get("/api/circle")
def api_circle(request: Request, id: str):
    return circle_detail(require(request), id)


@app.get("/report", response_class=HTMLResponse)
def report():
    return REPORT_PAGE


@app.get("/", response_class=HTMLResponse)
def index():
    return HTML_PAGE


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>WHOOP Circle</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
:root{--bg:#06080b;--bg2:#0b0f14;--panel:#0f151c;--panel2:#151d26;--line:#1e2732;--txt:#eaf1f8;--muted:#7d8b9a;--accent:#00e5a0;--accent2:#38bdf8;--red:#ff4d5e;--amber:#ffb020;--green:#16e0a3;--violet:#a78bfa;--pink:#f472b6}
*{box-sizing:border-box}html,body{margin:0}
body{background:radial-gradient(1200px 600px at 82% -8%,rgba(0,229,160,.07),transparent),radial-gradient(900px 500px at 10% 110%,rgba(56,189,248,.05),transparent),var(--bg);color:var(--txt);font:15px/1.5 Inter,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;-webkit-font-smoothing:antialiased}
.app{display:flex;min-height:100vh}
.side{width:236px;flex:0 0 236px;background:linear-gradient(180deg,var(--bg2),var(--bg));border-right:1px solid var(--line);position:sticky;top:0;height:100vh;display:flex;flex-direction:column;padding:18px 12px;overflow-y:auto}
.brand{font-weight:800;letter-spacing:1px;font-size:16px;padding:6px 10px 14px}.brand b{color:var(--accent)}
.nav{display:flex;flex-direction:column;gap:2px}
.navgroup{font-size:10px;letter-spacing:1.4px;color:#55636f;font-weight:700;text-transform:uppercase;padding:14px 12px 5px}
.nav button{display:flex;align-items:center;gap:11px;background:none;border:none;color:var(--muted);text-align:left;padding:9px 12px;border-radius:9px;font-size:14px;font-weight:600;cursor:pointer;width:100%}
.nav button .ic{width:16px;text-align:center}
.nav button:hover{background:var(--panel);color:var(--txt)}
.nav button.active{background:linear-gradient(90deg,rgba(0,229,160,.16),rgba(0,229,160,.02));color:var(--txt);box-shadow:inset 2px 0 0 var(--accent)}
.sidefoot{margin-top:auto;border-top:1px solid var(--line);padding-top:12px;font-size:13px;color:var(--muted)}
.sidefoot .nm{color:var(--txt);font-weight:700;margin-bottom:8px;padding-left:2px}
.main{flex:1;min-width:0;padding:24px 28px 60px}
.topbar{display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;gap:14px;flex-wrap:wrap}
.topbar h2{margin:0;font-size:23px;font-weight:800;letter-spacing:-.3px}
.pill{font-size:12px;color:var(--muted);border:1px solid var(--line);border-radius:20px;padding:5px 12px;background:var(--panel)}
button.b{cursor:pointer;border:none;border-radius:9px;padding:9px 14px;font-weight:700;font-size:13px}
.b.p{background:var(--accent);color:#04130b}.b.g{background:var(--panel2);color:var(--txt);border:1px solid var(--line)}
.b.sm{padding:5px 10px;font-size:12px}.b.red{background:#3a1b22;color:#ff9aa6;border:1px solid #5a2730}
.grid{display:grid;gap:16px}.g4{grid-template-columns:repeat(4,1fr)}.g3{grid-template-columns:repeat(3,1fr)}.g2{grid-template-columns:repeat(2,1fr)}
.cards{grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}
@media(max-width:900px){.side{display:none}.g4,.g3,.g2{grid-template-columns:1fr}.main{padding:16px}}
.panel{background:linear-gradient(180deg,var(--panel),var(--bg2));border:1px solid var(--line);border-radius:18px;padding:20px;box-shadow:0 1px 0 rgba(255,255,255,.02) inset,0 8px 24px rgba(0,0,0,.18)}
.panel h3{margin:0 0 12px;font-size:12px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:700}
.span2{grid-column:span 2}.span3{grid-column:span 3}
.big{font-size:42px;font-weight:800;line-height:1;letter-spacing:-1px}
.lbl{font-size:11px;text-transform:uppercase;letter-spacing:1.1px;color:var(--muted);font-weight:700}
.v{font-size:28px;font-weight:800;letter-spacing:-.5px;margin-top:2px}.v small{font-size:13px;color:var(--muted);font-weight:600}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;vertical-align:middle;margin-right:6px}
.d-optimal,.d-good{background:var(--green)}.d-watch{background:var(--amber)}.d-flag,.d-bad{background:var(--red)}
.chip{display:inline-block;background:var(--panel2);border:1px solid var(--line);border-radius:7px;padding:3px 10px;margin:2px;font-size:12px}
.badge{display:inline-block;background:var(--panel2);border:1px solid var(--line);border-radius:20px;padding:4px 12px;margin:3px;font-size:13px}
table{width:100%;border-collapse:collapse;font-size:14px}th,td{text-align:left;padding:7px 9px;border-bottom:1px solid var(--line)}
th{color:var(--muted);font-weight:700;font-size:11px;text-transform:uppercase;letter-spacing:.5px}
tr.me td{background:rgba(0,229,160,.08)}
.refbar{height:9px;border-radius:6px;background:var(--panel2);position:relative;margin-top:8px;overflow:hidden}
.refband{position:absolute;top:0;bottom:0;background:rgba(0,229,160,.22)}.refmark{position:absolute;top:-3px;width:3px;height:15px;border-radius:2px;background:var(--txt)}
.pctbar{height:7px;border-radius:5px;background:var(--panel2);overflow:hidden;margin-top:6px}.pctfill{height:100%;background:linear-gradient(90deg,var(--accent2),var(--accent))}
input,textarea,select{background:var(--panel2);border:1px solid var(--line);color:var(--txt);border-radius:9px;padding:8px 10px;font:inherit;width:100%}
label{font-size:12px;color:var(--muted);display:block;margin:9px 0 4px}
.row{display:flex;gap:10px;flex-wrap:wrap}.row>div{flex:1;min-width:120px}
.muted{color:var(--muted)}.small{font-size:13px}.pos{color:var(--green)}.neg{color:var(--red)}
.insight{background:var(--panel2);border-left:3px solid var(--accent2);padding:10px 13px;border-radius:6px;margin-bottom:8px;font-size:14px}
.banner{border-radius:14px;padding:15px 17px;border:1px solid var(--line);font-size:14px}
.ban-ok{background:linear-gradient(90deg,rgba(22,224,163,.12),transparent);border-color:rgba(22,224,163,.4)}
.ban-watch{background:linear-gradient(90deg,rgba(255,176,32,.12),transparent);border-color:rgba(255,176,32,.4)}
.ban-alert{background:linear-gradient(90deg,rgba(255,77,94,.14),transparent);border-color:rgba(255,77,94,.5)}
.tag{display:inline-block;background:var(--panel2);border:1px solid var(--line);border-radius:7px;padding:2px 9px;margin:2px;font-size:12px;cursor:pointer}
.tag.on{background:var(--accent);color:#04130b;border-color:var(--accent)}
.hidden{display:none}.center{text-align:center;padding:80px 20px}
.spin{display:inline-block;width:12px;height:12px;border:2px solid var(--line);border-top-color:var(--accent);border-radius:50%;animation:s .8s linear infinite;vertical-align:middle}
@keyframes s{to{transform:rotate(360deg)}}
.pbox{width:30px;height:30px;border-radius:7px;border:1px solid var(--line);background:var(--panel2);display:inline-flex;align-items:center;justify-content:center;cursor:pointer;font-size:13px}
.pbox.on{background:var(--accent);color:#04130b;border-color:var(--accent)}.pbox.off{opacity:.3;cursor:default}
.ccard{cursor:pointer}.ccard:hover{border-color:var(--accent)}
.toggle{display:flex;align-items:center;gap:8px;margin:6px 0;font-size:14px}.toggle input{width:auto}
.code{font-family:ui-monospace,Menlo,monospace;background:var(--panel2);border:1px solid var(--line);border-radius:6px;padding:4px 9px}
.rank{color:var(--muted);width:22px;display:inline-block}
.reveal{animation:rv .4s ease}@keyframes rv{from{opacity:0;transform:translateY(6px)}to{opacity:1}}
a{color:var(--accent2)}
.hm{display:flex;flex-direction:column;gap:2px}.hmrow{display:flex;align-items:center;gap:8px;font-size:11px;color:var(--muted)}
.hmtrack{flex:1;height:11px;border-radius:4px;background:var(--panel2);position:relative}
.hmbar{position:absolute;top:0;bottom:0;background:linear-gradient(90deg,var(--violet),var(--accent2));border-radius:4px;opacity:.85}
#bar{position:fixed;top:0;left:0;height:3px;width:0;opacity:0;background:linear-gradient(90deg,var(--accent2),var(--accent));z-index:99;transition:width .3s ease,opacity .3s ease;box-shadow:0 0 8px var(--accent)}
.ch{position:relative;width:100%;height:200px;margin-top:8px}.ch canvas{position:absolute!important;inset:0;width:100%!important;height:100%!important}
.tl{border-left:2px solid var(--line);margin-left:6px;padding-left:14px}
.tlrow{position:relative;padding:7px 0;font-size:13.5px}
.tlrow:before{content:'';position:absolute;left:-21px;top:12px;width:9px;height:9px;border-radius:50%;background:var(--accent2)}
</style></head><body>
<div id="bar"></div>
<div id="signin" class="center hidden">
<div style="font-size:30px;font-weight:800;letter-spacing:1px">WHOOP <span style="color:var(--accent)">CIRCLE</span></div>
<p class="muted" style="max-width:460px;margin:14px auto">Clinical-grade analytics on your WHOOP data — a full command center, deep insights, forecasting, a peptide tracker with correlations, and private friend circles.</p>
<button class="b p" style="font-size:15px;padding:12px 22px" onclick="location.href='/login'">Sign in with WHOOP</button>
<p class="muted small" id="credWarn"></p></div>

<div id="app" class="app hidden">
<aside class="side">
<div class="brand">WHOOP <b>CIRCLE</b></div>
<div class="nav" id="nav">
<button data-tab="dashboard" class="active" onclick="tab('dashboard')"><span class="ic">◎</span> Dashboard</button>
<div class="navgroup">Health</div>
<button data-tab="vitals" onclick="tab('vitals')"><span class="ic">✦</span> Vitals</button>
<button data-tab="sleep" onclick="tab('sleep')"><span class="ic">☾</span> Sleep</button>
<button data-tab="load" onclick="tab('load')"><span class="ic">▲</span> Recovery &amp; Load</button>
<div class="navgroup">Analysis</div>
<button data-tab="insights" onclick="tab('insights')"><span class="ic">◈</span> Insights</button>
<button data-tab="explore" onclick="tab('explore')"><span class="ic">⚡</span> Explore</button>
<div class="navgroup">Track</div>
<button data-tab="peptides" onclick="tab('peptides')"><span class="ic">⬡</span> Peptides</button>
<button data-tab="journal" onclick="tab('journal')"><span class="ic">✎</span> Journal</button>
<div class="navgroup">Social</div>
<button data-tab="circles" onclick="tab('circles')"><span class="ic">◍</span> Circles</button>
<div class="navgroup"></div>
<button data-tab="report" onclick="tab('report')"><span class="ic">▤</span> Report</button>
<button data-tab="settings" onclick="tab('settings')"><span class="ic">⚙</span> Settings</button>
</div>
<div class="sidefoot"><div class="nm" id="sideName"></div>
<span class="pill" id="statusPill"></span>
<div style="margin-top:10px;display:flex;gap:8px"><button class="b g sm" onclick="recentSync()">Sync</button><button class="b g sm" onclick="signOut()">Sign out</button></div></div>
</aside>
<main class="main">
<div class="topbar"><h2 id="ttl">Dashboard</h2><span class="muted small" id="asOf"></span></div>

<section id="tab-dashboard">
<div class="grid g4">
<div class="panel" style="grid-row:span 2;display:flex;flex-direction:column;align-items:center;justify-content:center"><h3 style="align-self:flex-start">Readiness</h3><div id="dRing"></div><div id="dRec" class="small muted" style="text-align:center;margin-top:8px"></div></div>
<div class="panel span3"><h3>Early-warning</h3><div class="muted small" style="margin:-6px 0 8px">Watches for early signs of illness or overtraining — before you feel them.</div><div id="dEw"></div></div>
<div class="panel"><h3>Load</h3><div id="dLoad"></div></div>
<div class="panel"><h3>Sleep</h3><div id="dSleep"></div></div>
<div class="panel"><h3>Latest</h3><div id="dLatest"></div></div>
</div>
<div class="panel" style="margin-top:15px"><h3>Your overall health picture</h3><div id="dOverall"></div></div>
<div class="panel" style="margin-top:15px"><h3>Vitals snapshot</h3><div class="muted small" style="margin:-6px 0 10px">Your key body signals vs <i>your own</i> normal. Green dot = healthy · amber = keep an eye · red = off.</div><div class="grid cards" id="dVitals"></div></div>
<div class="grid g2" style="margin-top:15px">
<div class="panel"><h3>Recovery &amp; strain — last 30</h3><div class="muted small" style="margin:-6px 0 6px">Green = how recovered you are · Orange = how hard you pushed that day.</div><div class="ch"><canvas id="dChart"></canvas></div></div>
<div class="panel"><h3>Your weekly read-out</h3><div id="dNarr" class="reveal" style="font-size:15px;line-height:1.7"></div>
<div id="dStreaks" style="margin-top:12px"></div></div>
</div>
<div class="panel" style="margin-top:15px"><h3>How you compare to people your age &amp; sex</h3><div id="dCompare"></div></div>
</section>

<section id="tab-vitals" class="hidden">
<div class="panel" id="vEw" style="margin-bottom:15px"></div>
<div class="panel" style="margin-bottom:15px"><h3>How you compare to people your age &amp; sex</h3><div id="vCompare"></div></div>
<div class="grid g2" id="vFull"></div>
<p class="muted small" style="margin-top:10px">Each marker compares to <b>your own</b> rolling baseline (shaded band = typical range). z = SDs from baseline · CV = day-to-day variability.</p>
</section>

<section id="tab-sleep" class="hidden">
<div class="grid g4">
<div class="panel"><h3>Regularity</h3><div id="sSri" style="text-align:center"></div></div>
<div class="panel"><h3>Sleep debt (7d)</h3><div id="sDebt"></div></div>
<div class="panel"><h3>Avg duration</h3><div id="sDur"></div></div>
<div class="panel"><h3>Social jetlag</h3><div id="sJet"></div></div>
</div>
<div class="panel" style="margin-top:15px"><h3>Is your sleep healthy? — plain read</h3><div id="sRisk"></div></div>
<div class="grid g2" style="margin-top:15px">
<div class="panel"><h3>Architecture vs clinical norms</h3><div class="ch"><canvas id="sArch"></canvas></div><div id="sArchNote" class="small muted" style="margin-top:8px"></div></div>
<div class="panel"><h3>Stage mix</h3><div class="ch"><canvas id="sStage"></canvas></div></div>
</div>
<div class="panel" style="margin-top:15px"><h3>Bedtime &amp; wake heatmap (last 30 nights)</h3><div id="sHeat"></div><div class="small muted" style="margin-top:6px">Each bar = time asleep across a 24h day (6pm → 6pm). Tight alignment = strong circadian rhythm.</div></div>
<div class="panel" style="margin-top:15px"><h3>Sleep quality trend</h3><div class="ch"><canvas id="sTrend"></canvas></div></div>
</section>

<section id="tab-load" class="hidden">
<div class="grid g4">
<div class="panel"><h3>ACWR</h3><div id="lAcwr" style="text-align:center"></div></div>
<div class="panel"><h3>Acute (7d)</h3><div id="lAcute"></div></div>
<div class="panel"><h3>Chronic (28d)</h3><div id="lChronic"></div></div>
<div class="panel"><h3>Monotony</h3><div id="lMono"></div></div>
</div>
<div class="panel" style="margin-top:15px"><h3>Strain &amp; workload ratio</h3><div class="ch"><canvas id="lChart"></canvas></div></div>
<div class="grid g2" style="margin-top:15px">
<div class="panel"><h3>Recovery vs Strain</h3><div class="ch"><canvas id="lRec"></canvas></div></div>
<div class="panel"><h3>HR-zone distribution</h3><div class="ch"><canvas id="lZones"></canvas></div></div>
</div>
<div class="panel" style="margin-top:15px"><h3>By sport</h3><div class="ch"><canvas id="lSports"></canvas></div></div>
</section>

<section id="tab-insights" class="hidden">
<div class="panel" style="margin-bottom:15px"><h3>Last 7 days vs last 30 days</h3><div id="iSummary"></div></div>
<div class="grid g2">
<div class="panel"><h3>What drives your recovery</h3><div class="ch"><canvas id="iDrivers"></canvas></div><div class="small muted" style="margin-top:6px">Correlation of each factor with next-day recovery (−1 to +1).</div></div>
<div class="panel"><h3>Forecast — tomorrow's recovery</h3><div id="iForecast"></div></div>
</div>
<div class="panel" style="margin-top:15px"><h3>Why today looks the way it does</h3><div id="iWhy"></div></div>
<div class="panel" style="margin-top:15px"><h3>Trends, deltas &amp; percentiles</h3><div class="grid cards" id="iTrends"></div></div>
<div class="panel" style="margin-top:15px"><h3>Anomaly timeline</h3><div id="iAnom"></div></div>
</section>

<section id="tab-explore" class="hidden">
<div class="panel"><h3>Interactive explorer</h3>
<div class="row"><div><label>Metric A</label><select id="eA"></select></div><div><label>Metric B</label><select id="eB"></select></div>
<div><label>Days</label><select id="eDays"><option value="30">30</option><option value="60">60</option><option value="90" selected>90</option><option value="9999">All</option></select></div>
<div style="flex:0;display:flex;align-items:flex-end"><button class="b g sm" onclick="location.href='/api/explore/csv'">⭳ CSV</button></div></div>
<div class="ch"><canvas id="eChart"></canvas></div></div>
<div class="grid g2" style="margin-top:15px">
<div class="panel"><h3>Goals</h3><div id="gList"></div>
<div class="row" style="margin-top:10px"><div><label>Metric</label><select id="gMetric"></select></div><div><label>Target</label><input id="gTarget" type="number" step="0.1"></div>
<div><label>Direction</label><select id="gDir"><option value="min">at least</option><option value="max">at most</option></select></div>
<div style="flex:0;display:flex;align-items:flex-end"><button class="b p sm" onclick="saveGoal()">Add</button></div></div></div>
<div class="panel"><h3>Compare two periods</h3>
<div class="row"><div><label>A from</label><input type="date" id="cAf"></div><div><label>A to</label><input type="date" id="cAt"></div></div>
<div class="row"><div><label>B from</label><input type="date" id="cBf"></div><div><label>B to</label><input type="date" id="cBt"></div></div>
<button class="b g sm" style="margin-top:10px" onclick="runCompare()">Compare</button>
<div class="ch"><canvas id="cChart"></canvas></div></div>
</div>
</section>

<section id="tab-peptides" class="hidden">
<div class="panel"><h3>Add a peptide</h3>
<div class="row"><div><label>Name</label><input id="pName" placeholder="e.g. GHK-Cu" oninput="onPepName()" autocomplete="off"></div><div><label>Dose</label><input id="pDose" placeholder="e.g. 2mg"></div></div>
<div id="pRec"></div>
<label>Days of the week</label><div id="pDays"></div>
<div style="margin-top:10px"><button class="b p" onclick="addPeptide()">Add peptide</button></div></div>
<div class="panel"><h3>My peptides</h3><div id="pVials" class="grid cards"></div></div>
<div class="panel"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
<h3 style="margin:0">Week of <span id="pWeekLabel"></span></h3>
<div><button class="b g sm" onclick="shiftWeek(-1)">‹ Prev</button> <button class="b g sm" onclick="shiftWeek(1)">Next ›</button></div></div>
<div id="pGrid" style="overflow-x:auto"></div></div>
<div class="panel"><h3>Outcomes — before vs during</h3><div id="pOut"></div></div>
<div class="panel"><h3>Peptide ↔ health correlations</h3>
<p class="muted small" style="margin-top:-6px" id="pCorrNote"></p><div id="pCorr"></div></div>
<div class="panel"><h3>Peptide library</h3><div id="pLib" class="grid cards"></div></div>
</section>

<section id="tab-journal" class="hidden">
<div class="panel"><h3>Log a day</h3>
<div class="row"><div><label>Date</label><input type="date" id="jDay"></div><div><label>Mood (1–5)</label><input type="number" min="1" max="5" id="jMood" placeholder="optional"></div></div>
<label>Habits &amp; decisions</label><div id="tagBox"></div>
<input id="jCustom" placeholder="add custom tag + Enter" style="margin-top:8px">
<label>Notes</label><textarea id="jNotes" rows="2"></textarea>
<div style="margin-top:10px"><button class="b p" onclick="saveJournal()">Save day</button> <span id="jMsg" class="muted"></span></div></div>
<div class="panel"><h3>Decision impact</h3><div id="decBaseline" class="muted small" style="margin-bottom:10px"></div><div id="decTable"></div></div>
<div class="panel"><h3>Logged days</h3><div id="jList"></div></div>
</section>

<section id="tab-circles" class="hidden">
<div id="circlesList">
<div class="panel"><div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:8px">
<button class="b p" onclick="createCircle()">+ Create circle</button><button class="b g" onclick="joinCircle()">Join with code</button></div>
<span class="muted small">Circles are private &amp; invite-only. You only see people inside a circle you share, and you approve every member.</span></div>
<div id="circleCards" class="grid cards"></div></div>
<div id="circleDetail" class="hidden"></div>
</section>

<section id="tab-report" class="hidden">
<div class="panel"><h3>Health report</h3><p class="muted">A clean, printable summary of your readiness, vitals, sleep, load and peptide analytics.</p>
<button class="b p" onclick="window.open('/report','_blank')">Open printable report</button></div>
</section>

<section id="tab-settings" class="hidden">
<div class="panel"><h3>Display name</h3><p class="muted small">What friends see inside your circles.</p>
<div class="row"><div><input id="setName"></div><div style="flex:0"><button class="b p" onclick="saveName()">Save</button></div></div><span class="muted small" id="setMsg"></span></div>
<div class="panel"><h3>Age &amp; sex</h3><p class="muted small">Used to compare your stats against the average person your age &amp; sex (WHOOP doesn't share this). Private to you.</p>
<div class="row"><div><label>Age</label><input id="setAge" type="number" min="13" max="100" placeholder="e.g. 28"></div><div><label>Sex</label><select id="setSex"><option value="">—</option><option value="male">Male</option><option value="female">Female</option></select></div><div style="flex:0;display:flex;align-items:flex-end"><button class="b p" onclick="saveProfile()">Save</button></div></div><span class="muted small" id="setMsg2"></span></div>
<div class="panel"><h3>About the analytics</h3><p class="muted small">Everything is computed against your own rolling baseline using personal z-scores, EWMA training load (ACWR), and published clinical/sports-science thresholds. Informational only — not medical advice.</p></div>
<div class="panel"><h3>Account</h3><button class="b red" onclick="signOut()">Sign out</button></div>
</section>
</main></div>
<script>
const $=s=>document.querySelector(s);
let _inflight=0;
async function api(u,o){_inflight++;const bar=document.getElementById('bar');if(bar){bar.style.opacity='1';bar.style.width='75%';}
 try{for(let i=0;i<5;i++){try{const r=await fetch(u,o);const t=await r.text();return JSON.parse(t);}catch(e){if(i===4)throw e;await new Promise(z=>setTimeout(z,1200));}}}
 finally{_inflight=Math.max(0,_inflight-1);if(_inflight===0&&bar){bar.style.width='100%';setTimeout(()=>{bar.style.width='0';bar.style.opacity='0';},260);}}}
const fmt=v=>(v===null||v===undefined||v==='')?'—':v;
const DAYS=['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
const TITLES={dashboard:'Dashboard',vitals:'Vitals',sleep:'Sleep',load:'Recovery & Load',insights:'Insights',explore:'Explore',peptides:'Peptides',journal:'Journal',circles:'Circles',report:'Report',settings:'Settings'};
let charts={},pWeek=null,me=null,curCircle=null,exploreRows=null;
const bandColor=b=>({prime:'var(--green)',moderate:'var(--amber)',recover:'var(--red)'}[b]||'var(--muted)');
const zoneColor=z=>({optimal:'var(--green)',detraining:'var(--amber)',caution:'var(--amber)','high risk':'var(--red)',danger:'var(--red)'}[z]||'var(--muted)');
const stColor=s=>({optimal:'var(--green)',watch:'var(--amber)',flag:'var(--red)'}[s]||'var(--muted)');

function spark(vals,color,w,h){w=w||140;h=h||30;const v=vals.filter(x=>x!=null);if(v.length<2)return'';
 const mn=Math.min(...v),mx=Math.max(...v),rg=(mx-mn)||1;
 const pts=v.map((y,i)=>[(i/(v.length-1))*w,h-2-((y-mn)/rg)*(h-4)]);
 return'<svg width="'+w+'" height="'+h+'" viewBox="0 0 '+w+' '+h+'"><path d="'+pts.map((p,i)=>(i?'L':'M')+p[0].toFixed(1)+' '+p[1].toFixed(1)).join(' ')+'" fill="none" stroke="'+color+'" stroke-width="2" stroke-linejoin="round"/></svg>';}
function ring(score,color,sub,size){size=size||150;const R=size/2-13,C=2*Math.PI*R,off=C*(1-(score==null?0:score)/100);const c=size/2;
 return'<svg width="'+size+'" height="'+size+'" viewBox="0 0 '+size+' '+size+'"><circle cx="'+c+'" cy="'+c+'" r="'+R+'" fill="none" stroke="var(--panel2)" stroke-width="12"/>'+
 '<circle cx="'+c+'" cy="'+c+'" r="'+R+'" fill="none" stroke="'+color+'" stroke-width="12" stroke-linecap="round" stroke-dasharray="'+C+'" stroke-dashoffset="'+off+'" transform="rotate(-90 '+c+' '+c+')"/>'+
 '<text x="'+c+'" y="'+(c-3)+'" text-anchor="middle" font-size="'+(size/4.2)+'" font-weight="800" fill="var(--txt)">'+(score==null?'—':score)+'</text>'+
 '<text x="'+c+'" y="'+(c+18)+'" text-anchor="middle" font-size="11" fill="var(--muted)" letter-spacing="1">'+(sub||'').toUpperCase()+'</text></svg>';}
function refbar(low,high,latest){const pad=(high-low)||1,min=low-pad,max=high+pad,rg=(max-min)||1;
 return'<div class="refbar"><div class="refband" style="left:'+((low-min)/rg*100)+'%;width:'+((high-low)/rg*100)+'%"></div><div class="refmark" style="left:'+Math.max(0,Math.min(100,(latest-min)/rg*100))+'%"></div></div>';}
function pctbar(p){return'<div class="pctbar"><div class="pctfill" style="width:'+(p||0)+'%"></div></div>';}
/* peptide catalog: name -> [use, liquid tint] */
const PEPTINFO={'BPC-157':['tissue repair / gut','#bcdcff'],'TB-500':['recovery / healing','#d3c8ff'],'GHK-Cu':['skin / collagen','#5fc9c0'],'Ipamorelin':['GH secretagogue','#e4d8ff'],'CJC-1295':['GH secretagogue','#c7e2ff'],'Sermorelin':['GH / sleep','#d2efe4'],'Tesamorelin':['fat loss / GH','#c7e2ff'],'GHRP-2':['GH / appetite','#e4d8ff'],'GHRP-6':['GH / appetite','#e6dcff'],'Hexarelin':['GH pulse','#d7e6ff'],'MK-677':['oral GH / sleep','#ffdfae'],'IGF-1 LR3':['growth / recovery','#c7e2ff'],'Melanotan II':['tan / libido','#e6bf94'],'Melanotan I':['tan','#edcfa2'],'PT-141':['libido','#f5c2d3'],'Semaglutide':['GLP-1 / weight','#dce9ff'],'Tirzepatide':['GLP-1/GIP / weight','#dce9ff'],'Retatrutide':['triple-G / weight','#dce9ff'],'AOD-9604':['fat loss','#d2efe4'],'Epitalon':['longevity / sleep','#e3d7ff']};
const _norm=s=>(s||'').toLowerCase().replace(/[^a-z0-9]/g,'');
function matchPep(name){const n=_norm(name);if(n.length<2)return null;return Object.keys(PEPTINFO).find(k=>{const kk=_norm(k);return kk.startsWith(n)||n.startsWith(kk)||(n.length>=3&&kk.includes(n));})||null;}
function pepTint(name){const k=matchPep(name);return k?PEPTINFO[k][1]:'#8be3c9';}
/* suggested weekly schedule (schedule pattern only, NOT dosing advice) */
const DAILY=[0,1,2,3,4,5,6];
const PEPSUGGEST={'Semaglutide':[[0],'typically once weekly'],'Tirzepatide':[[0],'typically once weekly'],'Retatrutide':[[0],'typically once weekly'],'PT-141':[[],'used as-needed'],'Melanotan I':[[0,2,4],'often a few days/week'],'Hexarelin':[[0,1,2,3,4],'often 5 days/week']};
function pepSuggest(k){return PEPSUGGEST[k]||[DAILY,'commonly run daily'];}
function onPepName(){const q=$('#pName').value;const k=matchPep(q);const box=$('#pRec');if(!box)return;
 if(!k){box.innerHTML='';return;}
 const info=PEPTINFO[k],sug=pepSuggest(k);
 box.innerHTML='<div class="panel" style="background:var(--panel2);display:flex;gap:14px;align-items:center;margin-top:8px"><div style="width:64px;flex:0 0 64px">'+vial(k,0.7,'#16e0a3',info[1])+'</div><div style="flex:1"><b>'+k+'</b> <span class="muted small">'+info[0]+'</span><div class="small" style="margin-top:3px">Suggested schedule: <b>'+sug[1]+'</b></div><div style="margin-top:6px"><button class="b g sm" onclick="applySuggest(\''+k+'\')">Use suggested days</button> <span class="muted small">schedule only — not medical advice</span></div></div></div>';}
function applySuggest(k){pickedDays=new Set(pepSuggest(k)[0]);renderDayPicker();}
function vial(name,fill,glow,tint){fill=Math.max(0,Math.min(1,fill||0));tint=tint||'#8be3c9';glow=glow||'#16e0a3';
 const innerTop=58,liqBottom=190,top=44,level=innerTop+(1-fill)*(liqBottom-innerTop),u=Math.random().toString(36).slice(2,7);
 let fs=13;if(name&&name.length>10)fs=11;if(name&&name.length>13)fs=9.5;
 return '<svg viewBox="0 0 140 220" width="100%" style="max-width:130px"><defs>'+
 '<linearGradient id="g'+u+'" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="#fff" stop-opacity=".26"/><stop offset=".25" stop-color="#fff" stop-opacity=".05"/><stop offset=".7" stop-color="#000" stop-opacity=".10"/><stop offset="1" stop-color="#000" stop-opacity=".22"/></linearGradient>'+
 '<linearGradient id="l'+u+'" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="'+tint+'" stop-opacity=".95"/><stop offset="1" stop-color="'+tint+'" stop-opacity=".7"/></linearGradient>'+
 '<radialGradient id="gl'+u+'" cx=".5" cy=".5" r=".5"><stop offset="0" stop-color="'+glow+'" stop-opacity=".55"/><stop offset="1" stop-color="'+glow+'" stop-opacity="0"/></radialGradient>'+
 '<clipPath id="c'+u+'"><path d="M40 '+innerTop+' h60 v118 a30 30 0 0 1 -60 0 z"/></clipPath></defs>'+
 '<ellipse cx="70" cy="120" rx="66" ry="92" fill="url(#gl'+u+')"/>'+
 '<path d="M40 '+innerTop+' h60 v118 a30 30 0 0 1 -60 0 z" fill="#0e161d" stroke="'+glow+'" stroke-opacity=".5" stroke-width="1.5"/>'+
 '<g clip-path="url(#c'+u+')"><rect x="38" y="'+level+'" width="64" height="'+(liqBottom-level+8)+'" fill="url(#l'+u+')"/><ellipse cx="70" cy="'+level+'" rx="30" ry="4" fill="'+tint+'"/></g>'+
 '<path d="M40 '+innerTop+' h60 v118 a30 30 0 0 1 -60 0 z" fill="url(#g'+u+')"/>'+
 '<rect x="47" y="66" width="7" height="120" rx="3.5" fill="#fff" opacity=".22"/>'+
 '<rect x="52" y="'+top+'" width="36" height="16" fill="#cfd8e0"/><rect x="48" y="'+(top-14)+'" width="44" height="16" rx="3" fill="#aeb8c2"/><rect x="54" y="'+(top-22)+'" width="32" height="10" rx="3" fill="#6b7480"/>'+
 '<rect x="34" y="118" width="72" height="46" rx="5" fill="#f4f7fa" opacity=".95"/><rect x="34" y="118" width="72" height="12" rx="5" fill="'+glow+'" opacity=".85"/>'+
 '<text x="70" y="147" text-anchor="middle" font-family="Inter,Arial" font-weight="800" font-size="'+fs+'" fill="#0b0f14">'+(name||'')+'</text>'+
 '<text x="70" y="159" text-anchor="middle" font-family="Inter,Arial" font-size="7" fill="#5b6b78" letter-spacing="1">WHOOP CIRCLE</text></svg>';}
const vColor=v=>({good:'#16e0a3',bad:'#ff4d5e',neutral:'#ffb020'}[v]||'#ffb020');

async function boot(){const m=await api('/api/me');me=m;
 if(!m.signed_in){$('#signin').classList.remove('hidden');if(!m.has_credentials)$('#credWarn').innerHTML='⚠️ Server has no WHOOP credentials configured yet.';return;}
 $('#app').classList.remove('hidden');$('#sideName').textContent=m.name;$('#jDay').value=new Date().toISOString().slice(0,10);
 renderTagBox();renderDayPicker();pWeek=mondayOf(new Date());initExploreControls();pollSync();loadDashboard();}
function setStatus(t){$('#statusPill').innerHTML=t;}
async function pollSync(){const s=await api('/api/sync/status');const c=s.counts||{};const n=Object.values(c).reduce((a,b)=>a+b,0);
 if(s.running){setStatus('<span class="spin"></span> syncing '+n);setTimeout(pollSync,1600);}else{setStatus('● up to date');refreshActive();}}
async function recentSync(){setStatus('<span class="spin"></span> syncing…');await fetch('/api/sync/recent',{method:'POST'});pollSync();}
async function signOut(){await fetch('/api/logout',{method:'POST'});location.href='/';}
let active='dashboard';
function tab(name){active=name;document.querySelectorAll('#nav button').forEach(b=>b.classList.toggle('active',b.dataset.tab===name));
 document.querySelectorAll('main section').forEach(s=>s.classList.add('hidden'));$('#tab-'+name).classList.remove('hidden');$('#ttl').textContent=TITLES[name];refreshActive();}
function refreshActive(){({dashboard:loadDashboard,vitals:loadVitals,sleep:loadSleep,load:loadLoad,insights:loadInsights,explore:loadExplore,peptides:loadPeptides,journal:loadJournal,circles:loadCircles,settings:loadSettings}[active]||(()=>{}))();}
function ewHtml(ew){const cls={ok:'ban-ok',watch:'ban-watch',alert:'ban-alert'}[ew.level],ic={ok:'✅',watch:'⚠️',alert:'🚨'}[ew.level];
 let h='<div class="banner '+cls+'"><div style="font-size:15px;font-weight:700;margin-bottom:5px">'+ic+' '+ew.level.toUpperCase()+' · '+ew.confidence+'% confidence</div>'+ew.message;
 if(ew.drivers&&ew.drivers.length)h+='<div style="margin-top:8px">'+ew.drivers.map(d=>'<span class="chip">'+d+'</span>').join('')+'</div>';return h+'</div>';}
function vitTile(v){const col=stColor(v.status);return'<div class="panel" style="padding:14px"><div style="display:flex;justify-content:space-between;align-items:flex-start;gap:10px"><div><div class="lbl"><span class="dot d-'+v.status+'"></span>'+v.name+'</div><div class="v">'+v.latest+' <small>'+v.unit+'</small></div></div>'+organImg(v.name,col,64)+'</div><div style="margin-top:6px">'+spark(v.trend,col,150,26)+'</div><div class="muted small" style="margin-top:3px">z '+(v.z>0?'+':'')+v.z+(v.cv!=null?' · cv '+v.cv+'%':'')+'</div></div>';}

/* ---------- DASHBOARD ---------- */
async function loadDashboard(){
 const d=await api('/api/dashboard');const rd=d.readiness,ew=d.early_warning,ld=d.load,sm=d.sleep,ov=d.overview;
 $('#dRing').innerHTML=ring(rd.score,bandColor(rd.band),rd.band||'',160);$('#dRec').textContent=rd.recommendation||'';
 $('#dEw').innerHTML=ewHtml(ew);
 $('#dLoad').innerHTML=ld.enough?'<div class="big" style="color:'+zoneColor(ld.zone)+'">'+ld.acwr+'</div><div class="lbl">ACWR · '+ld.zone+'</div><div class="muted small" style="margin-top:6px">acute '+ld.acute+' / chronic '+ld.chronic+'</div>':'<span class="muted small">Need 2+ weeks.</span>';
 $('#dSleep').innerHTML='<div class="big">'+fmt(sm.avg_hours)+'<small style="font-size:16px" class="muted">h</small></div><div class="lbl">avg sleep</div><div class="muted small" style="margin-top:6px">'+(sm.debt_h!=null?sm.debt_h+'h debt · ':'')+(sm.regularity!=null?'SRI '+sm.regularity:'')+'</div>';
 const L=ov.latest;$('#dLatest').innerHTML='<div class="big">'+fmt(L.recovery_pct)+'<small style="font-size:16px" class="muted">%</small></div><div class="lbl">recovery</div><div class="muted small" style="margin-top:6px">strain '+fmt(L.strain)+' · sleep '+fmt(L.sleep_quality)+'</div>';
 $('#dVitals').innerHTML=d.vitals.map(vitTile).join('');
 const s=d.recovery_series,labels=s.map(p=>p.day);
 mkChart('dChart','line',{labels,datasets:[{label:'Recovery %',data:s.map(p=>p.recovery),borderColor:'#16e0a3',tension:.3,pointRadius:0,yAxisID:'y'},{label:'Strain',data:s.map(p=>p.strain),borderColor:'#ff8a3a',tension:.3,pointRadius:0,yAxisID:'y1'}]},{scales:{y:{position:'left',min:0,max:100},y1:{position:'right',min:0,max:21,grid:{drawOnChartArea:false}}}});
 $('#dNarr').innerHTML=d.narrative;
 $('#dOverall').innerHTML=overallHtml(d.overall);
 $('#dCompare').innerHTML=compareHtml(d.population);}
function overallHtml(o){if(!o||!o.have_data)return '<span class="muted small">Sync your WHOOP to see your summary.</span>';
 const c={good:'#16e0a3',watch:'#ffb020',bad:'#ff4d5e'}[o.color];
 let h='<span class="badge" style="background:'+c+'22;color:'+c+';font-size:14px;font-weight:800">'+o.level+'</span>';
 h+='<div style="font-size:15.5px;line-height:1.6;margin:10px 0">'+o.headline+'</div>';
 if(o.good.length)h+='<div class="small" style="margin-bottom:3px"><span style="color:var(--green)">●</span> Healthy: '+o.good.join(', ')+'</div>';
 if(o.watch.length)h+='<div class="small" style="margin-bottom:3px"><span style="color:var(--amber)">●</span> Keep an eye on: '+o.watch.join(', ')+'</div>';
 if(o.off.length)h+='<div class="small" style="margin-bottom:3px"><span style="color:var(--red)">●</span> Off: '+o.off.join(', ')+'</div>';
 if(o.reasons.length)h+='<div style="margin-top:12px"><div class="lbl">If something\'s off, common reasons</div>'+o.reasons.map(r=>'<div class="small" style="margin-top:5px"><b>'+r.name+':</b> <span class="muted">'+r.reasons.join(' · ')+'</span></div>').join('')+'</div>';
 return h+'<div class="muted small" style="margin-top:12px">'+o.disclaimer+'</div>';}
function summaryHtml(s){if(!s||!s.enough)return '<span class="muted small">Need at least a week of data.</span>';
 return '<div style="font-size:15.5px;line-height:1.6;margin-bottom:12px">'+s.narrative+'</div><div class="grid cards">'+s.rows.map(r=>{const dc=r.direction==='improving'?'#16e0a3':r.direction==='declining'?'#ff4d5e':'#7d8b9a';const ar=r.direction==='improving'?'▲':r.direction==='declining'?'▼':'—';
  return '<div class="panel" style="padding:12px"><div class="lbl">'+r.metric+'</div><div class="v" style="font-size:22px">'+r.avg7+(r.unit?' '+r.unit:'')+'</div><div class="small muted">last 7 days</div><div class="small" style="margin-top:4px">30-day avg '+r.avg30+(r.unit?' '+r.unit:'')+' · <span style="color:'+dc+'">'+ar+' '+r.direction+'</span></div></div>';}).join('')+'</div>';}
const RISKMAP={ok:['#16e0a3','✓ Healthy — no concern'],watch:['#ffb020','⚠ Keep an eye on it'],concern:['#ff4d5e','⚑ Pay attention to this']};
function organAsset(name){const n=(name||'').toLowerCase();
 if(n.includes('hrv'))return'hrv';
 if(n.includes('resting')||n.includes('heart'))return'heart';
 if(n.includes('resp')||n.includes('breath'))return'lungs';
 if(n.includes('spo')||n.includes('oxygen')||n.includes('blood'))return'blood';
 if(n.includes('temp'))return'temp';
 if(n.includes('regularit'))return'clock';
 if(n.includes('debt'))return'hourglass';
 if(n.includes('duration')||n.includes('sleep'))return'moon';
 return null;}
function organImg(name,color,size){const a=organAsset(name);if(!a)return'';size=size||64;const c=color||'#00e5a0';
 return '<img src="/asset/'+a+'.jpg" alt="" loading="lazy" style="width:'+size+'px;height:'+size+'px;border-radius:14px;object-fit:cover;flex:0 0 auto;box-shadow:0 0 0 1px '+c+'66,0 0 22px '+c+'40">';}
function trioHtml(r){const u=r.unit?' '+r.unit:'';const cells=[['Today',r.d1],['7-day avg',r.d7],['30-day avg',r.d30]];
 return '<div style="display:flex;gap:6px;margin:8px 0">'+cells.map(c=>'<div style="flex:1;background:var(--bg);border:1px solid var(--line);border-radius:8px;padding:6px 2px;text-align:center"><div style="font-size:16px;font-weight:800">'+(c[1]==null?'—':c[1]+u)+'</div><div style="font-size:9px;letter-spacing:.4px;text-transform:uppercase;color:var(--muted);margin-top:1px">'+c[0]+'</div></div>').join('')+'</div>';}
function riskBox(r){const rk=RISKMAP[r.risk]||RISKMAP.ok;const isok=(r.risk||'ok')==='ok';
 return '<div style="margin-top:10px;border-top:1px solid var(--line);padding-top:9px;background:'+rk[0]+(isok?'14':'10')+';border-radius:9px;padding:9px;border:1px solid '+rk[0]+'33">'+
  '<span class="badge" style="background:'+rk[0]+'2e;color:'+rk[0]+';font-size:13px;font-weight:800;padding:3px 12px">'+rk[1]+'</span>'+
  (r.risk_note?'<div class="small" style="margin-top:7px">'+r.risk_note+'</div>':'')+
  (r.why?'<div class="muted small" style="margin-top:5px"><b>Why it matters:</b> '+r.why+'</div>':'')+
  (r.causes?'<div class="muted small" style="margin-top:3px"><b>Common causes:</b> '+r.causes+'</div>':'')+'</div>';}
function compareHtml(pop){if(!pop||!pop.have_profile)return '<span class="muted small">Set your <b>age &amp; sex</b> in Settings to see how you stack up against the average person your age.</span>';
 return '<div class="muted small" style="margin-bottom:10px">'+pop.note+' Each card shows today, your 7-day and 30-day averages.</div><div class="grid g2">'+pop.rows.map(r=>{const col=r.status==='better'?'#16e0a3':r.status==='below'?'#ff4d5e':'#ffb020';const mx=(Math.max(r.you,r.avg)*1.3)||1;
  return '<div class="panel" style="background:var(--panel2);padding:14px"><div style="display:flex;justify-content:space-between;align-items:center;gap:10px"><div style="display:flex;align-items:center;gap:13px">'+organImg(r.metric,col,64)+'<b>'+r.metric+'</b></div><span class="badge" style="background:'+col+'22;color:'+col+'">'+r.status+'</span></div>'+
   trioHtml(r)+
   '<div class="small" style="margin:6px 0">'+r.plain+'</div>'+
   '<div style="font-size:11px;color:var(--muted)">You (7-day) — '+r.you+' '+r.unit+'</div><div class="pctbar"><div class="pctfill" style="width:'+Math.min(100,r.you/mx*100)+'%;background:'+col+'"></div></div>'+
   '<div style="font-size:11px;color:var(--muted);margin-top:4px">Average — '+r.avg+' '+r.unit+'</div><div class="pctbar"><div class="pctfill" style="width:'+Math.min(100,r.avg/mx*100)+'%;background:#3a4654"></div></div>'+
   riskBox(r)+'</div>';}).join('')+'</div>';}

/* ---------- VITALS ---------- */
async function loadVitals(){const[vp,ew,pop]=await Promise.all([api('/api/health/vitals'),api('/api/health/early-warning'),api('/api/health/population')]);
 $('#vEw').innerHTML='<h3>Early-warning</h3>'+ewHtml(ew);
 $('#vCompare').innerHTML=compareHtml(pop);
 const RLBL={good:'✓ Healthy',watch:'⚠ Keep an eye on it',off:'⚑ Pay attention to this'};
 $('#vFull').innerHTML=vp.vitals.map((v,i)=>{const rc={good:'#16e0a3',watch:'#ffb020',off:'#ff4d5e'}[v.rating]||'#7d8b9a';const rl=RLBL[v.rating]||v.rating;
  return '<div class="panel"><div style="display:flex;justify-content:space-between;align-items:baseline"><div style="display:flex;gap:14px;align-items:center">'+organImg(v.name,rc,92)+'<div><div class="lbl"><span class="dot d-'+v.status+'"></span>'+v.name+'</div><div class="v" style="font-size:32px">'+v.latest+' <small style="font-size:14px">'+v.unit+'</small></div></div></div><div style="text-align:right">'+spark(v.trend,stColor(v.status),150,36)+'<div class="muted small">30-day</div></div></div>'+refbar(v.low,v.high,v.latest)+'<div style="margin-top:8px;background:'+rc+'12;border:1px solid '+rc+'33;border-radius:9px;padding:8px"><span class="badge" style="background:'+rc+'2e;color:'+rc+';font-size:13px;font-weight:800;padding:3px 12px">'+rl+'</span> <span class="small">'+v.plain+'</span></div><div class="muted small" style="margin-top:6px">your range '+v.low+'–'+v.high+' '+v.unit+' · z '+(v.z>0?'+':'')+v.z+(v.cv!=null?' · var '+v.cv+'%':'')+'</div></div>';}).join('');}

/* ---------- SLEEP ---------- */
async function loadSleep(){const[sm,sr,cir]=await Promise.all([api('/api/health/sleepmed'),api('/api/sleep'),api('/api/circadian')]);
 if(sm.enough){
  const pl=sm.plains||{},vc={good:'#16e0a3',watch:'#ffb020',bad:'#ff4d5e',neutral:'#7d8b9a'};
  const rateChip=p=>p?'<span class="badge" style="background:'+vc[p.color]+'22;color:'+vc[p.color]+'">'+p.label+'</span>':'';
  const rateNote=p=>p?'<div class="muted small" style="margin-top:5px">'+p.text+'</div>':'';
  $('#sSri').innerHTML=ring(sm.regularity,sm.regularity>=85?'var(--green)':sm.regularity>=70?'var(--amber)':'var(--red)','SRI',130)+'<div style="margin-top:6px">'+rateChip(pl.regularity)+'</div>'+rateNote(pl.regularity);
  $('#sDebt').innerHTML='<div class="big" style="color:'+(sm.debt_h>=6?'var(--red)':sm.debt_h>=3?'var(--amber)':'var(--green)')+'">'+fmt(sm.debt_h)+'<small style="font-size:18px" class="muted">h</small></div><div class="lbl">last 7 nights</div><div style="margin-top:5px">'+rateChip(pl.debt)+'</div>'+rateNote(pl.debt);
  $('#sDur').innerHTML='<div class="big">'+fmt(sm.avg_hours)+'<small style="font-size:18px" class="muted">h</small></div><div class="lbl">per night</div><div style="margin-top:5px">'+rateChip(pl.duration)+'</div>'+rateNote(pl.duration);
  $('#sJet').innerHTML='<div class="big">'+fmt(cir.social_jetlag_h)+'<small style="font-size:18px" class="muted">h</small></div><div class="lbl">weekend shift</div><div class="muted small" style="margin-top:5px">difference in your sleep timing on weekends vs weekdays. Under 1h is good.</div>';
  const srItems=[['Sleep duration',pl.duration],['Sleep regularity',pl.regularity],['Sleep debt',pl.debt]];
  if(sm.breathing&&(sm.breathing.risk!=='ok'||sm.spo2!=null))srItems.push(['Breathing / blood oxygen',{risk:sm.breathing.risk,danger:sm.breathing.note,why:sm.breathing.why,causes:sm.breathing.causes,trio:sm.breathing.trio}]);
  $('#sRisk').innerHTML='<div class="grid g2">'+srItems.map(([name,p])=>{if(!p)return'';
   const rc2={ok:'#16e0a3',watch:'#ffb020',concern:'#ff4d5e'}[p.risk||'ok'];
   return '<div class="panel" style="background:var(--panel2);padding:14px"><div style="display:flex;align-items:center;gap:13px">'+organImg(name,rc2,64)+'<b>'+name+'</b></div>'+(p.trio?trioHtml(p.trio):'')+riskBox({risk:p.risk||'ok',risk_note:p.danger,why:p.why,causes:p.causes})+'</div>';}).join('')+'</div><div class="muted small" style="margin-top:8px">Plain-language read of your data — not a medical diagnosis.</div>';
  const stHex={optimal:'#16e0a3',watch:'#ffb020',flag:'#ff4d5e'};const a=sm.architecture;mkChart('sArch','bar',{labels:a.map(x=>x.stage),datasets:[{label:'You %',data:a.map(x=>x.pct),backgroundColor:a.map(x=>stHex[x.status]||'#7d8b9a')},{label:'Norm mid',data:a.map(x=>({Light:50,Deep:15,REM:22}[x.stage])),type:'line',borderColor:'#8b97a4',borderDash:[5,4],pointRadius:0}]},{scales:{y:{min:0,max:70}}});
  $('#sArchNote').innerHTML='<div class="muted small" style="margin-bottom:4px">How your night splits into sleep stages vs the clinical healthy range. Deep = physical recovery, REM = mental recovery.</div>'+a.map(x=>'<div class="small"><span class="dot d-'+x.status+'"></span><b>'+x.stage+'</b> '+x.pct+'% <span class="muted">(healthy '+x.norm+') — '+x.plain+'</span></div>').join('');
  mkChart('sStage','doughnut',{labels:['Light','REM','Deep'],datasets:[{data:[a.find(x=>x.stage=='Light').pct,a.find(x=>x.stage=='REM').pct,a.find(x=>x.stage=='Deep').pct],backgroundColor:['#38bdf8','#a78bfa','#00e5a0']}]},{plugins:{legend:{position:'bottom'}}});
  // heatmap
  const pts=cir.points.slice(-30);
  $('#sHeat').innerHTML='<div class="hm">'+pts.map(p=>{let a1=(p.onset-1080+1440)%1440,a2=(p.wake-1080+1440)%1440;let L=a1/1440*100,W=((a2-a1+1440)%1440)/1440*100;return'<div class="hmrow"><span style="width:64px">'+p.day.slice(5)+'</span><div class="hmtrack"><div class="hmbar" style="left:'+L+'%;width:'+W+'%"></div></div></div>';}).join('')+'</div>';
 } else {$('#sSri').innerHTML='<span class="muted">Need more sleep data.</span>';}
 const labels=sr.series.map(p=>p.day);
 mkChart('sTrend','line',{labels,datasets:[{label:'Quality',data:sr.series.map(p=>p.quality),borderColor:'#38bdf8',backgroundColor:'rgba(56,189,248,.12)',tension:.3,pointRadius:0,fill:true},{label:'7-night avg',data:sr.series.map(p=>p.rolling7),borderColor:'#00e5a0',borderWidth:2,pointRadius:0,tension:.3}]},{scales:{y:{min:0,max:100}}});}

/* ---------- LOAD ---------- */
async function loadLoad(){const[ld,rc,wo]=await Promise.all([api('/api/health/load'),api('/api/recovery'),api('/api/workouts')]);
 if(ld.enough){
  $('#lAcwr').innerHTML=ring(Math.min(100,Math.round(ld.acwr/2*100)),zoneColor(ld.zone),ld.zone,130)+'<div class="muted small" style="margin-top:6px">'+ld.message+'<br><span style="opacity:.8">Compares this week\'s training to your recent norm. 0.8–1.3 is the safe sweet spot.</span></div>';
  $('#lAcute').innerHTML='<div class="big">'+ld.acute+'</div><div class="lbl">7d EWMA strain</div>';
  $('#lChronic').innerHTML='<div class="big">'+ld.chronic+'</div><div class="lbl">28d EWMA strain</div>';
  const labels=ld.series.map(p=>p.day);
  mkChart('lChart','line',{labels,datasets:[{type:'bar',label:'Daily strain',data:ld.series.map(p=>p.strain),backgroundColor:'rgba(56,189,248,.35)',yAxisID:'y'},{label:'ACWR',data:ld.series.map(p=>p.acwr),borderColor:'#ffb020',borderWidth:2,pointRadius:0,tension:.3,yAxisID:'y1'}]},{scales:{y:{position:'left',min:0,max:21},y1:{position:'right',min:0,max:2.5,grid:{drawOnChartArea:false}}}});
 } else {$('#lAcwr').innerHTML='<span class="muted small">Need 2+ weeks.</span>';}
 $('#lMono').innerHTML=wo.monotony!=null?'<div class="big" style="color:'+(wo.monotony>2?'var(--red)':'var(--green)')+'">'+wo.monotony+'</div><div class="lbl">weekly strain '+fmt(wo.weekly_strain)+'</div><div class="muted small" style="margin-top:5px">'+wo.monotony_note+'</div>':'<span class="muted small">Need recent data.</span>';
 const s=rc.series.slice(-90),labels=s.map(p=>p.day);
 mkChart('lRec','line',{labels,datasets:[{label:'Recovery %',data:s.map(p=>p.recovery),borderColor:'#16e0a3',tension:.3,pointRadius:0,yAxisID:'y'},{label:'Strain',data:s.map(p=>p.strain),borderColor:'#ff8a3a',tension:.3,pointRadius:0,yAxisID:'y1'}]},{scales:{y:{position:'left',min:0,max:100},y1:{position:'right',min:0,max:21,grid:{drawOnChartArea:false}}}});
 if(wo.has_zones)mkChart('lZones','polarArea',{labels:['Z0','Z1','Z2','Z3','Z4','Z5'],datasets:[{data:wo.zone_pct,backgroundColor:['#334155','#38bdf8','#00e5a0','#ffb020','#ff8a3a','#ff4d5e']}]},{plugins:{legend:{position:'right'}}});
 else $('#lZones').parentElement.querySelector('canvas').replaceWith(Object.assign(document.createElement('div'),{className:'muted small',textContent:'No HR-zone data.'}));
 mkChart('lSports','bar',{labels:wo.sports.map(s=>s.sport),datasets:[{label:'Sessions',data:wo.sports.map(s=>s.count),backgroundColor:'#38bdf8'},{label:'Avg strain',data:wo.sports.map(s=>s.avg_strain),backgroundColor:'#00e5a0'}]});}

/* ---------- INSIGHTS ---------- */
async function loadInsights(){const[dr,fc,wy,tr,an,su]=await Promise.all([api('/api/insights/drivers'),api('/api/insights/forecast'),api('/api/insights/why'),api('/api/insights/trends'),api('/api/insights/anomalies'),api('/api/insights/summary')]);
 $('#iSummary').innerHTML=summaryHtml(su);
 const d=dr.drivers.filter(x=>x.r!=null).slice(0,7);
 mkChart('iDrivers','bar',{labels:d.map(x=>x.factor),datasets:[{label:'Correlation with recovery',data:d.map(x=>x.r),backgroundColor:d.map(x=>x.r>0?'#00e5a0':'#ff4d5e')}]},{indexAxis:'y',scales:{x:{min:-1,max:1}}});
 if(fc.has_data){$('#iForecast').innerHTML='<div style="display:flex;gap:20px;align-items:center;flex-wrap:wrap"><div style="text-align:center">'+ring(fc.predicted_recovery,bandColor(fc.predicted_recovery>=67?'prime':fc.predicted_recovery>=34?'moderate':'recover'),'pred',130)+'<div class="muted small">likely '+fc.range[0]+'–'+fc.range[1]+'%</div></div><div style="flex:1"><div class="lbl">Contributors</div>'+fc.contributors.map(c=>'<div style="margin:4px 0">'+c.factor+' <b class="'+(c.effect>0?'pos':'neg')+'">'+(c.effect>0?'+':'')+c.effect+'</b></div>').join('')+'<div class="muted small" style="margin-top:8px">Illness risk: '+fc.illness_risk+' ('+fc.illness_conf+'%)</div></div></div><div class="muted small" style="margin-top:8px">'+fc.note+'</div>';}
 else $('#iForecast').innerHTML='<span class="muted">Need more history to forecast.</span>';
 if(wy.has_data){$('#iWhy').innerHTML='<div class="muted small" style="margin-bottom:8px">'+wy.day+' · recovery '+fmt(wy.recovery)+'%'+(wy.habits.length?' · logged: '+wy.habits.join(', '):'')+'</div>'+(wy.reasons.length?wy.reasons.map(r=>'<div class="insight" style="border-left-color:'+(r.impact=='helped'?'var(--green)':r.impact=='hurt'?'var(--red)':'var(--accent2)')+'">'+r.factor+' was <b>'+r.note+'</b> ('+(r.z>0?'+':'')+r.z+' SD) — '+r.impact+'</div>').join(''):'<span class="muted">Everything near your baseline today.</span>');}
 else $('#iWhy').innerHTML='<span class="muted">Need more data.</span>';
 $('#iTrends').innerHTML=tr.metrics.map(m=>{const good=m.higher_better;const wc=m.wow==null?'':(m.wow>0)===(good!==false)?'pos':'neg';const mc=m.mom==null?'':(m.mom>0)===(good!==false)?'pos':'neg';
  const pv=m.percentile,pw=pv==null?'':(pv>=66?'in the top third of your usual':pv>=33?'about your usual':'in the low third of your usual');
  return'<div class="panel" style="padding:14px"><div class="lbl">'+m.metric+'</div><div class="v" style="font-size:24px">'+m.latest+'</div><div style="margin:4px 0">'+spark(m.spark,'#38bdf8',150,26)+'</div><div class="small muted">7d '+m.avg7+' · 30d '+m.avg30+'</div><div class="small">WoW <b class="'+wc+'">'+(m.wow>0?'+':'')+fmt(m.wow)+'</b> · MoM <b class="'+mc+'">'+(m.mom>0?'+':'')+fmt(m.mom)+'</b></div>'+pctbar(m.percentile)+'<div class="small muted" style="margin-top:4px">Today is '+pw+' (percentile '+m.percentile+').</div></div>';}).join('');
 $('#iAnom').innerHTML=an.events.length?'<div class="tl">'+an.events.map(e=>'<div class="tlrow"><b>'+e.day+'</b> — '+e.metric+' unusually <b class="'+(e.direction=='high'?'pos':'neg')+'">'+e.direction+'</b> ('+(e.z>0?'+':'')+e.z+' SD, '+e.value+')</div>').join('')+'</div>':'<span class="muted">No anomalies detected.</span>';}

/* ---------- EXPLORE ---------- */
function initExploreControls(){}
async function loadExplore(){const[ex,gl]=await Promise.all([api('/api/explore'),api('/api/goals')]);exploreRows=ex.rows;
 const opts=Object.entries(ex.labels).map(([k,l])=>'<option value="'+k+'">'+l+'</option>').join('');
 if(!$('#eA').options.length){$('#eA').innerHTML=opts;$('#eB').innerHTML=opts;$('#eA').value='recovery';$('#eB').value='hrv';$('#eA').onchange=drawExplore;$('#eB').onchange=drawExplore;$('#eDays').onchange=drawExplore;
  $('#gMetric').innerHTML=Object.entries(gl.available).map(([k,l])=>'<option value="'+k+'">'+l+'</option>').join('');}
 drawExplore();renderGoals(gl);}
function drawExplore(){if(!exploreRows)return;const a=$('#eA').value,b=$('#eB').value,n=+$('#eDays').value;const rows=exploreRows.slice(-n);
 mkChart('eChart','line',{labels:rows.map(r=>r.day),datasets:[{label:$('#eA').selectedOptions[0].text,data:rows.map(r=>r[a]),borderColor:'#00e5a0',tension:.3,pointRadius:0,yAxisID:'y'},{label:$('#eB').selectedOptions[0].text,data:rows.map(r=>r[b]),borderColor:'#a78bfa',tension:.3,pointRadius:0,yAxisID:'y1'}]},{scales:{y:{position:'left'},y1:{position:'right',grid:{drawOnChartArea:false}}}});}
function renderGoals(gl){$('#gList').innerHTML=gl.goals.length?gl.goals.map(g=>'<div style="display:flex;align-items:center;gap:12px;margin-bottom:10px"><div style="width:80px">'+ring(g.adherence,g.adherence>=70?'var(--green)':g.adherence>=40?'var(--amber)':'var(--red)','',70)+'</div><div style="flex:1"><b>'+g.label+'</b> '+(g.direction=='min'?'≥':'≤')+' '+g.target+'<div class="muted small">recent avg '+fmt(g.recent)+' · '+g.adherence+'% of days</div></div><button class="b red sm" onclick="delGoal(\''+g.metric+'\')">✕</button></div>').join(''):'<span class="muted small">No goals yet — add one below.</span>';}
async function saveGoal(){await fetch('/api/goals',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({metric:$('#gMetric').value,target:+$('#gTarget').value,direction:$('#gDir').value})});loadExplore();}
async function delGoal(m){await fetch('/api/goals/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({metric:m})});loadExplore();}
async function runCompare(){const q='a_from='+$('#cAf').value+'&a_to='+$('#cAt').value+'&b_from='+$('#cBf').value+'&b_to='+$('#cBt').value;const d=await api('/api/compare?'+q);
 const keys=Object.keys(d.deltas);mkChart('cChart','bar',{labels:keys.map(k=>d.labels[k]),datasets:[{label:'A',data:keys.map(k=>d.a[k]),backgroundColor:'#38bdf8'},{label:'B',data:keys.map(k=>d.b[k]),backgroundColor:'#00e5a0'}]});}

/* ---------- PEPTIDES ---------- */
let pickedDays=new Set([0,1,2,3,4,5,6]);
function renderDayPicker(){$('#pDays').innerHTML=DAYS.map((d,i)=>'<span class="tag '+(pickedDays.has(i)?'on':'')+'" onclick="tglDay('+i+')">'+d+'</span>').join('');}
function tglDay(i){pickedDays.has(i)?pickedDays.delete(i):pickedDays.add(i);renderDayPicker();}
function mondayOf(d){const x=new Date(d);const day=(x.getDay()+6)%7;x.setDate(x.getDate()-day);return x.toISOString().slice(0,10);}
function dateOfWeek(i){const d=new Date(pWeek);d.setDate(d.getDate()+i);return d.toISOString().slice(0,10);}
function shiftWeek(n){const d=new Date(pWeek);d.setDate(d.getDate()+n*7);pWeek=mondayOf(d);loadPeptides();}
async function addPeptide(){const name=$('#pName').value.trim();if(!name){alert('Name?');return;}await fetch('/api/peptides',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,dose:$('#pDose').value.trim(),days:[...pickedDays].sort()})});$('#pName').value='';$('#pDose').value='';$('#pRec').innerHTML='';loadPeptides();}
async function delPeptide(pid){if(!confirm('Delete this peptide and its history?'))return;await fetch('/api/peptides/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({peptide_id:pid})});loadPeptides();}
async function togglePep(pid,ds,cur){await fetch('/api/peptides/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({peptide_id:pid,day:ds,taken:!cur})});loadPeptides();}
async function saveNote(pid){await fetch('/api/peptides/note',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({peptide_id:pid,week_start:pWeek,note:document.getElementById('note-'+pid).value})});const m=document.getElementById('nmsg-'+pid);if(m){m.textContent='saved ✓';setTimeout(()=>m.textContent='',1200);}}
async function loadPeptides(){const[d,out,corr]=await Promise.all([api('/api/peptides?week='+pWeek),api('/api/health/peptide-outcomes'),api('/api/peptides/correlations')]);
 $('#pWeekLabel').textContent=pWeek;
 const vmap={};(corr.peptides||[]).forEach(p=>{if(p.vcolor)vmap[p.name]={color:vColor(p.vcolor),verdict:p.verdict};});
 // adaptive "my peptides" vials
 $('#pVials').innerHTML=d.peptides.length?d.peptides.map(p=>{const logs=d.logs[p.peptide_id]||{};let taken=0;p.days.forEach(i=>{if(logs[dateOfWeek(i)])taken++;});const adh=p.days.length?taken/p.days.length:0;const v=vmap[p.name]||{color:'#7d8b9a',verdict:'need data'};
  return '<div class="panel" style="padding:12px;text-align:center">'+vial(p.name,adh,v.color,pepTint(p.name))+'<div class="nm" style="font-weight:700;font-size:13px;margin-top:4px">'+p.name+'</div><div class="muted small">'+Math.round(adh*100)+'% this wk</div><span class="badge" style="background:'+v.color+'22;color:'+v.color+';margin-top:4px">'+v.verdict+'</span></div>';}).join(''):'<span class="muted small">Add a peptide to see its adaptive vial.</span>';
 // full library
 $('#pLib').innerHTML=Object.entries(PEPTINFO).map(([n,i])=>'<div class="panel" style="padding:12px;text-align:center">'+vial(n,0.7,'#16e0a3',i[1])+'<div style="font-weight:700;font-size:13px;margin-top:4px">'+n+'</div><div class="muted small">'+i[0]+'</div></div>').join('');
 if(!d.peptides.length){$('#pGrid').innerHTML='<span class="muted">No peptides yet — add one above.</span>';}
 else{let h='<table><tr><th>Peptide</th>'+DAYS.map((x,i)=>'<th>'+x+'<br><span class="muted" style="font-weight:400">'+dateOfWeek(i).slice(5)+'</span></th>').join('')+'<th>Wk</th></tr>';
 for(const p of d.peptides){const logs=d.logs[p.peptide_id]||{};let taken=0,sched=p.days.length;
  h+='<tr><td><b>'+p.name+'</b>'+(p.dose?' <span class="muted small">'+p.dose+'</span>':'')+'<br><button class="b red sm" style="margin-top:4px" onclick="delPeptide(\''+p.peptide_id+'\')">del</button></td>';
  for(let i=0;i<7;i++){const ds=dateOfWeek(i);if(p.days.includes(i)){const on=!!logs[ds];if(on)taken++;h+='<td><span class="pbox '+(on?'on':'')+'" onclick="togglePep(\''+p.peptide_id+'\',\''+ds+'\','+(on?'true':'false')+')">'+(on?'✓':'')+'</span></td>';}else h+='<td><span class="pbox off"></span></td>';}
  h+='<td class="'+(taken>=sched?'pos':'')+'">'+taken+'/'+sched+'</td></tr>';
  h+='<tr><td colspan="9" style="border:0"><label>Notes — is '+p.name+' working?</label><div class="row"><div><textarea id="note-'+p.peptide_id+'" rows="1">'+(d.notes[p.peptide_id]||'')+'</textarea></div><div style="flex:0"><button class="b g sm" onclick="saveNote(\''+p.peptide_id+'\')">Save</button> <span class="muted small" id="nmsg-'+p.peptide_id+'"></span></div></div></td></tr>';}
 $('#pGrid').innerHTML=h+'</table>';}
 $('#pOut').innerHTML=out.peptides.length?out.peptides.map(p=>p.status?'<div class="muted small">'+p.name+' — '+p.status+'</div>':'<div class="panel" style="background:var(--panel2);margin-bottom:12px"><div style="display:flex;justify-content:space-between"><b>'+p.name+(p.dose?' · '+p.dose:'')+'</b><span class="muted small">since '+p.start+' · '+p.confidence+' confidence</span></div><table style="margin-top:8px"><tr><th>Metric</th><th>Before</th><th>During</th><th>Δ</th></tr>'+p.metrics.map(m=>'<tr><td>'+m.label+'</td><td>'+fmt(m.before)+'</td><td>'+fmt(m.during)+'</td><td class="'+(m.delta>0?'pos':m.delta<0?'neg':'')+'">'+(m.delta==null?'—':(m.delta>0?'+':'')+m.delta)+'</td></tr>').join('')+'</table>'+(p.note?'<div class="muted small" style="margin-top:6px">📝 '+p.note+'</div>':'')+'</div>').join(''):'<span class="muted">Log doses to see analysis.</span>';
 $('#pCorrNote').textContent=corr.disclaimer;
 $('#pCorr').innerHTML=corr.peptides.length?corr.peptides.map(p=>p.status?'<div class="muted small">'+p.name+' — '+p.status+'</div>':'<div class="panel" style="background:var(--panel2);margin-bottom:12px"><div style="display:flex;justify-content:space-between;align-items:center"><b>'+p.name+' <span class="badge" style="background:'+vColor(p.vcolor)+'22;color:'+vColor(p.vcolor)+'">'+(p.vcolor==="good"?"✅ ":p.vcolor==="bad"?"⚠️ ":"➖ ")+p.verdict+'</span></b><span class="muted small">'+p.doses+' doses · adherence↔recovery r='+fmt(p.adherence_corr)+'</span></div><table style="margin-top:8px"><tr><th>Metric</th><th>On days</th><th>Off days</th><th>Δ</th><th>Effect</th></tr>'+p.metrics.map(m=>'<tr><td>'+m.label+'</td><td>'+m.on+'</td><td>'+m.off+'</td><td class="'+(m.delta>0?'pos':m.delta<0?'neg':'')+'">'+(m.delta>0?'+':'')+m.delta+'</td><td>'+(Math.abs(m.effect)>=.5?'<b>':'')+m.effect+(Math.abs(m.effect)>=.5?'</b>':'')+'</td></tr>').join('')+'</table></div>').join(''):'<span class="muted">Add peptides &amp; log doses to see correlations.</span>';}

/* ---------- JOURNAL ---------- */
const PRESET=['alcohol','caffeine_late','late_meal','screen_in_bed','workout','stress','travel','poor_diet','meditation','early_bedtime','hydrated','cold_plunge','social','late_night'];
let chosen=new Set(),customT=[];
function renderTagBox(){const all=[...PRESET,...customT];$('#tagBox').innerHTML=all.map(t=>'<span class="tag '+(chosen.has(t)?'on':'')+'" onclick="tglTag(\''+t+'\')">'+t+'</span>').join('');}
function tglTag(t){chosen.has(t)?chosen.delete(t):chosen.add(t);renderTagBox();}
document.addEventListener('keydown',e=>{if(e.target.id==='jCustom'&&e.key==='Enter'&&e.target.value.trim()){const t=e.target.value.trim().toLowerCase().replace(/\s+/g,'_');if(!customT.includes(t)&&!PRESET.includes(t))customT.push(t);chosen.add(t);e.target.value='';renderTagBox();}});
async function saveJournal(){const day=$('#jDay').value;if(!day){$('#jMsg').textContent='Pick a date.';return;}await fetch('/api/journal',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({day,mood:$('#jMood').value?+$('#jMood').value:null,notes:$('#jNotes').value,tags:[...chosen]})});$('#jMsg').textContent='Saved ✓';chosen.clear();$('#jNotes').value='';$('#jMood').value='';renderTagBox();loadJournal();setTimeout(()=>$('#jMsg').textContent='',1500);}
async function loadJournal(){const[j,d]=await Promise.all([api('/api/journal'),api('/api/decisions')]);
 $('#decBaseline').innerHTML='Baseline — recovery <b>'+fmt(d.baseline.recovery)+'%</b>, HRV <b>'+fmt(d.baseline.hrv)+' ms</b>, sleep <b>'+fmt(d.baseline.sleep_quality)+'</b>. ('+d.journal_days+' days)';
 $('#decTable').innerHTML=d.has_data?'<table><tr><th>Habit</th><th>Verdict</th><th>Recovery Δ</th><th>HRV Δ</th><th>Sleep Δ</th><th>n</th></tr>'+d.ratings.map(r=>'<tr><td>'+r.tag+'</td><td class="'+(r.rating==='good call'?'pos':r.rating==='costly'?'neg':'muted')+'">'+r.emoji+' '+r.rating+'</td><td class="'+dc(r.recovery_delta)+'">'+dl(r.recovery_delta)+'</td><td class="'+dc(r.hrv_delta)+'">'+dl(r.hrv_delta)+'</td><td class="'+dc(r.sleep_delta)+'">'+dl(r.sleep_delta)+'</td><td>'+r.n+'</td></tr>').join('')+'</table>':'<span class="muted">Log days with tags to see impact.</span>';
 $('#jList').innerHTML=j.length?'<table><tr><th>Day</th><th>Mood</th><th>Tags</th><th>Notes</th></tr>'+j.map(e=>'<tr><td>'+e.day+'</td><td>'+fmt(e.mood)+'</td><td>'+(e.tags||[]).map(t=>'<span class="tag">'+t+'</span>').join('')+'</td><td class="muted">'+(e.notes||'')+'</td></tr>').join('')+'</table>':'<span class="muted">No entries yet.</span>';}
const dl=v=>v==null?'—':(v>0?'+':'')+v;const dc=v=>v==null?'':(v>0?'pos':v<0?'neg':'');

/* ---------- CIRCLES ---------- */
async function loadCircles(){curCircle=null;$('#circleDetail').classList.add('hidden');$('#circlesList').classList.remove('hidden');const cs=await api('/api/circles');
 $('#circleCards').innerHTML=cs.length?cs.map(c=>'<div class="panel ccard" onclick="openCircle(\''+c.circle_id+'\')"><div class="lbl">'+(c.is_owner?'Owner':'Member')+(c.pending?' · <span class="pos">'+c.pending+' pending</span>':'')+'</div><div style="font-size:19px;font-weight:800;margin:4px 0">'+c.name+'</div><div class="muted small">'+c.members+' member'+(c.members===1?'':'s')+'</div></div>').join(''):'<span class="muted">No circles yet.</span>';}
async function createCircle(){const name=prompt('Name this circle (e.g. Gym crew):');if(!name)return;const r=await api('/api/circles',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name})});alert('Circle created! Invite code: '+r.invite_code+'\nShare it only with people you want in this circle.');loadCircles();}
async function joinCircle(){const code=prompt('Enter the invite code:');if(!code)return;const r=await api('/api/circles/join',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({code})});if(r.status==='pending')alert('Request sent! The owner has to approve you.');else if(r.status==='active')alert('You are already in this circle.');else alert(r.detail||'Could not join.');loadCircles();}
async function openCircle(cid){const d=await api('/api/circle?id='+cid);curCircle=cid;$('#circlesList').classList.add('hidden');const el=$('#circleDetail');el.classList.remove('hidden');const c=d.circle;
 let h='<div class="panel"><button class="b g sm" onclick="loadCircles()">‹ All circles</button> <button class="b red sm" style="float:right" onclick="leaveCircle(\''+cid+'\','+c.is_owner+')">'+(c.is_owner?'Delete circle':'Leave')+'</button><h3 style="margin-top:10px">'+c.name+'</h3>';
 if(c.is_owner)h+='<div class="small">Invite code: <span class="code">'+c.invite_code+'</span> <button class="b g sm" onclick="copyCode(\''+c.invite_code+'\')">copy</button> <button class="b g sm" onclick="revoke(\''+cid+'\')">new code</button></div>';
 h+='</div>';
 if(d.pending.length)h+='<div class="panel"><h3>Pending approvals</h3>'+d.pending.map(p=>'<div class="toggle"><span class="chip">'+p.display_name+'</span><button class="b p sm" onclick="approve(\''+cid+'\','+p.user_id+')">Approve</button><button class="b red sm" onclick="decline(\''+cid+'\','+p.user_id+')">Decline</button></div>').join('')+'</div>';
 const s=d.my_sharing,SH=[['share_recovery','Recovery'],['share_sleep','Sleep'],['share_strain','Strain'],['share_hrv','HRV'],['share_peptides','Peptides']];
 h+='<div class="panel"><h3>What you share here</h3>'+SH.map(([k,l])=>'<label class="toggle"><input type="checkbox" '+(s[k]?'checked':'')+' onchange="setShare(\''+cid+'\',\''+k+'\',this.checked)"> '+l+'</label>').join('')+'<span class="muted small">Peptides off by default. Applies only to this circle.</span></div>';
 h+='<div class="panel"><h3>Members</h3>'+d.members.map(m=>'<span class="chip">'+m.name+(m.me?' (you)':'')+'</span>').join('')+'</div>';
 h+='<div class="panel"><h3>Leaderboards</h3>';for(const b of d.leaderboards){h+='<div style="margin-bottom:14px"><div class="lbl" style="margin-bottom:6px">'+b.label+'</div>'+(b.entries.length?'<table>'+b.entries.map((e,i)=>'<tr class="'+(e.me?'me':'')+'"><td><span class="rank">'+(i+1)+'</span>'+e.name+'</td><td style="text-align:right;font-weight:700">'+e.value+'</td></tr>').join('')+'</table>':'<span class="muted small">Nobody sharing this yet.</span>')+'</div>';}h+='</div>';
 h+='<div class="panel"><h3>Streaks &amp; challenges</h3>'+(d.streaks.length?'<table><tr><th>Member</th><th>Green streak</th><th>Sleep streak</th></tr>'+d.streaks.map(e=>'<tr class="'+(e.me?'me':'')+'"><td>'+e.name+'</td><td>'+e.green_streak+' d</td><td>'+e.sleep_streak+' d</td></tr>').join('')+'</table>':'<span class="muted small">No shared recovery yet.</span>')+'</div>';
 h+='<div class="panel"><h3>Achievements</h3>'+d.achievements.map(a=>'<div style="margin-bottom:8px"><b>'+a.name+(a.me?' (you)':'')+'</b><br>'+(a.badges.length?a.badges.map(x=>'<span class="badge">'+x+'</span>').join(''):'<span class="muted small">no badges yet</span>')+'</div>').join('')+'</div>';
 if(d.peptides.length)h+='<div class="panel"><h3>Shared peptides</h3>'+d.peptides.map(p=>'<div style="margin-bottom:8px"><b>'+p.name+(p.me?' (you)':'')+'</b><br>'+(p.items.length?p.items.map(it=>'<span class="chip">'+it.name+(it.dose?' '+it.dose:'')+' · '+it.taken+'/'+it.per_week+' wk</span>').join(''):'<span class="muted small">none active</span>')+'</div>').join('')+'</div>';
 el.innerHTML=h;}
async function approve(cid,u){await fetch('/api/circles/approve',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({circle_id:cid,user_id:u})});openCircle(cid);}
async function decline(cid,u){await fetch('/api/circles/decline',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({circle_id:cid,user_id:u})});openCircle(cid);}
async function setShare(cid,k,v){const b={circle_id:cid};b[k]=v;await fetch('/api/circles/sharing',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)});openCircle(cid);}
async function revoke(cid){if(!confirm('Generate a new code? The old one stops working.'))return;const r=await api('/api/circles/revoke',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({circle_id:cid})});alert('New code: '+r.invite_code);openCircle(cid);}
async function leaveCircle(cid,owner){if(!confirm(owner?'Delete this circle for everyone?':'Leave this circle?'))return;await fetch('/api/circles/leave',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({circle_id:cid})});loadCircles();}
function copyCode(c){navigator.clipboard&&navigator.clipboard.writeText(c);}

/* ---------- SETTINGS ---------- */
function loadSettings(){$('#setName').value=me?me.name:'';if(me){$('#setAge').value=me.age||'';$('#setSex').value=me.sex||'';}}
async function saveProfile(){const age=$('#setAge').value,sex=$('#setSex').value;await fetch('/api/profile',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({age:age?+age:null,sex})});if(me){me.age=age?+age:null;me.sex=sex;}$('#setMsg2').textContent='Saved ✓ — check your Dashboard';setTimeout(()=>$('#setMsg2').textContent='',2500);}
async function saveName(){const n=$('#setName').value.trim();if(!n)return;await fetch('/api/profile',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({display_name:n})});me.name=n;$('#sideName').textContent=n;$('#setMsg').textContent='Saved ✓';setTimeout(()=>$('#setMsg').textContent='',1500);}

function mkChart(id,type,data,opts){opts=opts||{};const el=$('#'+id);if(!el)return;if(charts[id])charts[id].destroy();
 charts[id]=new Chart(el,{type,data,options:{responsive:true,maintainAspectRatio:false,animation:{duration:300},plugins:Object.assign({legend:{labels:{color:'#7d8b9a',boxWidth:12}}},opts.plugins||{}),indexAxis:opts.indexAxis||'x',scales:opts.scales?Object.fromEntries(Object.entries(opts.scales).map(([k,v])=>[k,Object.assign({},v,{ticks:{color:'#7d8b9a'},grid:{color:'#18202a'}})])):(type==='doughnut'||type==='polarArea'?{}:{x:{ticks:{color:'#7d8b9a'},grid:{color:'#18202a'}},y:{ticks:{color:'#7d8b9a'},grid:{color:'#18202a'}}})}});
 requestAnimationFrame(()=>{try{charts[id].resize();}catch(e){}});}
boot();
</script></body></html>
"""

REPORT_PAGE = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Health Report — WHOOP Circle</title>
<style>
body{margin:0;background:#f4f6f9;color:#12181f;font:14px/1.55 Inter,-apple-system,Segoe UI,Roboto,Arial,sans-serif}
.sheet{max-width:820px;margin:24px auto;background:#fff;padding:44px 52px;box-shadow:0 2px 20px rgba(0,0,0,.08)}
h1{font-size:26px;margin:0 0 2px}.sub{color:#67727e;margin-bottom:22px}
h2{font-size:14px;text-transform:uppercase;letter-spacing:1px;color:#8a95a1;border-bottom:2px solid #eef1f4;padding-bottom:6px;margin:26px 0 12px}
.readrow{display:flex;gap:24px;align-items:center;background:#0b0f14;color:#fff;border-radius:14px;padding:20px 24px}
.readnum{font-size:46px;font-weight:800;line-height:1}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.card{border:1px solid #e7ebef;border-radius:12px;padding:14px 16px}
.k{font-size:11px;text-transform:uppercase;letter-spacing:.6px;color:#8a95a1;font-weight:700}
.v{font-size:24px;font-weight:800}
table{width:100%;border-collapse:collapse;font-size:13.5px}th,td{text-align:left;padding:7px 8px;border-bottom:1px solid #eef1f4}
th{color:#8a95a1;font-size:11px;text-transform:uppercase;letter-spacing:.5px}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px}
.g{background:#12b886}.a{background:#f59f00}.r{background:#fa5252}
.pos{color:#12b886}.neg{color:#fa5252}
.banner{border-radius:10px;padding:12px 14px;font-size:13.5px;margin:8px 0}
.bok{background:#e6f7f0}.bwatch{background:#fff4e0}.balert{background:#ffe9ec}
.note{color:#8a95a1;font-size:12px;margin-top:8px}
.toolbar{max-width:820px;margin:0 auto;text-align:right}
.btn{background:#0b0f14;color:#fff;border:none;border-radius:8px;padding:9px 16px;font-weight:700;cursor:pointer}
@media print{.toolbar{display:none}body{background:#fff}.sheet{box-shadow:none;margin:0}}
</style></head><body>
<div class="toolbar"><button class="btn" onclick="window.print()">🖨 Print / Save as PDF</button></div>
<div class="sheet" id="sheet"><p>Loading…</p></div>
<script>
const api=u=>fetch(u).then(r=>r.json());
const f=v=>(v==null||v==='')?'—':v;
const dcls=s=>({optimal:'g',watch:'a',flag:'r'}[s]||'a');
(async()=>{
 const[me,rd,ew,vp,ld,sm,po,nar,dec]=await Promise.all([api('/api/me'),api('/api/health/readiness'),api('/api/health/early-warning'),api('/api/health/vitals'),api('/api/health/load'),api('/api/health/sleepmed'),api('/api/health/peptide-outcomes'),api('/api/health/narrative'),api('/api/decisions')]);
 if(!me.signed_in){document.getElementById('sheet').innerHTML='<p>Please sign in first.</p>';return;}
 const bandC={prime:'#12b886',moderate:'#f59f00',recover:'#fa5252'}[rd.band]||'#8a95a1';
 let h='<h1>Health Report</h1><div class="sub">'+me.name+' · generated '+nar.generated+'</div>';
 h+='<div class="readrow"><div><div style="font-size:11px;letter-spacing:1px;opacity:.7">READINESS</div><div class="readnum" style="color:'+bandC+'">'+f(rd.score)+'</div><div style="opacity:.8">'+f(rd.band)+'</div></div><div style="flex:1">'+f(rd.recommendation)+'</div></div>';
 h+='<h2>Weekly summary</h2><p>'+nar.narrative+'</p>';
 const ec={ok:'bok',watch:'bwatch',alert:'balert'}[ew.level];
 h+='<h2>Early-warning</h2><div class="banner '+ec+'"><b>'+ew.level.toUpperCase()+' · '+ew.confidence+'% confidence</b><br>'+ew.message+(ew.drivers.length?'<br><span class="note">Drivers: '+ew.drivers.join('; ')+'</span>':'')+'</div>';
 h+='<h2>Vitals vs your baseline</h2><table><tr><th>Marker</th><th>Latest</th><th>Your range</th><th>z</th><th>Status</th></tr>'+vp.vitals.map(v=>'<tr><td>'+v.name+'</td><td>'+v.latest+' '+v.unit+'</td><td>'+v.low+'–'+v.high+'</td><td>'+(v.z>0?'+':'')+v.z+'</td><td><span class="dot '+dcls(v.status)+'"></span>'+v.status+'</td></tr>').join('')+'</table>';
 h+='<h2>Training load</h2>';
 if(ld.enough)h+='<div class="grid"><div class="card"><div class="k">ACWR</div><div class="v">'+ld.acwr+'</div><div class="note">'+ld.zone+'</div></div><div class="card"><div class="k">Acute / Chronic</div><div class="v">'+ld.acute+' / '+ld.chronic+'</div><div class="note">7d vs 28d EWMA strain</div></div></div><p class="note">'+ld.message+'</p>';
 else h+='<p class="note">Not enough data yet.</p>';
 h+='<h2>Sleep</h2>';
 if(sm.enough){h+='<div class="grid"><div class="card"><div class="k">Avg duration</div><div class="v">'+f(sm.avg_hours)+'h</div></div><div class="card"><div class="k">Sleep debt (7d)</div><div class="v">'+f(sm.debt_h)+'h</div></div><div class="card"><div class="k">Regularity</div><div class="v">'+f(sm.regularity)+'/100</div></div><div class="card"><div class="k">Avg SpO₂ / Resp</div><div class="v">'+f(sm.spo2)+'% / '+f(sm.respiratory_rate)+'</div></div></div>';
  h+='<table style="margin-top:12px"><tr><th>Stage</th><th>You</th><th>Norm</th><th>Status</th></tr>'+sm.architecture.map(a=>'<tr><td>'+a.stage+'</td><td>'+a.pct+'%</td><td>'+a.norm+'</td><td><span class="dot '+dcls(a.status)+'"></span>'+a.status+'</td></tr>').join('')+'</table>';
  if(sm.breathing_flags.length)h+='<p class="note">'+sm.breathing_flags.join(' ')+'</p>';}
 else h+='<p class="note">Not enough data yet.</p>';
 if(dec.has_data){h+='<h2>Decision impact</h2><table><tr><th>Habit</th><th>Verdict</th><th>Recovery Δ</th><th>n</th></tr>'+dec.ratings.slice(0,8).map(r=>'<tr><td>'+r.tag+'</td><td>'+r.emoji+' '+r.rating+'</td><td class="'+(r.recovery_delta>0?'pos':r.recovery_delta<0?'neg':'')+'">'+(r.recovery_delta==null?'—':(r.recovery_delta>0?'+':'')+r.recovery_delta)+'</td><td>'+r.n+'</td></tr>').join('')+'</table>';}
 if(po.peptides.length){h+='<h2>Peptide outcomes (before vs during)</h2>';for(const p of po.peptides){if(p.status){h+='<p><b>'+p.name+'</b> <span class="note">'+p.status+'</span></p>';continue;}
  h+='<p style="margin:10px 0 4px"><b>'+p.name+(p.dose?' · '+p.dose:'')+'</b> <span class="note">since '+p.start+' · '+p.confidence+' confidence</span></p><table><tr><th>Metric</th><th>Before</th><th>During</th><th>Δ</th></tr>'+p.metrics.map(m=>'<tr><td>'+m.label+'</td><td>'+f(m.before)+'</td><td>'+f(m.during)+'</td><td class="'+(m.delta>0?'pos':m.delta<0?'neg':'')+'">'+(m.delta==null?'—':(m.delta>0?'+':'')+m.delta)+'</td></tr>').join('')+'</table>';}}
 h+='<p class="note" style="margin-top:26px;border-top:1px solid #eef1f4;padding-top:12px">This report is generated from your own WHOOP data using personal-baseline statistics and published sports-science/clinical thresholds. It is informational only and not medical advice. Discuss any concerning patterns with a qualified clinician.</p>';
 document.getElementById('sheet').innerHTML=h;
})();
</script></body></html>
"""
