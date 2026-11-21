#!/usr/bin/env python3
"""Slow, independent reference implementation of rules.md (pure Python).
Usage: python reference.py DATA_DIR OUT_DIR   -> writes OUT_DIR/user_daily.csv, campaign_daily.csv
Used only to cross-check the Rust pipeline on generated datasets."""
import calendar
import csv
import glob
import json
import os
import sys
from collections import defaultdict

DAY = 86_400_000
DIG = set(b"0123456789")


def lines_of(path):
    with open(path, "rb") as f:
        data = f.read()
    parts = data.split(b"\n")
    if data.endswith(b"\n"):
        parts = parts[:-1]
    return parts[1:]  # header skipped


def fields(line, n):
    if line == b"":
        return None
    fs = line.split(b",")
    if len(fs) != n:
        return None
    out = []
    for f in fs:
        if b'"' in f:
            if len(f) >= 2 and f[0:1] == b'"' and f[-1:] == b'"' and b'"' not in f[1:-1]:
                out.append(f[1:-1])
            else:
                return None
        else:
            out.append(f)
    return out


def is_digits(b):
    return len(b) > 0 and all(c in DIG for c in b)


def civil_ms(b):
    # YYYY-MM-DDTHH:MM:SS
    if len(b) != 19 or b[4:5] != b"-" or b[7:8] != b"-" or b[10:11] != b"T" or b[13:14] != b":" or b[16:17] != b":":
        return None
    parts = [b[0:4], b[5:7], b[8:10], b[11:13], b[14:16], b[17:19]]
    if not all(is_digits(x) for x in parts):
        return None
    y, mo, d, h, mi, s = (int(x) for x in parts)
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or s > 59 or d < 1:
        return None
    if d > calendar.monthrange(y, mo)[1] if y >= 1 else d > 31:
        return None
    # days from civil
    yy = y - 1 if mo <= 2 else y
    era = yy // 400
    yoe = yy - era * 400
    doy = (153 * ((mo + 9) % 12) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    days = era * 146097 + doe - 719468
    return days * DAY + h * 3600000 + mi * 60000 + s * 1000


def parse_ts(b):
    if b is None or len(b) == 0:
        return None
    if is_digits(b):
        return int(b)
    if len(b) == 20 and b[19:20] == b"Z":
        return civil_ms(b[:19])
    if len(b) == 25 and b[19:20] in (b"+", b"-") and b[22:23] == b":":
        base = civil_ms(b[:19])
        if base is None or not is_digits(b[20:22]) or not is_digits(b[23:25]):
            return None
        oh, om = int(b[20:22]), int(b[23:25])
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60000
        return base - off if b[19:20] == b"+" else base + off
    return None


def main():
    data, out = sys.argv[1], sys.argv[2]
    os.makedirs(out, exist_ok=True)
    man = json.load(open(os.path.join(data, "manifest.json")))
    ws = parse_ts(man["window_start"].encode())
    we = parse_ts(man["window_end"].encode())

    products = {}
    for line in lines_of(os.path.join(data, "products.csv")):
        f = fields(line, 2)
        if f:
            products[f[0]] = f[1]
    campaigns = {}
    for line in lines_of(os.path.join(data, "campaigns.csv")):
        f = fields(line, 2)
        if f:
            campaigns[f[0]] = f[1]

    # links: union find
    parent = {}

    def find(x):
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    for path in sorted(glob.glob(os.path.join(data, "links", "*.csv"))):
        for line in lines_of(path):
            f = fields(line, 2)
            if not f or f[0] == b"" or f[1] == b"":
                continue
            a, b = f
            parent.setdefault(a, a)
            parent.setdefault(b, b)
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb
    name = {}
    for x in parent:
        r = find(x)
        if r not in name or x < name[r]:
            name[r] = x

    def person(u):
        if u in parent:
            return name[find(u)]
        return u

    # events
    best = {}  # eid -> (ingest, fields)
    for path in sorted(glob.glob(os.path.join(data, "events", "*.csv"))):
        for line in lines_of(path):
            f = fields(line, 7)
            if not f or f[0] == b"":
                continue
            ing = parse_ts(f[3])
            ing = ing if ing is not None else 1 << 62
            cur = best.get(f[0])
            if cur is None or ing < cur[0]:
                best[f[0]] = (ing, f)
    per = defaultdict(list)  # person -> [(ts, price or None, category)]
    for eid, (ing, f) in best.items():
        _, user, ts_b, _, et, prod, price_b = f
        if user == b"" or prod == b"":
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (ws <= ts < we):
            continue
        if et not in (b"view", b"cart", b"purchase"):
            continue
        price = None
        if et == b"purchase":
            if not is_digits(price_b):
                continue
            price = int(price_b)
            if not (1 <= price <= 10_000_000):
                continue
        if ing - ts > DAY:
            continue
        per[person(user)].append((ts, price, products.get(prod, b"unknown")))

    clicks = defaultdict(list)  # person -> [(ts, cid, camp)]
    seen = set()
    for path in sorted(glob.glob(os.path.join(data, "clicks", "*.csv"))):
        for line in lines_of(path):
            f = fields(line, 4)
            if not f or f[0] == b"":
                continue
            cid, user, camp, ts_b = f
            if user == b"" or camp not in campaigns:
                continue
            ts = parse_ts(ts_b)
            if ts is None or not (ws - 7 * DAY <= ts < we):
                continue
            if cid in seen:
                continue
            seen.add(cid)
            clicks[person(user)].append((ts, cid, camp))

    ud = {}
    cd = defaultdict(lambda: [0, 0, 0, 0])
    for p, evs in per.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        for e in evs:
            if sessions and e[0] - sessions[-1][-1][0] <= 1_800_000:
                sessions[-1].append(e)
            else:
                sessions.append([e])
        cks = clicks.get(p, [])
        for sess in sessions:
            start = sess[0][0]
            day = start // DAY
            rev = sum(e[1] for e in sess if e[1] is not None)
            haspurch = any(e[1] is not None for e in sess)
            row = ud.setdefault((p, day), [0, 0, 0, defaultdict(int)])
            row[0] += 1
            row[1] += len(sess)
            row[2] += rev
            for e in sess:
                if e[1] is not None:
                    row[3][e[2]] += e[1]
            window = [c for c in cks if start - 7 * DAY <= c[0] <= start]
            if window:
                mt = max(c[0] for c in window)
                cred = min(c[1] for c in window if c[0] == mt)
                camp = next(c[2] for c in window if c[0] == mt and c[1] == cred)
            else:
                camp = b"organic"
            r = cd[(camp, day)]
            r[0] += 1
            r[1] += 1 if haspurch else 0
            r[2] += rev
            for c in set(c[2] for c in window):
                cd[(c, day)][3] += 1

    import datetime
    def dstr(d):
        return (datetime.date(1970, 1, 1) + datetime.timedelta(days=d)).isoformat()

    with open(os.path.join(out, "user_daily.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
        for (p, day), (s, e, r, cats) in ud.items():
            top = ""
            if cats:
                m = max(cats.values())
                top = min(c for c, v in cats.items() if v == m).decode()
            w.writerow([p.decode(), dstr(day), s, e, r, top])
    with open(os.path.join(out, "campaign_daily.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"])
        for (c, day), (s, ps, r, a) in cd.items():
            ch = b"organic" if c == b"organic" else campaigns[c]
            w.writerow([c.decode(), ch.decode(), dstr(day), s, ps, r, 0 if c == b"organic" else a])


if __name__ == "__main__":
    main()
