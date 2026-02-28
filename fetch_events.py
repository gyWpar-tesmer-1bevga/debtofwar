#!/usr/bin/env python3
"""
THE DEBT OF WAR — fetch_events.py
Ingests RSS feeds, classifies events, estimates cost impact.
Outputs: data/events.json + data/meta.json
"""

import json, time, hashlib, re, xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

# ── RSS FEEDS (all free, no keys) ──────────────────────────────
FEEDS = [
    {"name": "Reuters World",     "url": "https://feeds.reuters.com/Reuters/worldNews"},
    {"name": "Reuters Top",       "url": "https://feeds.reuters.com/reuters/topNews"},
    {"name": "BBC World",         "url": "http://feeds.bbci.co.uk/news/world/rss.xml"},
    {"name": "Al Jazeera",        "url": "https://www.aljazeera.com/xml/rss/all.xml"},
    {"name": "AP Top News",       "url": "https://rsshub.app/apnews/topics/apf-topnews"},
    {"name": "France24",          "url": "https://www.france24.com/en/rss"},
    {"name": "Defense News",      "url": "https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml"},
    {"name": "Middle East Eye",   "url": "https://www.middleeasteye.net/rss"},
]

# ── CONFLICT KEYWORDS ──────────────────────────────────────────
CONFLICT_KEYWORDS = [
    "strike", "airstrike", "missile", "bomb", "bombing", "explosion",
    "attack", "killed", "dead", "casualties", "troops", "military",
    "war", "conflict", "offensive", "shelling", "drone", "rocket",
    "invasion", "assault", "clash", "ceasefire", "nuclear", "sanction",
    "Iran", "Gaza", "Ukraine", "Russia", "Israel", "Hamas", "Hezbollah",
    "Sudan", "Yemen", "Syria", "Taliban", "Houthi", "North Korea"
]

# ── WEAPON COST TABLE (USD) ────────────────────────────────────
# Source: CSIS, RAND, DoD procurement data, media reports
WEAPON_COSTS = [
    # (regex pattern, cost_usd, label)
    (r"nuclear|nuke",                    2_000_000_000, "Nuclear weapon deployment"),
    (r"aircraft carrier",                  800_000_000, "Aircraft carrier operation/day"),
    (r"B-2|B2 bomber",                     135_000,     "B-2 bomber sortie/hr"),
    (r"F-35|F35",                           36_000,     "F-35 sortie/hr"),
    (r"F-16|F16",                           22_000,     "F-16 sortie/hr"),
    (r"tomahawk",                        2_000_000,     "Tomahawk cruise missile"),
    (r"patriot.{0,20}(missile|intercept)", 4_000_000,  "Patriot interceptor"),
    (r"HIMARS|himars",                     100_000,     "HIMARS rocket"),
    (r"JDAM|jdam",                          30_000,     "JDAM guided bomb"),
    (r"abrams|tank.{0,10}(destroy|hit)",  10_000_000,  "Abrams tank"),
    (r"drone.{0,20}(strike|attack)",        50_000,    "Drone strike est."),
    (r"airstrike|air strike",              500_000,    "Airstrike estimated cost"),
    (r"artillery|shelling|shell",           10_000,    "Artillery round"),
    (r"missile",                           500_000,    "Missile (generic est.)"),
    (r"bomb",                               50_000,    "Bomb/munition"),
    (r"explosion|blast",                    20_000,    "Explosive device"),
]

# ── SEVERITY ───────────────────────────────────────────────────
HIGH_WORDS   = {"killed","dead","casualties","massacre","nuclear","invasion",
                "airstrike","missile strike","bombing","dozens","hundreds"}
MEDIUM_WORDS = {"attack","attacked","clash","shelling","drone","wounded","offensive"}
LOW_WORDS    = {"ceasefire","tension","protest","sanctions","warning"}

def classify(text):
    t = text.lower()
    h = sum(1 for w in HIGH_WORDS   if w in t)
    m = sum(1 for w in MEDIUM_WORDS if w in t)
    if h >= 1: return "high"
    if m >= 1: return "medium"
    return "low"

def estimate_cost(text):
    """Return (cost_usd, label) for the most expensive weapon pattern found."""
    for pattern, cost, label in WEAPON_COSTS:
        if re.search(pattern, text, re.IGNORECASE):
            return cost, label
    return 0, None

def event_id(title, date):
    return hashlib.md5((title[:50] + str(date)).encode()).hexdigest()[:10]

# ── FETCH RSS ──────────────────────────────────────────────────
HEADERS = {"User-Agent": "DebtOfWar/2.0 (conflict-cost-tracker; educational)"}

def fetch_feed(feed):
    try:
        req = Request(feed["url"], headers=HEADERS)
        with urlopen(req, timeout=10) as r:
            raw = r.read()
        root = ET.fromstring(raw)
        items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")
        results = []
        for item in items[:30]:
            def g(tag):
                el = item.find(tag)
                return (el.text or "").strip() if el is not None else ""
            title = g("title") or g("{http://www.w3.org/2005/Atom}title")
            link  = g("link")  or g("{http://www.w3.org/2005/Atom}link")
            desc  = g("description") or g("summary") or g("{http://www.w3.org/2005/Atom}summary")
            pub   = g("pubDate") or g("published") or g("{http://www.w3.org/2005/Atom}published")
            # Strip HTML tags from desc
            desc = re.sub(r"<[^>]+>", "", desc)
            full = f"{title} {desc}"
            results.append({"title": title, "link": link, "desc": desc,
                             "pub": pub, "full": full, "source": feed["name"]})
        print(f"  ✓ {feed['name']}: {len(results)} items")
        return results
    except Exception as e:
        print(f"  ✗ {feed['name']}: {e}")
        return []

def parse_date(s):
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"]:
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
        except: pass
    return datetime.now(timezone.utc)

# ── EXISTING EVENTS (to merge, not duplicate) ──────────────────
def load_existing():
    p = Path("data/events.json")
    if p.exists():
        try:
            return {e["id"]: e for e in json.loads(p.read_text())}
        except: pass
    return {}

# ── MAIN ───────────────────────────────────────────────────────
def main():
    print("=== THE DEBT OF WAR — Feed Ingestion ===")
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    existing = load_existing()
    new_events = []
    seen_titles = set()

    for feed in FEEDS:
        items = fetch_feed(feed)
        time.sleep(0.5)
        for item in items:
            title = item["title"]
            if not title or title[:40] in seen_titles: continue
            if not any(kw.lower() in item["full"].lower() for kw in CONFLICT_KEYWORDS): continue

            ts = parse_date(item["pub"])
            if ts < cutoff: continue

            seen_titles.add(title[:40])
            cost, cost_label = estimate_cost(item["full"])
            sev   = classify(item["full"])
            eid   = event_id(title, ts.date())

            new_events.append({
                "id":          eid,
                "title":       title[:200],
                "source":      item["source"],
                "url":         item["link"],
                "description": item["desc"][:300],
                "timestamp":   ts.isoformat(),
                "severity":    sev,
                "cost_usd":    cost,
                "cost_label":  cost_label,
                "is_new":      eid not in existing,
            })

    # Merge with existing, newest first, keep 72h window
    merged = {e["id"]: e for e in new_events}
    merged.update({k: v for k, v in existing.items() if k not in merged})
    all_events = sorted(merged.values(), key=lambda x: x["timestamp"], reverse=True)

    # Only keep last 72h
    recent_cutoff = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
    all_events = [e for e in all_events if e["timestamp"] >= recent_cutoff][:200]

    # Mark only events from last 3h as truly "new" for alert purposes
    alert_cutoff = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
    for e in all_events:
        e["is_breaking"] = e["timestamp"] >= alert_cutoff and e["severity"] == "high"

    Path("data").mkdir(exist_ok=True)
    Path("data/events.json").write_text(json.dumps(all_events, indent=2))

    # Meta file — total cost added today by detected events
    today_cost = sum(e["cost_usd"] for e in all_events
                     if e["timestamp"] >= datetime.now(timezone.utc).replace(
                         hour=0,minute=0,second=0).isoformat())
    meta = {
        "last_updated":    datetime.now(timezone.utc).isoformat(),
        "event_count":     len(all_events),
        "breaking_count":  sum(1 for e in all_events if e.get("is_breaking")),
        "today_extra_cost": today_cost,
        "high_count":      sum(1 for e in all_events if e["severity"]=="high"),
    }
    Path("data/meta.json").write_text(json.dumps(meta, indent=2))
    print(f"\n✓ {len(all_events)} events saved. Today extra cost: ${today_cost:,.0f}")
    print(f"  Breaking: {meta['breaking_count']} | High severity: {meta['high_count']}")

if __name__ == "__main__":
    main()
