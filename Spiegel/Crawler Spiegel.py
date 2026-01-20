import os
import re
import time
import csv
import gzip
import io
import sys
import argparse\r?\nimport requests
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from classify_rules import classify_url

# --- Settings ---
START_YEAR = 2025
END_YEAR = 2026
ROBOTS_URL = "https://www.spiegel.de/robots.txt"  # kann ggf. Zugriffe steuern

EXCLUDE_FRAU = {
    "spielerfrau",
    "frau-von",
    "frau-des",
    "ehefrau",
    "exfrau",
    "ex-frau",
    "freundin",
    "gattin",
    "verlobte",
    # French
    "epouse",
    "epouse-de",
    "ex-epouse",
    # Italian
    "moglie",
    "ex-moglie",
}

WOMEN_NAMES = {
    "rapinoe",
    "morgan",
    "hegerberg",
    "miedema",
    "putellas",
    "bonmati",
    "kerr",
    "marta",
    "kirby",
    "mead",
    "bronze",
    "hamm",
    # Switzerland
    "bachmann",
    "maendly",
    "beney",
    "maritz",
    "crnogorcevic",
    "thalmann",
    "calligaris",
    "lehmann",
    "waelti",
    # International
    "popp",
    "oberdorf",
    "hansen",
    "hasegawa",
    "foord",
    "graham",
    "rodman",
    "lavelle",
    "press",
}

MEN_NAMES = {
    "messi",
    "ronaldo",
    "mbappe",
    "haaland",
    "neymar",
    "lewandowski",
    "benzema",
    "modric",
    "kroos",
    "kane",
    "salah",
    "bellingham",
    "vinicius",
}

OUTLET_WOMEN = {
    "frauen",
    "frauenfussball",
    "fussballerinnen",
    "frauenliga",
    "frauenbundesliga",
    "2-frauen-bundesliga",
    "dfb-frauen",
    "uefa-frauen",
    "fifa-frauen",
    "frauen-em",
    "frauen-wm",
    "frauen-weltmeisterschaft",
    "frauen-europameisterschaft",
    "frauen-nationalmannschaft",
    "frauen-nationalteam",
    "frauennati",
    "frauen-nati",
}

OUTLET_MEN = {
    "bundesliga",
    "2-bundesliga",
    "dritte-liga",
    "dfb-pokal",
    "champions-league",
    "europa-league",
    "conference-league",
}

WOMEN_TOKENS = OUTLET_WOMEN | WOMEN_NAMES
MEN_TOKENS = OUTLET_MEN | MEN_NAMES

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Maturaarbeit; contact: your-email@example.com)"
}

REQUEST_DELAY = 0.8
MAX_SITEMAPS = None     # keine Begrenzung
MAX_URLS = None       # keine Begrenzung
DEBUG = True
MAX_DEBUG_URLS = 5

def setup_logging(log_path: str):
    if not log_path:
        return
    log_file = open(log_path, "a", encoding="utf-8")
    sys.stdout = log_file
    sys.stderr = log_file

def classify(text: str) -> str:
    return classify_url(text, "Spiegel")

def tokenize(url: str) -> set[str]:
    lower = url.lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", lower)
    return set(t for t in cleaned.split() if t)

def matches_rules(url: str) -> bool:
    tokens = tokenize(url)
    return bool(tokens & (WOMEN_TOKENS | MEN_TOKENS | EXCLUDE_FRAU))

def year_from_lastmod(lastmod: str):
    if not lastmod:
        return None
    try:
        return int(lastmod[:4])
    except:
        return None

def year_from_url(url: str):
    parts = urlparse(url).path.split("/")
    for p in parts:
        if p.isdigit() and len(p) == 4:
            y = int(p)
            if 1900 < y < 2100:
                return y
    return None

def year_from_sitemap_url(url: str):
    # Spiegel article sitemaps are like .../sitemap-YYYY-MM_N.xml
    parts = url.rsplit("/", 1)[-1]
    if parts.startswith("sitemap-") and len(parts) >= 12:
        try:
            return int(parts[8:12])
        except ValueError:
            return None
    return None

def fetch(url: str) -> bytes:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.content
    if url.endswith(".gz"):
        data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
    return data

def get_sitemaps_from_robots() -> list[str]:
    r = requests.get(ROBOTS_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    sitemaps = []
    for line in r.text.splitlines():
        line = line.strip()
        if line.lower().startswith("sitemap:"):
            sitemaps.append(line.split(":", 1)[1].strip())
    return sitemaps

def parse_sitemap(xml_bytes: bytes):
    root = ET.fromstring(xml_bytes)
    tag = root.tag.lower()
    if tag.endswith("sitemapindex"):
        items = []
        for sm in root.findall(".//{*}sitemap"):
            loc_el = sm.find("{*}loc")
            last_el = sm.find("{*}lastmod")
            if loc_el is None or not loc_el.text:
                continue
            items.append((loc_el.text.strip(), last_el.text.strip() if last_el is not None and last_el.text else ""))
        return "index", items
    if tag.endswith("urlset"):
        items = []
        for u in root.findall(".//{*}url"):
            loc_el = u.find("{*}loc")
            last_el = u.find("{*}lastmod")
            if loc_el is None or not loc_el.text:
                continue
            items.append((loc_el.text.strip(), last_el.text.strip() if last_el is not None and last_el.text else ""))
        return "urlset", items
    return "unknown", []

def choose_best_root_sitemap(sitemaps: list[str]) -> str:
    # Prefer the full article sitemap index, not video/plus/news sitemaps
    for sm in sitemaps:
        if sm.rstrip("/").endswith("www.spiegel.de/sitemap.xml"):
            return sm
    for sm in sitemaps:
        if sm.rstrip("/").endswith("/sitemap.xml") and "/sitemaps/videos/" not in sm:
            return sm
    for sm in sitemaps:
        if "news-de.xml" in sm:
            return sm
    return sitemaps[0] if sitemaps else ""

def iter_urlsets(root_sitemap: str):
    queue = [root_sitemap]
    seen = set()
    while queue and (MAX_SITEMAPS is None or len(seen) < MAX_SITEMAPS):
        sm_url = queue.pop(0)
        if sm_url in seen:
            continue
        seen.add(sm_url)

        try:
            xml = fetch(sm_url)
            typ, items = parse_sitemap(xml)
        except Exception as e:
            print(f"[WARN] Sitemap fetch/parse failed: {sm_url} -> {e}")
            continue

        if typ == "index":
            for loc, lastmod in items:
                # Only article sitemaps are needed (avoid video sitemaps with invalid XML)
                if "/sitemaps/article/" not in loc:
                    continue
                sm_year = year_from_sitemap_url(loc) or year_from_lastmod(lastmod)
                if sm_year is not None and (sm_year < START_YEAR or sm_year > END_YEAR):
                    continue
                queue.append(loc)
        elif typ == "urlset":
            sm_year = year_from_sitemap_url(sm_url)
            yield sm_url, items, sm_year

        time.sleep(REQUEST_DELAY)

def main():
    try:
        sitemaps = get_sitemaps_from_robots()
        if not sitemaps:
            raise RuntimeError("Keine Sitemap im robots.txt gefunden.")
        root = choose_best_root_sitemap(sitemaps)
        if DEBUG:
            print("[INFO] Root sitemap:", root)
    except Exception as e:
        print("[ERROR] Konnte Sitemaps nicht ermitteln:", e)
        return

    rows = []
    total_sitemaps = 0
    total_entries = 0
    football_candidates = 0
    year_filtered = 0
    debug_urls = []

    for sm_url, entries, sm_year in iter_urlsets(root):
        total_sitemaps += 1
        total_entries += len(entries)
        for loc, lastmod in entries:
            if not matches_rules(loc):
                continue
            football_candidates += 1

            y = year_from_lastmod(lastmod) or year_from_url(loc) or sm_year
            if y is None or y < START_YEAR or y > END_YEAR:
                year_filtered += 1
                continue

            rows.append({"year": y, "lastmod": lastmod, "url": loc, "category": classify(loc)})
            if DEBUG and len(debug_urls) < MAX_DEBUG_URLS:
                debug_urls.append(loc)

            if MAX_URLS is not None and len(rows) >= MAX_URLS:
                print("[INFO] MAX_URLS erreicht – Stop.")
                break
        if MAX_URLS is not None and len(rows) >= MAX_URLS:
            break

    counts = {}
    for r in rows:
        y = r["year"]
        c = r["category"]
        counts.setdefault(y, {"Frauenfussball": 0, "Herrenfussball": 0, "Total": 0})
        counts[y][c] += 1
        counts[y]["Total"] += 1

    base_dir = os.path.dirname(__file__)
    urls_path = os.path.join(base_dir, "spiegel_fussball_urls_2024_2025.csv")
    counts_path = os.path.join(base_dir, "spiegel_fussball_counts_2024_2025.csv")

    with open(urls_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["year", "lastmod", "category", "url"])
        w.writeheader()
        w.writerows(rows)

    with open(counts_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["year", "Frauenfussball", "Herrenfussball", "Total", "Frauen_Anteil"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for y in sorted(counts.keys()):
            total = counts[y]["Total"]
            women = counts[y]["Frauenfussball"]
            w.writerow({
                "year": y,
                "Frauenfussball": women,
                "Herrenfussball": counts[y]["Herrenfussball"],
                "Total": total,
                "Frauen_Anteil": round((women / total) if total else 0.0, 4)
            })

    print("Fertig.")
    print("Export:", urls_path)
    print("Export:", counts_path)
    if DEBUG:
        print(f"[INFO] Sitemaps: {total_sitemaps}, Entries: {total_entries}")
        print(f"[INFO] Football URLs: {football_candidates}, In-range: {len(rows)}, Year filtered: {year_filtered}")
        if debug_urls:
            print("[INFO] Sample URLs:")
            for u in debug_urls:
                print(" -", u)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spiegel Crawler")
    parser.add_argument("--log", help="Pfad fuer Logfile (optional).", default="")
    parser.add_argument("--debug", action="store_true", help="Debug-Ausgaben aktivieren.")
    args = parser.parse_args()
    setup_logging(args.log)
    if args.debug:
        DEBUG = True
    main()




