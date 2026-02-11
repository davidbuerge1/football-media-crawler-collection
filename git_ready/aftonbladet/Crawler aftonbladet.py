import re
import time
import csv
import requests
import xml.etree.ElementTree as ET
from urllib.parse import unquote

# --- Settings ---
START_YEAR = 2005
END_YEAR = 2025

SITEMAP_INDEX = "https://www.aftonbladet.se/sitemap.xml"

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
    "epouse",
    "epouse-de",
    "ex-epouse",
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
    "hamm",
    "popp",
    "oberdorf",
    "hansen",
    "hasegawa",
    "foord",
    "rodman",
    "lavelle",
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
    "damfotboll",
    "damallsvenskan",
    "damlandslag",
    "damlandslaget",
    "damlandslagetfotboll",
    "damfotbollslandslaget",
    "dam",
    "kvinn",
    "kvinnor",
    "wsl",
    "nwsl",
}

OUTLET_MEN = {
    "herrfotboll",
    "herrlandslag",
    "herrlandslaget",
    "allsvenskan",
    "superettan",
    "premier-league",
    "champions-league",
    "europa-league",
    "fa-cup",
}

SOCCER_TOKENS = {
    "fotboll",
    "football",
    "soccer",
}

WOMEN_SOCCER_HINTS = {
    "damfotboll",
    "damallsvenskan",
    "damlandslag",
    "damlandslaget",
    "dam",
    "kvinn",
    "kvinnor",
    "wsl",
    "nwsl",
}

EXCLUDE_SPORTS = {
    "handboll",
    "handball",
    "basket",
    "basketboll",
    "ishockey",
    "hockey",
    "innebandy",
    "bandy",
    "skidor",
    "skidskytte",
    "alpint",
    "tennis",
    "badminton",
    "golf",
    "formel1",
    "formula1",
    "f1",
    "motorsport",
}

WOMEN_TOKENS = OUTLET_WOMEN | WOMEN_NAMES
MEN_TOKENS = OUTLET_MEN | MEN_NAMES

REQUIRE_PATH_CONTAINS = "/sportbladet/fotboll/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Maturaarbeit; contact: your-email@example.com)"
}

REQUEST_DELAY = 0.2
MAX_SITEMAPS = 20000
STATUS_EVERY_SECONDS = 1.0


def tokenize(url: str) -> set[str]:
    decoded = unquote(url)
    lower = (
        decoded.lower()
        .replace("\u00e5", "a")
        .replace("\u00e4", "a")
        .replace("\u00f6", "o")
        .replace("\u00e6", "ae")
        .replace("\u00f8", "o")
    )
    cleaned = re.sub(r"[^a-z0-9]+", " ", lower)
    return set(t for t in cleaned.split() if t)


def classify_by_url(url: str) -> str:
    if REQUIRE_PATH_CONTAINS and REQUIRE_PATH_CONTAINS not in url:
        return "Herrenfussball"
    tokens = tokenize(url)
    if tokens & EXCLUDE_SPORTS:
        return "Herrenfussball"
    has_women_context = bool(tokens & OUTLET_WOMEN)
    if (tokens & WOMEN_NAMES) and not has_women_context:
        has_women_context = False
    if has_women_context and (tokens & SOCCER_TOKENS or tokens & WOMEN_SOCCER_HINTS):
        return "Frauenfussball"
    if tokens & EXCLUDE_FRAU:
        return "Herrenfussball"
    if tokens & MEN_TOKENS:
        return "Herrenfussball"
    return "Herrenfussball"


def matches_rules(url: str) -> bool:
    if REQUIRE_PATH_CONTAINS and REQUIRE_PATH_CONTAINS not in url:
        return False
    tokens = tokenize(url)
    if tokens & EXCLUDE_SPORTS:
        return False
    if tokens & MEN_TOKENS:
        return True
    if tokens & EXCLUDE_FRAU:
        return True
    if (tokens & OUTLET_WOMEN) and (tokens & SOCCER_TOKENS or tokens & WOMEN_SOCCER_HINTS):
        return True
    return False


def year_month_from_lastmod(lastmod: str):
    if not lastmod or len(lastmod) < 7:
        return None, None
    try:
        return int(lastmod[:4]), int(lastmod[5:7])
    except ValueError:
        return None, None


def year_month_from_url(url: str):
    m = re.search(r"/(19\d{2}|20\d{2})/(\d{2})/", url)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"(19\d{2}|20\d{2})(\d{2})(\d{2})", url)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def year_from_sitemap_url(url: str):
    m = re.search(r"/(19\d{2}|20\d{2})-(\d{2})-[^/]*\.xml", url)
    return int(m.group(1)) if m else None


def month_from_sitemap_url(url: str):
    m = re.search(r"/(19\d{2}|20\d{2})-(\d{2})-[^/]*\.xml", url)
    return int(m.group(2)) if m else None


def fetch(url: str) -> bytes:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.content


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


def iter_sitemaps():
    xml = fetch(SITEMAP_INDEX)
    typ, items = parse_sitemap(xml)
    if typ == "urlset":
        yield SITEMAP_INDEX
        return
    if typ != "index":
        raise RuntimeError("Root sitemap is not an index.")
    count = 0
    for loc, _lastmod in items:
        y = year_from_sitemap_url(loc)
        if y is not None and (y < START_YEAR or y > END_YEAR):
            continue
        yield loc
        count += 1
        if count >= MAX_SITEMAPS:
            break


def list_sitemaps():
    return list(iter_sitemaps())


def print_status(message: str, last_print: float) -> float:
    now = time.time()
    if now - last_print >= STATUS_EVERY_SECONDS:
        print(message, end="\r", flush=True)
        return now
    return last_print


def main():
    last_print = 0.0
    sitemaps = list_sitemaps()
    total_sitemaps = len(sitemaps)
    rows = []
    matched_urls = 0
    for idx, sm_url in enumerate(sitemaps, start=1):
        try:
            xml = fetch(sm_url)
            typ, entries = parse_sitemap(xml)
        except Exception as e:
            print(f"[WARN] Failed sitemap: {sm_url} -> {e}")
            continue

        if typ != "urlset":
            continue

        for loc, lastmod in entries:
            if not matches_rules(loc):
                continue
            matched_urls += 1

            y, m = year_month_from_lastmod(lastmod)
            if y is None:
                y, m = year_month_from_url(loc)
            if y is None:
                y = year_from_sitemap_url(sm_url)
                m = month_from_sitemap_url(sm_url)

            if y is None or y < START_YEAR or y > END_YEAR:
                continue

            rows.append({
                "year": y,
                "month": m or "",
                "lastmod": lastmod,
                "url": loc,
                "category": classify_by_url(loc)
            })

        last_print = print_status(
            f"Sitemaps: {idx}/{total_sitemaps} | Matches: {matched_urls} | Rows: {len(rows)}",
            last_print,
        )
        time.sleep(REQUEST_DELAY)

    if total_sitemaps:
        print(f"Sitemaps: {total_sitemaps}/{total_sitemaps} | Matches: {matched_urls} | Rows: {len(rows)}")
    counts = {}
    for r in rows:
        y = r["year"]
        c = r["category"]
        counts.setdefault(y, {"Frauenfussball": 0, "Herrenfussball": 0, "Total": 0})
        counts[y][c] += 1
        counts[y]["Total"] += 1

    urls_csv = f"aftonbladet_fussball_urls_{START_YEAR}_{END_YEAR}.csv"
    counts_csv = f"aftonbladet_fussball_counts_{START_YEAR}_{END_YEAR}.csv"

    with open(urls_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["year", "month", "lastmod", "category", "url"])
        w.writeheader()
        w.writerows(rows)

    with open(counts_csv, "w", newline="", encoding="utf-8") as f:
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
    print(f"Export: {urls_csv}")
    print(f"Export: {counts_csv}")


if __name__ == "__main__":
    main()
