import re
import time
import csv
import requests
import xml.etree.ElementTree as ET
from urllib.parse import unquote
from datetime import datetime
from email.utils import parsedate_to_datetime

# --- Settings ---
START_YEAR = 2005
END_YEAR = time.gmtime().tm_year

SITEMAP_INDEX = "https://www.espn.com/sitemap.xml"
REQUIRE_PATH_CONTAINS = "/soccer/"

RSS_FEEDS = [
    "https://www.espn.com/espn/rss/soccer/news",
    "http://soccernet.espn.com/rss/news",
]

RSS_FEED_DISCOVERY_URLS = [
    "https://www.espn.com/soccer/story/_/id/37380404/rss-feeds",
]


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
    "hasegawa",
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
    "women",
    "womens",
    "uswnt",
    "nwsl",
    "wsl",
    "lionesses",
    "matildas",
    "shebelieves",
    "womens-world-cup",
    "womens-champions-league",
}

OUTLET_MEN = {
    "premier-league",
    "champions-league",
    "europa-league",
    "conference-league",
    "fa-cup",
    "la-liga",
    "serie-a",
    "bundesliga",
    "ligue-1",
    "mls",
    "concacaf",
    "gold-cup",
    "copa-america",
    "world-cup",
    "uefa",
}

SOCCER_TOKENS = {
    "soccer",
    "football",
}

WOMEN_SOCCER_HINTS = {
    "women",
    "womens",
    "uswnt",
    "nwsl",
    "wsl",
    "lionesses",
    "matildas",
}

WOMEN_TOKENS = OUTLET_WOMEN | WOMEN_NAMES
MEN_TOKENS = OUTLET_MEN | MEN_NAMES

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Maturaarbeit; contact: your-email@example.com)"
}

REQUEST_DELAY = 0.2
MAX_SITEMAPS = 20000
STATUS_EVERY_SECONDS = 1.0


def tokenize(url: str) -> set[str]:
    decoded = unquote(url)
    lower = decoded.lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", lower)
    return set(t for t in cleaned.split() if t)


def classify_by_url(url: str) -> str:
    if REQUIRE_PATH_CONTAINS and REQUIRE_PATH_CONTAINS not in url:
        return "Herrenfussball"
    tokens = tokenize(url)
    has_women_context = bool(tokens & OUTLET_WOMEN)
    if has_women_context and (tokens & (SOCCER_TOKENS | WOMEN_SOCCER_HINTS | WOMEN_NAMES)):
        return "Frauenfussball"
    if tokens & EXCLUDE_FRAU:
        return "Herrenfussball"
    if tokens & MEN_TOKENS:
        return "Herrenfussball"
    if tokens & WOMEN_NAMES and has_women_context:
        return "Frauenfussball"
    return "Herrenfussball"


def matches_rules(url: str) -> bool:
    if REQUIRE_PATH_CONTAINS and REQUIRE_PATH_CONTAINS not in url:
        return False
    tokens = tokenize(url)
    if tokens & EXCLUDE_FRAU:
        return True
    return True


def year_month_from_lastmod(lastmod: str):
    if not lastmod:
        return None, None
    text = lastmod.strip()
    if len(text) >= 7 and text[:4].isdigit():
        try:
            return int(text[:4]), int(text[5:7])
        except ValueError:
            pass
    try:
        dt = parsedate_to_datetime(text)
        return dt.year, dt.month
    except Exception:
        pass
    try:
        iso = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        return dt.year, dt.month
    except Exception:
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
            loc_els = sm.findall("{*}loc")
            last_el = sm.find("{*}lastmod")
            if not loc_els:
                continue
            for loc_el in loc_els:
                if loc_el is None or not loc_el.text:
                    continue
                items.append((loc_el.text.strip(), last_el.text.strip() if last_el is not None and last_el.text else ""))
        return "index", items
    if tag.endswith("urlset"):
        items = []
        for u in root.findall(".//{*}url"):
            loc_el = u.find("{*}loc")
            last_el = u.find("{*}lastmod")
            pub_el = u.find(".//{*}publication_date")
            if loc_el is None or not loc_el.text:
                continue
            if last_el is not None and last_el.text:
                last_text = last_el.text.strip()
            elif pub_el is not None and pub_el.text:
                last_text = pub_el.text.strip()
            else:
                last_text = ""
            items.append((loc_el.text.strip(), last_text))
        return "urlset", items
    return "unknown", []

def parse_rss_or_atom(xml_bytes: bytes):
    root = ET.fromstring(xml_bytes)
    tag = root.tag.lower()
    items = []
    if tag.endswith("rss") or tag.endswith("rdf"):
        for item in root.findall(".//item"):
            link_el = item.find("link")
            pub_el = item.find("pubDate")
            if link_el is None or not link_el.text:
                continue
            items.append((link_el.text.strip(), pub_el.text.strip() if pub_el is not None and pub_el.text else ""))
        return items
    if tag.endswith("feed"):
        for entry in root.findall(".//{*}entry"):
            link_el = entry.find("{*}link")
            updated_el = entry.find("{*}updated")
            published_el = entry.find("{*}published")
            href = link_el.get("href") if link_el is not None else None
            if not href:
                continue
            date_text = ""
            if published_el is not None and published_el.text:
                date_text = published_el.text.strip()
            elif updated_el is not None and updated_el.text:
                date_text = updated_el.text.strip()
            items.append((href.strip(), date_text))
        return items
    return items



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


def discover_rss_feeds():
    feeds = set(RSS_FEEDS)
    for url in RSS_FEED_DISCOVERY_URLS:
        try:
            html = fetch(url).decode("utf-8", errors="ignore")
        except Exception:
            continue
        for m in re.findall(r"https?://[^\s\"']+", html):
            if "rss" in m or "feed" in m:
                feeds.add(m.rstrip(")\"'"))
    return sorted(feeds)


def main():
    last_print = 0.0
    sitemaps = list_sitemaps()
    total_sitemaps = len(sitemaps)
    rss_feeds = discover_rss_feeds()
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


    if rss_feeds:
        for i, feed_url in enumerate(rss_feeds, start=1):
            try:
                xml = fetch(feed_url)
                entries = parse_rss_or_atom(xml)
            except Exception:
                continue
            for loc, lastmod in entries:
                if not matches_rules(loc):
                    continue
                matched_urls += 1

                y, m = year_month_from_lastmod(lastmod)
                if y is None:
                    y, m = year_month_from_url(loc)
                if y is None:
                    continue

                if y < START_YEAR or y > END_YEAR:
                    continue

                rows.append({
                    "year": y,
                    "month": m or "",
                    "lastmod": lastmod,
                    "url": loc,
                    "category": classify_by_url(loc)
                })
            last_print = print_status(
                f"RSS Feeds: {i}/{len(rss_feeds)} | Matches: {matched_urls} | Rows: {len(rows)}",
                last_print,
            )
            time.sleep(REQUEST_DELAY)

    if total_sitemaps:
        print(f"Sitemaps: {total_sitemaps}/{total_sitemaps} | Matches: {matched_urls} | Rows: {len(rows)}")
    if rss_feeds:
        print(f"RSS Feeds: {len(rss_feeds)}/{len(rss_feeds)} | Matches: {matched_urls} | Rows: {len(rows)}")

    counts = {}
    for r in rows:
        y = r["year"]
        c = r["category"]
        counts.setdefault(y, {"Frauenfussball": 0, "Herrenfussball": 0, "Total": 0})
        counts[y][c] += 1
        counts[y]["Total"] += 1

    urls_csv = f"espn_fussball_urls_{START_YEAR}_{END_YEAR}.csv"
    counts_csv = f"espn_fussball_counts_{START_YEAR}_{END_YEAR}.csv"

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
