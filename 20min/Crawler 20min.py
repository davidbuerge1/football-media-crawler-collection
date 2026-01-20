import re
import time
import csv
import os
import sys
import requests
import xml.etree.ElementTree as ET

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from classify_rules import classify_url

# --- Settings ---
START_YEAR = 2005
END_YEAR = 2025

SITEMAP_INDEX = "https://www.20min.ch/sitemaps/de/articles.xml"
FOOTBALL_MARKER = "fussball"

WOMEN_KW = [
    "frauen", "women", "wsl", "frauennati", "frauen-nati",
    "frauenfussball", "frauen-bundesliga", "dfb-frauen",
    "uefa-frauen", "fifa-frauen"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Maturaarbeit; contact: your-email@example.com)"
}

REQUEST_DELAY = 0.2
MAX_SITEMAPS = 20000


def classify_by_url(url: str) -> str:
    return classify_url(url, "20min")


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
    m = re.search(r"/(19\d{2}|20\d{2})-\d{2}-\d{2}\.xml", url)
    return int(m.group(1)) if m else None


def month_from_sitemap_url(url: str):
    m = re.search(r"/(19\d{2}|20\d{2})-(\d{2})-\d{2}\.xml", url)
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
    if typ != "index":
        raise RuntimeError("Root sitemap is not an index.")
    count = 0
    for loc, lastmod in items:
        y = year_from_sitemap_url(loc) or (year_month_from_lastmod(lastmod)[0] if lastmod else None)
        if y is None or y < START_YEAR or y > END_YEAR:
            continue
        yield loc
        count += 1
        if count >= MAX_SITEMAPS:
            break


def main():
    rows = []
    for sm_url in iter_sitemaps():
        try:
            xml = fetch(sm_url)
            typ, entries = parse_sitemap(xml)
        except Exception as e:
            print(f"[WARN] Failed sitemap: {sm_url} -> {e}")
            continue

        if typ != "urlset":
            continue

        for loc, lastmod in entries:
            low = loc.lower()
            if FOOTBALL_MARKER not in low:
                continue

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

        time.sleep(REQUEST_DELAY)

    counts = {}
    for r in rows:
        y = r["year"]
        c = r["category"]
        counts.setdefault(y, {"Frauenfussball": 0, "Herrenfussball": 0, "Total": 0})
        counts[y][c] += 1
        counts[y]["Total"] += 1

    urls_csv = f"20min_fussball_urls_{START_YEAR}_{END_YEAR}.csv"
    counts_csv = f"20min_fussball_counts_{START_YEAR}_{END_YEAR}.csv"

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
