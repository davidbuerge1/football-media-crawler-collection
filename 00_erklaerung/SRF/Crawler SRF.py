import re
import time
import csv
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

# --- Settings ---
START_YEAR = 2005
END_YEAR = 2025

SITEMAP_TEMPLATE = "https://www.srf.ch/sitemaps/aron/articles/{year}_{month:02d}.xml"
FOOTBALL_PREFIX = "https://www.srf.ch/sport/fussball/"

# Keywords 
WOMEN_KW = [
    "frauen", "woman", "women", "lionesses", "frauen-nati", "frauennati",
    "frauenfussball", "frauenfußball", "axa-women", "womens", "women-s",
    "frauen-super-league", "women-s-super-league", "wsl"
]

MEN_HINTS = [
    "super-league", "champions-league", "europa-league", "nati", "bundesliga",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Maturaarbeit)"
}

def classify_by_url(url: str) -> str:
    u = url.lower()
    if any(k in u for k in WOMEN_KW):
        return "Frauenfussball"
    # Default-Regel: Fussball-URL ohne Frauen-Keywords => Herrenfussball
    return "Herrenfussball"

def fetch_sitemap(year: int, month: int) -> str:
    url = SITEMAP_TEMPLATE.format(year=year, month=month)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def parse_sitemap(xml_text: str):
    """
    Returns list of tuples: (loc, lastmod)
    """
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    root = ET.fromstring(xml_text)
    out = []
    for url_el in root.findall("sm:url", ns):
        loc_el = url_el.find("sm:loc", ns)
        lastmod_el = url_el.find("sm:lastmod", ns)
        if loc_el is None:
            continue
        loc = loc_el.text.strip()
        lastmod = lastmod_el.text.strip() if lastmod_el is not None and lastmod_el.text else ""
        out.append((loc, lastmod))
    return out

rows = []

for year in range(START_YEAR, END_YEAR + 1):
    for month in range(1, 13):
        try:
            xml_text = fetch_sitemap(year, month)
            entries = parse_sitemap(xml_text)
        except Exception as e:
            print(f"[WARN] {year}-{month:02d}: {e}")
            continue

        for loc, lastmod in entries:
            if not loc.startswith(FOOTBALL_PREFIX):
                continue

            category = classify_by_url(loc)
            rows.append({
                "year": year,
                "month": month,
                "lastmod": lastmod,
                "url": loc,
                "category": category
            })

        # freundlich crawlen
        time.sleep(0.8)

# --- Auswertung ---
counts = {}
for r in rows:
    y = r["year"]
    c = r["category"]
    counts.setdefault(y, {"Frauenfussball": 0, "Herrenfussball": 0, "Total": 0})
    counts[y][c] += 1
    counts[y]["Total"] += 1

# CSV 1: alle Fussball-URLs
with open("srf_fussball_urls_2005_2025.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["year", "month", "lastmod", "category", "url"])
    w.writeheader()
    w.writerows(rows)

# CSV 2: Jahresvergleich
with open("srf_fussball_counts_2005_2025.csv", "w", newline="", encoding="utf-8") as f:
    fieldnames = ["year", "Frauenfussball", "Herrenfussball", "Total", "Frauen_Anteil"]
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    for y in sorted(counts.keys()):
        total = counts[y]["Total"]
        women = counts[y]["Frauenfussball"]
        women_share = (women / total) if total else 0.0
        w.writerow({
            "year": y,
            "Frauenfussball": women,
            "Herrenfussball": counts[y]["Herrenfussball"],
            "Total": total,
            "Frauen_Anteil": round(women_share, 4)
        })

print("Fertig.")
print("Export: srf_fussball_urls_2005_2025.csv")
print("Export: srf_fussball_counts_2005_2025.csv")
