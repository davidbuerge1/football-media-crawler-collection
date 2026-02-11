import csv
import re
import sys
from pathlib import Path

ROOT = Path(r"C:\Users\David\Documents\Crawler")

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

GERMAN_WOMEN = {
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

GERMAN_MEN = {
    "bundesliga",
    "2-bundesliga",
    "dritte-liga",
    "dfb-pokal",
    "champions-league",
    "europa-league",
    "conference-league",
}

ENGLISH_WOMEN = {
    "women",
    "womens",
    "wsl",
    "nwsl",
    "womens-super-league",
    "women-s-super-league",
    "lionesses",
    "matildas",
}

ENGLISH_MEN = {
    "premier-league",
    "champions-league",
    "europa-league",
    "fa-cup",
    "carabao-cup",
}

FRENCH_WOMEN = {
    "feminin",
    "feminine",
    "feminines",
    "football-feminin",
    "equipe-de-france-feminine",
    "d1-arkema",
    "division-1-feminine",
}

FRENCH_MEN = {
    "ligue-1",
    "ligue-2",
    "coupe-de-france",
}

ITALIAN_WOMEN = {
    "femminile",
    "femminili",
    "calciofemminile",
    "nazionale-femminile",
    "serie-a-femminile",
}

ITALIAN_MEN = {
    "serie-a",
    "serie-b",
    "coppa-italia",
    "supercoppa",
}

OUTLET_RULES = {
    "Spiegel": {"women": GERMAN_WOMEN, "men": GERMAN_MEN},
    "SRF": {"women": GERMAN_WOMEN, "men": GERMAN_MEN},
    "20min": {"women": GERMAN_WOMEN, "men": GERMAN_MEN},
    "Watson": {"women": GERMAN_WOMEN | ENGLISH_WOMEN, "men": GERMAN_MEN | ENGLISH_MEN},
    "LeMonde": {"women": FRENCH_WOMEN, "men": FRENCH_MEN},
    "LeFigaro": {"women": FRENCH_WOMEN, "men": FRENCH_MEN},
    "Reppubblica": {"women": ITALIAN_WOMEN, "men": ITALIAN_MEN},
}


def tokenize(url: str) -> set[str]:
    lower = url.lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", lower)
    return set(t for t in cleaned.split() if t)


def classify_url(url: str, outlet: str) -> str:
    tokens = tokenize(url)
    rules = OUTLET_RULES.get(outlet, {"women": set(), "men": set()})
    women_tokens = rules["women"] | WOMEN_NAMES
    men_tokens = rules["men"] | MEN_NAMES

    if tokens & women_tokens:
        return "Frauenfussball"
    if tokens & EXCLUDE_FRAU:
        return "Herrenfussball"
    if tokens & men_tokens:
        return "Herrenfussball"
    return "Herrenfussball"


def reclassify_file(csv_path: Path, outlet: str):
    out_urls = csv_path.with_name(csv_path.stem + "_reclassified.csv")
    counts_name = csv_path.stem.replace("_urls_", "_counts_") + "_reclassified.csv"
    out_counts = csv_path.with_name(counts_name)

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    if "category" not in fieldnames:
        fieldnames.append("category")

    counts = {}
    for row in rows:
        url = row.get("url", "")
        new_cat = classify_url(url, outlet)
        row["category"] = new_cat
        year = row.get("year", "")
        if year:
            y = int(year)
            counts.setdefault(y, {"Frauenfussball": 0, "Herrenfussball": 0, "Total": 0})
            counts[y][new_cat] += 1
            counts[y]["Total"] += 1

    with out_urls.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    with out_counts.open("w", encoding="utf-8", newline="") as f:
        fieldnames_counts = ["year", "Frauenfussball", "Herrenfussball", "Total", "Frauen_Anteil"]
        writer = csv.DictWriter(f, fieldnames=fieldnames_counts)
        writer.writeheader()
        for y in sorted(counts.keys()):
            total = counts[y]["Total"]
            women = counts[y]["Frauenfussball"]
            writer.writerow(
                {
                    "year": y,
                    "Frauenfussball": women,
                    "Herrenfussball": counts[y]["Herrenfussball"],
                    "Total": total,
                    "Frauen_Anteil": round((women / total) if total else 0.0, 4),
                }
            )

    return out_urls, out_counts


def print_progress(done: int, total: int, label: str):
    if total <= 0:
        return
    width = 30
    filled = int((done / total) * width)
    bar = "#" * filled + "-" * (width - filled)
    sys.stdout.write(f"\r[{bar}] {done}/{total} {label}")
    sys.stdout.flush()
    if done == total:
        sys.stdout.write("\n")


def main():
    targets = []
    for outlet, _rules in OUTLET_RULES.items():
        outlet_dir = ROOT / outlet
        if not outlet_dir.exists():
            continue
        for csv_path in outlet_dir.glob("*_urls_*.csv"):
            if "reclassified" in csv_path.name:
                continue
            targets.append((csv_path, outlet))

    if not targets:
        print("No URL CSVs found.")
        return

    total = len(targets)
    done = 0
    for csv_path, outlet in targets:
        done += 1
        print_progress(done, total, f"{outlet}:{csv_path.name}")
        out_urls, out_counts = reclassify_file(csv_path, outlet)
        print(f"[OK] {out_urls}")
        print(f"[OK] {out_counts}")


if __name__ == "__main__":
    main()


