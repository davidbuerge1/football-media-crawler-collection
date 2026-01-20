import re

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
    # International (more unique)
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


def matches_rules(url: str, outlet: str) -> bool:
    tokens = tokenize(url)
    rules = OUTLET_RULES.get(outlet, {"women": set(), "men": set()})
    women_tokens = rules["women"] | WOMEN_NAMES
    men_tokens = rules["men"] | MEN_NAMES
    return bool(tokens & (women_tokens | men_tokens | EXCLUDE_FRAU))


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
