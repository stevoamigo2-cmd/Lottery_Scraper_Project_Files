#!/usr/bin/env python3
# lottery_hot_numbers_with_firestore.py
# Scrape multiple National Lottery & other draw-history pages, compute "hot" numbers,
# and save results to Firestore. Designed to run inside CI (GitHub Actions)
# or locally. Uses firebase-admin.

import os
import json
import io
import re
import csv
import time
from datetime import datetime, timedelta
from collections import Counter

import requests
from bs4 import BeautifulSoup

# firebase imports
import firebase_admin
from firebase_admin import credentials, firestore

# ------------ Config ------------
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LotteryHotBot/1.0)"}
DAYS_BACK = int(os.environ.get("DAYS_BACK", "60"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "15"))

# NOTE: csv_url values are examples — confirm working CSV download links for your target host.
# Many official lottery sites or state lottery portals expose CSV downloads, but paths can change

LOTTERIES = {
    "euromillions": {
        "html_url": "https://www.national-lottery.co.uk/results/euromillions/draw-history",
        "csv_url":  "https://www.national-lottery.co.uk/results/euromillions/draw-history/csv",
        "page_id": "euromillions",
    },
    "lotto": {
        "html_url": "https://www.national-lottery.co.uk/results/lotto/draw-history",
        "csv_url":  "https://www.national-lottery.co.uk/results/lotto/draw-history/csv",
        "page_id": "lotto",
    },
    "thunderball": {
        "html_url": "https://www.national-lottery.co.uk/results/thunderball/draw-history",
        "csv_url":  "https://www.national-lottery.co.uk/results/thunderball/draw-history/csv",
        "page_id": "thunderball",
    },
    "set-for-life": {
        "html_url": "https://www.national-lottery.co.uk/results/set-for-life/draw-history",
        "csv_url":  "https://www.national-lottery.co.uk/results/set-for-life/draw-history/csv",
        "page_id": "set-for-life",
    },
    # Third-party / state CSV examples for US multi-jurisdiction games:
    "megamillions": {
        "html_url": "https://www.megamillions.com/winning-numbers/previous-drawings.aspx",
        # Example CSV from a state portal — replace if invalid for your environment.
        "csv_url": "https://www.texaslottery.com/export/sites/lottery/Games/Mega_Millions/Winning_Numbers/megamillions.csv",
        "page_id": "megamillions",
    },
    "powerball": {
        "html_url": "https://www.powerball.com/previous-results",
        # Example CSV from a state portal — replace if invalid for your environment.
        "csv_url": "https://www.texaslottery.com/export/sites/lottery/Games/Powerball/Winning_Numbers/powerball.csv",
        "page_id": "powerball",
    },
    "euromillions-hotpicks": {
        "html_url": "https://www.national-lottery.co.uk/results/euromillions-hotpicks/draw-history",
        "csv_url": None,
        "page_id": "euromillions-hotpicks",
    },
    "lotto-hotpicks": {
        "html_url": "https://www.national-lottery.co.uk/results/lotto-hotpicks/draw-history",
        "csv_url": None,
        "page_id": "lotto-hotpicks",
    },
    "spain_loterias_sheet": {
        "html_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vTov1BuA0nkVGTS48arpPFkc9cG7B40Xi3BfY6iqcWTrMwCBg5b50-WwvnvaR6mxvFHbDBtYFKg5IsJ/pub?gid=1",
        "csv_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vTov1BuA0nkVGTS48arpPFkc9cG7B40Xi3BfY6iqcWTrMwCBg5b50-WwvnvaR6mxvFHbDBtYFKg5IsJ/pub?gid=1&single=true&output=csv",
        "page_id": "spain_loterias",
        "note": "Public Google Sheet published as CSV — parser now recognizes FECHA/COMBINACIÓN GANADORA/COMP./R./JOKER"
    },
    "south_africa_lotto": {
        "html_url": "https://www.africanlottery.net/lotto-results",  # optional, can be used for HTML fallback
        "csv_url": "https://www.africanlottery.net/download/sa_lotto.csv",
        "page_id": "sa_lotto",
        "note": "Official CSV download for South Africa Lotto."
    },
    "ghana_fortune_thursday": {
        "html_url": "https://lotteryguru.com/ghana-lottery-results/gh-fortune-thursday/gh-fortune-thursday-results-history",
        "csv_url": None,
        "page_id": "ghana_fortune_thursday",
        "note": "Scraped from LotteryGuru history page; parsed into 5-number draws."
    },
    "australia_powerball": {
        "html_url": "https://www.lotterywest.wa.gov.au/games/powerball",
        "csv_url": "https://api.lotterywest.wa.gov.au/api/v1/games/5132/results-csv",
        "page_id": "australia_powerball",
        "note": "Official CSV endpoint (Lotterywest API).",
        "source": "Lotterywest download page / API."
    }
}

GAME_SPECS = {
    "australia_powerball": {"main": 7, "bonus": 1},
    "powerball": {"main": 5, "bonus": 1},
    "megamillions": {"main": 5, "bonus": 1},
    "euromillions": {"main": 5, "bonus": 2},
    "lotto": {"main": 6, "bonus": 0},
    "thunderball": {"main": 5, "bonus": 1},
    "set-for-life": {"main": 5, "bonus": 1},
    "powerball_au": {"main": 7, "bonus": 1},
    "spain_loterias": {"main": 6, "bonus": 2},
    "south_africa_lotto": {"main": 6, "bonus": 1},
    "ghana_fortune_thursday": {"main": 5, "bonus": 0},
    "ghanafortunethursday": {"main": 5, "bonus": 0},
    "gh-fortune-thursday": {"main": 5, "bonus": 0},
}

# enforce per-game numeric ranges for parsed balls (module-level)
GAME_RANGES = {
    "australia_powerball": {"main_max": 35, "bonus_max": 20},
    "powerball": {"main_max": 69, "bonus_max": 26},
    "megamillions": {"main_max": 70, "bonus_max": 25},
    "spain_loterias": {"main_max": 49, "bonus_max": 9},
}

# Per-game override for how many "top" items to return
HOT_TOP_N = {
    "australia_powerball": {"top_main": 10, "top_bonus": 10},
}


# ------------ Helpers ------------
def fetch_url(url, extra_headers=None):
    headers = dict(HEADERS)
    if extra_headers:
        headers.update(extra_headers)
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text


def fetch_soup(url):
    txt = fetch_url(url)
    return BeautifulSoup(txt, "html.parser")


def extract_numbers_from_span(text):
    nums = re.findall(r'\d{1,2}', text)
    return [int(n) for n in nums]


def try_parse_date_any(text):
    text = (text or "").strip()
    if not text:
        return None
    text = re.sub(r'(?i)draw date[:\s]*', '', text).strip()

    fmts = (
        "%d %b %Y", "%d %B %Y", "%d/%m/%Y", "%Y-%m-%d",
        "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y",
        "%b %d, %Y", "%B %d, %Y",
        "%a %d %b %Y", "%A %d %B %Y",
        "%a, %b %d, %Y", "%A, %B %d, %Y"
    )
    for fmt in fmts:
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            pass

    # try to find a date fragment inside the string
    m = re.search(r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})', text)
    if m:
        for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(m.group(1), fmt).date()
            except Exception:
                pass

    # try "MonthName day, year" inside text
    m2 = re.search(r'([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})', text)
    if m2:
        for fmt in ("%b %d, %Y", "%B %d, %Y"):
            try:
                return datetime.strptime(m2.group(1), fmt).date()
            except Exception:
                pass

    return None


def _enforce_ranges(mains, bonus, page_id=None):
    """
    Filter mains/bonus by GAME_RANGES for page_id (module-level).
    Returns filtered (mains, bonus).
    """
    ranges = GAME_RANGES.get(page_id) or {}
    if not ranges:
        return mains, bonus
    main_max = ranges.get("main_max", 9999)
    bonus_max = ranges.get("bonus_max", 9999)
    mains = [n for n in mains if isinstance(n, int) and 1 <= n <= main_max]
    bonus = [n for n in bonus if isinstance(n, int) and 1 <= n <= bonus_max]
    return mains, bonus


def _normalize_and_append(draws_list, date_obj, mains, bonus, page_id=None):
    """
    Normalize mains/bonus, enforce ranges for page_id, and append a draw dict
    onto draws_list (explicit list arg so this works in any scope).
    """
    if isinstance(mains, int):
        mains = [mains]
    if isinstance(bonus, int):
        bonus = [bonus]

    mains = [int(n) for n in mains if isinstance(n, int) and n >= 1]
    bonus = [int(n) for n in bonus if isinstance(n, int) and n >= 1]

    mains, bonus = _enforce_ranges(mains, bonus, page_id)
    draws_list.append({"date": date_obj.isoformat(), "main": mains, "bonus": bonus})


# --- original (kept) scrape_html for fallback ---
def scrape_html(draw_cfg):
    """
    More resilient HTML scraping fallback:
    - Try the original selector
    - If nothing found, search for list items or table rows containing a date & numbers
    """
    url = draw_cfg.get("html_url")
    if not url:
        return []
    print(f"[debug] Scrape HTML: {url}")
    soup = fetch_soup(url)

    # 1) original specific selector attempt
    selector = f"#draw_history_{draw_cfg.get('page_id')} ul.list_table_presentation"
    entries = soup.select(selector)
    draws = []
    if entries:
        for ul in entries:
            spans = ul.select("span.table_cell_block")
            if len(spans) >= 3:
                date_txt = spans[0].get_text(" ", strip=True)
                main_txt = spans[2].get_text(" ", strip=True) if len(spans) >= 3 else ""
                bonus_txt = spans[3].get_text(" ", strip=True) if len(spans) >= 4 else ""
                date_obj = try_parse_date_any(date_txt)
                if date_obj is None:
                    continue
                mains = extract_numbers_from_span(main_txt)
                bonuses = extract_numbers_from_span(bonus_txt)
                _normalize_and_append(draws, date_obj, mains, bonuses, page_id=draw_cfg.get("page_id"))
        if draws:
            return draws

    # 2) generic fallback: find any list/table rows that contain a date and some numbers
    candidates = soup.find_all(['li', 'tr', 'div'])
    for el in candidates:
        text = el.get_text(" ", strip=True)
        if not text:
            continue
        # require at least 3 numbers and a date-like substring
        if len(re.findall(r'\d{1,2}', text)) < 3:
            continue
        # try to parse date substring
        date_match = None
        m = re.search(r'(\d{1,2}\s+\w{3,9}\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\w+\s+\d{1,2},\s*\d{4})', text)
        if m:
            date_match = m.group(1)
        if not date_match:
            m2 = re.search(r'date[:\s]*([^\|\,\-]{6,40})', text, re.I)
            if m2:
                date_match = m2.group(1)
        if not date_match:
            continue
        date_obj = try_parse_date_any(date_match)
        if not date_obj:
            continue

        # extract numeric tokens (allow up to 3 digits in tokenization)
        nums = [int(x) for x in re.findall(r'\d{1,3}', text)]
        # remove any stray year tokens (simple heuristic)
        nums = [n for n in nums if n != date_obj.year]

        pid = draw_cfg.get("page_id")
        spec = GAME_SPECS.get(pid) if pid else None

        if spec:
            main_count = spec.get("main", 5)
            bonus_count = spec.get("bonus", 0)
            mains = nums[:main_count]
            bonus = nums[main_count: main_count + bonus_count]
        else:
            mains = nums[:5]
            bonus = nums[5:8]

        _normalize_and_append(draws, date_obj, mains, bonus, page_id=pid)

    print(f"[debug] scrape_html parsed draws: {len(draws)}")
    return draws


# ------------ NEW: National Lottery JSON fetcher ------------
def fetch_draws_json(draw_cfg):
    """
    Fetch draw results from likely JSON endpoints (National Lottery).
    Returns list of {"date": ISOdate, "main": [...], "bonus": [...]} newest-first
    """
    page_id = draw_cfg.get("page_id")
    if not page_id:
        return []
    candidates = []
    base = draw_cfg.get("html_url", "")
    candidates.append(f"https://www.national-lottery.co.uk/results/{page_id}/draw-history/draws")
    candidates.append(f"https://www.national-lottery.co.uk/results/{page_id}/draw-history.json")
    candidates.append(f"https://www.national-lottery.co.uk/api/results/{page_id}/draws")
    if base:
        if base.endswith("/draw-history"):
            candidates.append(base.rstrip("/") + "/draws")
            candidates.append(base.rstrip("/") + "/draw-history/draws")
        else:
            candidates.append(base.rstrip("/") + "/draw-history/draws")

    tried = set()
    draws = []
    for url in candidates:
        if not url or url in tried:
            continue
        tried.add(url)
        try:
            print(f"[debug-json] attempting {url}")
            # use a browser-like Accept/Referer for reliability
            headers = {
                "User-Agent": HEADERS.get("User-Agent"),
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Referer": draw_cfg.get("html_url") or "https://www.national-lottery.co.uk/",
            }
            r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            try:
                data = r.json()
            except Exception:
                print(f"[debug-json] not JSON at {url}")
                continue

            # locate the draws list in a few common shapes
            if isinstance(data, list):
                src = data
            elif isinstance(data, dict) and data.get("draws"):
                src = data.get("draws")
            elif isinstance(data, dict) and data.get("results"):
                src = data.get("results")
            else:
                # choose first list-like value if any
                cand = None
                if isinstance(data, dict):
                    for v in data.values():
                        if isinstance(v, list):
                            cand = v
                            break
                src = cand or []

            if not src:
                print(f"[debug-json] JSON at {url} contained no candidate draw list (keys: {list(data.keys())})")
                continue

            # Normalize rows
            for row in src:
                date_obj = None
                mains = []
                bonus = []
                if isinstance(row, dict):
                    for dk in ("drawDate", "date", "draw_date", "drawdate", "publishedDate"):
                        if row.get(dk):
                            try:
                                dd = str(row.get(dk))
                                date_obj = try_parse_date_any(dd) or datetime.strptime(dd[:10], "%Y-%m-%d").date()
                                break
                            except Exception:
                                try:
                                    date_obj = try_parse_date_any(str(row.get(dk)))
                                    if date_obj:
                                        break
                                except Exception:
                                    date_obj = None
                    # numbers
                    if row.get("numbers") and isinstance(row.get("numbers"), list):
                        numbers = row.get("numbers")
                        if numbers and isinstance(numbers[0], dict):
                            flat = []
                            for n in numbers:
                                if isinstance(n, dict):
                                    for k in ("value", "number", "num"):
                                        if k in n and str(n[k]).isdigit():
                                            flat.append(int(n[k])); break
                                else:
                                    if str(n).isdigit():
                                        flat.append(int(n))
                            numbers = flat
                        mains = [int(x) for x in numbers if str(x).isdigit()]
                    elif row.get("winningNumbers") and isinstance(row.get("winningNumbers"), dict):
                        wn = row.get("winningNumbers")
                        mains = [int(x) for x in (wn.get("main") or []) if str(x).isdigit()]
                        supp = wn.get("supplementary") or wn.get("supplementaryNumbers") or wn.get("supplementary_numbers")
                        if isinstance(supp, list):
                            bonus = [int(x) for x in supp if str(x).isdigit()]
                    elif row.get("main") and isinstance(row.get("main"), list):
                        mains = [int(x) for x in row.get("main") if str(x).isdigit()]
                        if row.get("bonus") and isinstance(row.get("bonus"), list):
                            bonus = [int(x) for x in row.get("bonus") if str(x).isdigit()]
                    else:
                        # flatten numeric lists conservatively
                        flattened = []
                        for v in row.values():
                            if isinstance(v, list) and all((isinstance(x, int) or (isinstance(x, str) and x.isdigit())) for x in v):
                                flattened.extend([int(x) for x in v])
                        if flattened:
                            mains = flattened

                if not date_obj:
                    # try some alternate keys
                    for candidate_key in ("draw", "draw_date", "published_at", "publishedAt"):
                        if isinstance(row, dict) and row.get(candidate_key):
                            date_obj = try_parse_date_any(str(row.get(candidate_key)))
                            if date_obj:
                                break

                if not date_obj:
                    continue

                spec = GAME_SPECS.get(page_id) or {}
                main_count = spec.get("main")
                bonus_count = spec.get("bonus", 0)
                if main_count and len(mains) > main_count:
                    mains = mains[:main_count]
                if not bonus and main_count and len(mains) > main_count:
                    bonus = mains[main_count:main_count+bonus_count]
                    mains = mains[:main_count]

                mains, bonus = _enforce_ranges(mains, bonus, page_id)
                _normalize_and_append(draws, date_obj, mains, bonus, page_id=page_id)

            if draws:
                draws.sort(key=lambda x: x["date"], reverse=True)
                print(f"[debug-json] parsed {len(draws)} draws from {url}")
                return draws

        except Exception as e:
            print(f"[warning] fetch_draws_json failed for {url}: {e}")

    return []


# ------------ NEW: fallback to national-lottery.com (independent aggregator) ------------
def fetch_from_national_lottery_dot_com(draw_cfg):
    """
    Emergency fallback: scrape national-lottery.com results page (server-side HTML).
    Returns list of {"date": ISOdate, "main":[...], "bonus":[...]} or [].
    """
    url = "https://www.national-lottery.com/results"
    try:
        txt = fetch_url(url)
    except Exception as e:
        print(f"[fallback] failed to fetch national-lottery.com: {e}")
        return []

    soup = BeautifulSoup(txt, "html.parser")
    title_map = {
        "lotto": "Lotto",
        "euromillions": "EuroMillions",
        "thunderball": "Thunderball",
        "set-for-life": "Set For Life",
        "lotto-hotpicks": "Lotto HotPicks",
        "euromillions-hotpicks": "EuroMillions HotPicks",
    }
    want_title = title_map.get(draw_cfg.get("page_id"))
    if not want_title:
        return []

    header = None
    for tag in soup.find_all(['h1','h2','h3','h4','strong','b']):
        txt = (tag.get_text(" ", strip=True) or "").strip()
        if not txt:
            continue
        if txt.lower().startswith(want_title.lower()) or want_title.lower() in txt.lower():
            header = tag
            break
    if not header:
        return []

    nums = []
    date_obj = None
    el = header.find_next_sibling()
    steps = 0
    while el and steps < 12:
        s = el.get_text(" ", strip=True)
        if s:
            if date_obj is None:
                d = try_parse_date_any(s)
                if d:
                    date_obj = d
            found = re.findall(r'\b(\d{1,2})\b', s)
            if found:
                nums.extend([int(x) for x in found])
            if re.match(r'^(Lotto|EuroMillions|Thunderball|Set For Life|Lotto HotPicks|EuroMillions HotPicks)', s, re.I):
                break
        el = el.find_next_sibling()
        steps += 1

    if not nums:
        return []

    spec = GAME_SPECS.get(draw_cfg.get("page_id"), {})
    main_count = spec.get("main", 5)
    bonus_count = spec.get("bonus", 0)

    mains = nums[:main_count]
    bonus = nums[main_count: main_count + bonus_count]

    if not date_obj:
        date_obj = datetime.utcnow().date()

    draws = []
    _normalize_and_append(draws, date_obj, mains, bonus, page_id=draw_cfg.get("page_id"))
    print(f"[fallback] parsed {len(draws)} draw(s) from national-lottery.com for {draw_cfg.get('page_id')}")
    return draws


# ------------ CSV parser / fetcher (small tweak: send Referer where applicable) ------------
def fetch_csv(draw_cfg):
    """
    Try a series of CSV url variants and return parsed draws or [].
    Will attempt different encodings and query param variants.
    """
    csv_url = draw_cfg.get("csv_url")
    variants = []
    if csv_url:
        variants.append(csv_url)
        if "?" not in csv_url:
            variants.append(csv_url + "?draws=200")
    html = draw_cfg.get("html_url", "")
    if html:
        if html.endswith("/draw-history"):
            variants.append(html + "/csv")
            variants.append(html + "/draw-history/csv")
        else:
            variants.append(html.rstrip("/") + "/csv")
            variants.append(html.rstrip("/") + "/csv?draws=200")

    tried = set()
    for u in variants:
        if not u or u in tried:
            continue
        tried.add(u)
        try:
            print(f"[debug] Attempting CSV URL: {u}")
            # include Referer for National Lottery CSV endpoints (helps some protections)
            headers = dict(HEADERS)
            if "national-lottery.co.uk" in u:
                headers.update({"Referer": draw_cfg.get("html_url") or "https://www.national-lottery.co.uk/"})
            r = requests.get(u, headers=headers, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            enc = r.encoding or getattr(r, "apparent_encoding", None) or "utf-8"
            try:
                txt = r.content.decode(enc, errors="replace")
            except Exception:
                txt = r.content.decode("ISO-8859-1", errors="replace")
            if draw_cfg.get("page_id") == "sa_lotto":
                draws = parse_sa_lotto_csv(txt)
                print(f"[debug] fetch_csv: sa_lotto parsed {len(draws)} rows from {u}")
            else:
                draws = parse_csv_text(txt, page_id=draw_cfg.get("page_id"))

            if draws:
                print(f"[debug] CSV parsed OK from {u} (rows: {len(draws)})")
                return draws
            else:
                sample = txt.splitlines()[:8]
                print(f"[debug] CSV from {u} parsed 0 draws; sample:\n" + "\n".join(sample))
        except Exception as e:
            print(f"[warning] CSV fetch failed for {u}: {e}")
    return []


def filter_recent(draws, days_back):
    cutoff = datetime.utcnow().date() - timedelta(days=days_back)
    return [d for d in draws if datetime.fromisoformat(d["date"]).date() >= cutoff]


def compute_hot(draws, top_main_n=10, top_bonus_n=10, page_id=None):
    """
    Count mains & bonuses and return two lists sized by top_main_n / top_bonus_n.
    """
    mc = Counter()
    bc = Counter()
    ranges = GAME_RANGES.get(page_id) or {}
    main_max = ranges.get("main_max")
    bonus_max = ranges.get("bonus_max")
    for d in draws:
        mains = d.get("main", []) or []
        if isinstance(mains, int):
            mains = [mains]
        mains = [int(n) for n in mains if isinstance(n, int) and n >= 1]
        if main_max is not None:
            mains = [n for n in mains if n <= main_max]

        bonus = d.get("bonus", []) or []
        if isinstance(bonus, int):
            bonus = [bonus]
        bonus = [int(n) for n in bonus if isinstance(n, int) and n >= 1]
        if bonus_max is not None:
            bonus = [n for n in bonus if n <= bonus_max]

        mc.update(mains)
        bc.update(bonus)

    return mc.most_common(top_main_n), bc.most_common(top_bonus_n)


def init_firestore():
    """
    Initialization logic:
    - If environment variable FIREBASE_SERVICE_ACCOUNT contains the full JSON,
      use that.
    - Otherwise Fall back to GOOGLE_APPLICATION_CREDENTIALS path.
    """
    try:
        if firebase_admin._apps:
            print("[debug] firebase-admin already initialized")
            return firestore.client()
    except Exception:
        pass

    sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
    gac = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if sa_json:
        print("[debug] Initializing Firestore from FIREBASE_SERVICE_ACCOUNT env")
        sa_obj = json.loads(sa_json)
        cred = credentials.Certificate(sa_obj)
        firebase_admin.initialize_app(cred)
    elif gac and os.path.exists(gac):
        print(f"[debug] Initializing Firestore from GOOGLE_APPLICATION_CREDENTIALS: {gac}")
        cred = credentials.Certificate(gac)
        firebase_admin.initialize_app(cred)
    else:
        print("[debug] Initializing Firestore with default application credentials (ADC)")
        firebase_admin.initialize_app()
    return firestore.client()


def save_to_firestore(db, key, out):
    col = db.collection("lotteries")
    doc = col.document(key)
    doc.set(out)


# ------------ Main run ------------
def run_and_save():
    db = None
    try:
        db = init_firestore()
        print("[info] Firestore client initialized.")
    except Exception as e:
        print("[warning] Could not initialize Firestore:", e)
        db = None

    results = {}
    for key, cfg in LOTTERIES.items():
        print(f"\n== Processing {key} ==")
        draws = []
        try:
            # TRY ORDER: official JSON -> CSV -> national-lottery.com fallback -> HTML
            # 1) JSON (fast/reliable for national-lottery.co.uk games)
            try:
                draws = fetch_draws_json(cfg)
                if draws:
                    print(f"[debug] parsed draws from official JSON: {len(draws)}")
            except Exception as e:
                print(f"[warning] fetch_draws_json error for {key}: {e}")
                draws = []

            # 2) CSV (if still empty)
            if not draws and cfg.get("csv_url"):
                try:
                    draws = fetch_csv(cfg)
                    if draws:
                        print(f"[debug] parsed draws from CSV: {len(draws)}")
                except Exception as e:
                    print(f"[warning] CSV fetch/parse failed for {key}: {e}")
                    draws = []

            # 3) emergency fallback to independent site (only for UK games)
            nl_games = {"euromillions", "lotto", "thunderball", "set-for-life", "euromillions-hotpicks", "lotto-hotpicks"}
            if not draws and cfg.get("page_id") in nl_games:
                try:
                    draws = fetch_from_national_lottery_dot_com(cfg)
                    if draws:
                        print(f"[debug] parsed draws from national-lottery.com: {len(draws)}")
                except Exception as e:
                    print(f"[warning] national-lottery.com fallback failed for {key}: {e}")
                    draws = []

            # 4) lastly, fallback to HTML scraping
            if not draws:
                print("[debug] No draws found by JSON/CSV/fallback, trying HTML scraping.")
                if cfg.get("page_id") == "ghana_fortune_thursday":
                    draws = scrape_lotteryguru_fortune_thursday(cfg)
                    print(f"[debug] parsed draws from LotteryGuru: {len(draws)}")
                else:
                    draws = scrape_html(cfg)
                    print(f"[debug] parsed draws from HTML: {len(draws)}")

            recent = filter_recent(draws, DAYS_BACK)
            print(f"[debug] recent draws (last {DAYS_BACK} days): {len(recent)}")

            cfg_hot = HOT_TOP_N.get(key, {})
            top_main_n = cfg_hot.get("top_main", 10)
            top_bonus_n = cfg_hot.get("top_bonus", 10)

            top_main, top_bonus = compute_hot(recent,
                                              top_main_n=top_main_n,
                                              top_bonus_n=top_bonus_n,
                                              page_id=key)

            out = {
                "fetched_at": datetime.utcnow().isoformat() + "Z",
                "draws_total": len(draws),
                "draws_recent": len(recent),
                "top_main": [{"number": n, "count": c} for n, c in top_main],
                "top_bonus": [{"number": n, "count": c} for n, c in top_bonus],
            }
            results[key] = out

            # local JSON save
            fname = f"{key}_hot.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2)
            print(f"[debug] Saved {fname}")

            # save to Firestore if available
            if db is not None:
                try:
                    save_to_firestore(db, key, out)
                    print(f"[info] Written {key} => Firestore/lotteries/{key}")
                except Exception as e:
                    print(f"[warning] Firestore write failed for {key}: {e}")

        except Exception as e:
            print(f"[error] {key} failed: {e}")

    return results


if __name__ == "__main__":
    run_and_save()
