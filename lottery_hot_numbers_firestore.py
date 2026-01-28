
#!/usr/bin/env python3
# lottery_hot_numbers_with_firestore_patched_api.py
# Original script updated to use National Lottery API download endpoints
# when the old /draw-history/csv paths are no longer valid.
#
# Behavior:
# - For each LOTTERIES entry, try csv_url first (unchanged).
# - If csv_url fails or is invalid for National Lottery pages, attempt to use
#   the National Lottery API endpoint discovered from the page or provided
#   via "api_game_id" in the lottery config.
# - If API fails, fall back to HTML scraping as before.
#
# Note: You may want to set X_DFE_DEVICE_ID environment variable if your
# requests require the same device id observed in browser DevTools.

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

# Optional: set this to the x-dfe-device-id value observed in browser requests
X_DFE_DEVICE_ID = os.environ.get("X_DFE_DEVICE_ID", None)

# NOTE: csv_url values are examples — confirm working CSV download links for your target host.
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
        # If you already know the API game id (from DevTools), set it here:
        # "api_game_id": 33,
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

# National Lottery API base (observed)
API_BASE_NATIONAL_LOTTERY = "https://api-dfe.national-lottery.co.uk"

# ------------ Helpers ------------
def fetch_url(url, headers=None, timeout=REQUEST_TIMEOUT):
    hdrs = dict(HEADERS)
    if headers:
        hdrs.update(headers)
    r = requests.get(url, headers=hdrs, timeout=timeout)
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


def parse_csv_text(csv_text, page_id=None):
    """
    Robust CSV parser:
    - Force-detect delimiter by inspecting the first non-empty line with common delimiters.
    - Prefer exact 'Winning Number N' + 'Powerball' columns when present.
    - Use strict numeric extraction for balls (\b\d{1,2}\b).
    - Enforce GAME_RANGES for the provided page_id on every path.
    Returns list of {"date": ISOdate, "main": [...], "bonus": [...]}
    """
    if not csv_text:
        return []

    csv_text = csv_text.lstrip('\ufeff\ufeff')
    lines = [ln for ln in csv_text.splitlines() if ln.strip()]
    sample = "\n".join(lines[:40]) if lines else csv_text[:4096]

    # --- robust delimiter detection: check header line with common delimiters ---
    first_line = lines[0] if lines else ""
    candidate_delims = [",", "\t", ";"]
    chosen_delim = None
    for delim in candidate_delims:
        parts = [p.strip().lower() for p in first_line.split(delim)]
        if any(("winning number" in p or "powerball" in p or "draw date" in p or "draw number" in p or p == "draw") for p in parts):
            chosen_delim = delim
            break
    if not chosen_delim:
        try:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            chosen_delim = dialect.delimiter
        except Exception:
            chosen_delim = "\t" if "\t" in sample else ","

    delimiter = chosen_delim

    # strict ball-extraction regex (word-boundary 1-2 digits)
    ball_re = re.compile(r'\b(\d{1,2})\b')

    draws = []

    # Try DictReader first (clean headered CSVs)
    f = io.StringIO(csv_text)
    reader = csv.DictReader(f, delimiter=delimiter)
    fieldnames = reader.fieldnames or []
    fn_lower = " ".join([(fn or "").lower() for fn in fieldnames])

    # --- special-case explicit Winning Number columns (Australia-style CSVs) ---
    if fieldnames and ("winning number" in fn_lower or "powerball" in fn_lower):
        f2 = io.StringIO(csv_text)
        reader2 = csv.DictReader(f2, delimiter=delimiter)
        for row in reader2:
            # find date column
            date_str = None
            for k in row:
                if k and any(tok in (k or "").lower() for tok in ("date", "draw date", "fecha", "draw")):
                    date_str = (row[k] or "").strip()
                    break
            if not date_str:
                continue
            date_obj = try_parse_date_any(date_str)
            if not date_obj:
                continue

            # collect Winning Number N columns in index order
            mains = []
            bonus = []
            win_cols = []
            for k in row.keys():
                if not k:
                    continue
                kl = k.lower()
                m = re.match(r'winning\s*number\s*(\d+)', kl)
                if m:
                    try:
                        idx = int(m.group(1))
                    except Exception:
                        idx = 0
                    win_cols.append((idx, k))
            win_cols.sort(key=lambda x: x[0])
            for idx, col in win_cols:
                v = (row.get(col) or "").strip()
                mnum = ball_re.search(v)
                if mnum:
                    try:
                        mains.append(int(mnum.group(1)))
                    except Exception:
                        pass

            # powerball (or equivalent) column
            pb_col = None
            for k in row.keys():
                if k and 'powerball' in k.lower():
                    pb_col = k
                    break
            if pb_col:
                v = (row.get(pb_col) or "").strip()
                mnum = ball_re.search(v)
                if mnum:
                    try:
                        bonus.append(int(mnum.group(1)))
                    except Exception:
                        pass

            # enforce ranges and append
            mains, bonus = _enforce_ranges(mains, bonus, page_id)
            _normalize_and_append(draws, date_obj, mains, bonus, page_id=page_id)

        if draws:
            return draws

    # --- Spanish-sheet or row-oriented (first col = date, rest numbers) ---
    f_rows = io.StringIO(csv_text)
    reader_rows = csv.reader(f_rows, delimiter=delimiter)
    all_rows = [r for r in reader_rows if any((c or "").strip() for c in r)]
    if all_rows:
        header = all_rows[0]
        header_lower = " ".join([(h or "").lower() for h in header])
        if "fecha" in header_lower or "combin" in header_lower or (sum(1 for h in header if not h or h.strip() == "") > 2):
            data_rows = all_rows[1:]
            for row in data_rows:
                if not row:
                    continue
                date_str = (row[0] or "").strip()
                date_obj = try_parse_date_any(date_str)
                if not date_obj:
                    joined = " ".join(row)
                    m = re.search(r'(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})', joined)
                    if m:
                        date_obj = try_parse_date_any(m.group(1))
                    if not date_obj:
                        continue
                tail = [c.strip() for c in row[1:] if c is not None and str(c).strip() != ""]
                # drop trailing long Joker-like numeric token
                if tail and re.fullmatch(r'\d{6,}', tail[-1]):
                    tail = tail[:-1]
                nums = []
                for v in tail:
                    for mm in ball_re.findall(v):
                        nums.append(int(mm))
                mains = nums[:6] if len(nums) >= 6 else nums
                bonus = nums[6:8] if len(nums) > 6 else []
                mains, bonus = _enforce_ranges(mains, bonus, page_id)
                _normalize_and_append(draws, date_obj, mains, bonus, page_id=page_id)

            if draws:
                return draws

    # --- headerless / tokenized fallback (space-separated etc.) ---
    f3 = io.StringIO(csv_text)
    reader3 = csv.reader(f3, delimiter=delimiter)
    for raw_row in reader3:
        if not raw_row:
            continue
        if len(raw_row) == 1:
            tokens = re.split(r'\s+', raw_row[0].strip())
        else:
            tokens = [c.strip() for c in raw_row if c is not None and str(c).strip() != ""]
        if not tokens:
            continue

        # find date index (three consecutive numeric tokens that look like a date)
        date_idx = None
        month = day = year = None
        for i in range(len(tokens) - 2):
            if tokens[i].isdigit() and tokens[i+1].isdigit() and tokens[i+2].isdigit():
                try:
                    a, b, c = int(tokens[i]), int(tokens[i+1]), int(tokens[i+2])
                except Exception:
                    continue
                if 1900 <= c <= 2100 and 1 <= a <= 12 and 1 <= b <= 31:
                    date_idx = i
                    month, day, year = a, b, c
                    break
                if 1900 <= c <= 2100 and 1 <= b <= 12 and 1 <= a <= 31:
                    date_idx = i
                    month, day, year = b, a, c
                    break

        if date_idx is not None:
            game_raw = " ".join(tokens[:date_idx]).lower()
            game = re.sub(r'[\s\-_]', '', game_raw)

            try:
                date_obj = datetime(year, month, day).date()
            except Exception:
                continue

            numeric_tail = tokens[date_idx+3:]
            numbers = []
            for n in numeric_tail:
                for mm in ball_re.findall(str(n)):
                    numbers.append(int(mm))

            spec = None
            for k in GAME_SPECS:
                if game.startswith(k):
                    spec = GAME_SPECS[k]
                    break

            if spec:
                main_count = spec.get("main", 5)
                mains = numbers[:main_count]
                bonus = numbers[main_count:]
            else:
                if len(numbers) == 6:
                    mains = numbers[:5]; bonus = numbers[5:]
                elif len(numbers) == 7:
                    mains = numbers[:5]; bonus = numbers[5:]
                elif len(numbers) == 8:
                    mains = numbers[:7]; bonus = numbers[7:]
                else:
                    mains = numbers[:5]; bonus = numbers[5:]

            mains, bonus = _enforce_ranges(mains, bonus, page_id)
            _normalize_and_append(draws, date_obj, mains, bonus, page_id=page_id)
            continue

        # last-resort: find a date snippet and extract last numeric tokens (strict 1-2 digit tokens)
        joined = " ".join(tokens)
        date_obj = None
        m = re.search(r'(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})', joined)
        if m:
            for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
                try:
                    date_obj = datetime.strptime(m.group(1), fmt).date()
                    break
                except Exception:
                    pass
        if not date_obj:
            parts = tokens[:4]
            try:
                if len(parts) >= 3 and all(p.isdigit() for p in parts[:3]):
                    try:
                        date_obj = datetime(int(parts[2]), int(parts[0]), int(parts[1])).date()
                    except Exception:
                        date_obj = datetime(int(parts[0]), int(parts[1]), int(parts[2])).date()
            except Exception:
                date_obj = None
        if not date_obj:
            continue
        nums = ball_re.findall(joined)
        if len(nums) >= 6:
            numbers = [int(x) for x in nums[-8:]]
            if len(numbers) >= 6:
                mains = numbers[:5]
                bonus = numbers[5:]
                mains, bonus = _enforce_ranges(mains, bonus, page_id)
                _normalize_and_append(draws, date_obj, mains, bonus, page_id=page_id)

    # final small dd.mm.YYYY style fallback (keeps your original behavior)
    if not draws and lines and re.search(r'\d{1,2}\.\d{1,2}\.\d{4}', lines[0]):
        for line in lines:
            parts = re.split(r'[\t,; ]+', line.strip())
            if len(parts) < 8:
                continue
            date_match = re.search(r'\d{1,2}\.\d{1,2}\.\d{4}', line)
            if not date_match:
                continue
            date_str = date_match.group(0)
            date_obj = try_parse_date_any(date_str)
            if not date_obj:
                continue
            nums = [int(x) for x in parts if re.match(r'^\d+$', x)]
            mains, bonus = nums[:6], nums[6:7]
            mains, bonus = _enforce_ranges(mains, bonus, page_id)
            _normalize_and_append(draws, date_obj, mains, bonus, page_id=page_id)

    return draws


def scrape_lotteryguru_fortune_thursday(draw_cfg, days_back=DAYS_BACK):
    """
    Robust LotteryGuru Fortune Thursday scraper with pagination.
    Returns list of {"date": ISOdate, "main": [...], "bonus": []}, newest-first.
    Fetches pages until we reach the cutoff (days_back) or lastPage.
    """
    base_url = draw_cfg.get("html_url")
    if not base_url:
        return []
    print(f"[debug] scrape_lotteryguru_fortune_thursday: base_url={base_url}")

    draws = []
    session = requests.Session()
    session.headers.update(HEADERS)

    # cutoff date (inclusive)
    cutoff = datetime.utcnow().date() - timedelta(days=days_back)

    # helper to parse a single page
    def parse_page(html):
        page_draws = []
        soup = BeautifulSoup(html, "html.parser")

        # every result block is a div with class lg-line
        lines = soup.select("div.lg-line")
        for line in lines:
            # find the date: there are two .lg-date columns; the second has the actual date & year
            date_nodes = line.select("div.lg-date")
            date_obj = None
            if date_nodes:
                # try second .lg-date (right aligned) with strong containing "02 Oct" and year after it
                if len(date_nodes) >= 2:
                    right = date_nodes[1]
                    strong = right.find("strong")
                    year_text = right.get_text(" ", strip=True)
                    if strong:
                        # build "02 Oct 2025" by combining strong + remaining text
                        day_month = strong.get_text(" ", strip=True)
                        # get text after the strong tag (usually the year)
                        after = strong.next_sibling
                        if after:
                            # sometimes next_sibling is a newline/text containing year
                            year = str(after).strip()
                        else:
                            # fallback: take right.text and remove the strong text
                            year = year_text.replace(day_month, "").strip()
                        candidate = f"{day_month} {year}".strip()
                        date_obj = try_parse_date_any(candidate)
                # fallback: try to find any date within the whole line
            if not date_obj:
                txt = line.get_text(" ", strip=True)
                m = re.search(r'(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}|\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})', txt)
                if m:
                    date_obj = try_parse_date_any(m.group(1))

            if not date_obj:
                continue

            # get numbers from ul.lg-numbers-small.game-number > li.lg-number
            nums = []
            ul = line.select_one("ul.lg-numbers-small.game-number")
            if ul:
                for li in ul.select("li.lg-number"):
                    t = li.get_text(" ", strip=True)
                    if re.search(r'\d', t):
                        try:
                            nums.append(int(re.search(r'\d{1,3}', t).group(0)))
                        except Exception:
                            pass
            else:
                # fallback: collect all numeric tokens in the line and take last 5
                found = [int(x) for x in re.findall(r'\d{1,3}', line.get_text(" ", strip=True))]
                found = [n for n in found if n != date_obj.year]
                nums = found[-5:] if len(found) >= 5 else found

            if len(nums) < 5:
                continue

            mains = nums[:5]
            page_draws.append({"date": date_obj.isoformat(), "main": mains, "bonus": []})

        # also return pageInfo attributes for pagination control if present
        page_info = {}
        pi = soup.find(id="pageInfo")
        if pi:
            try:
                page_info["pageNumber"] = int(pi.get("pageNumber", 1))
                page_info["pageSize"] = int(pi.get("pageSize", 10))
                page_info["lastPage"] = int(pi.get("lastPage", 1))
                page_info["totalElementCount"] = int(pi.get("totalElementCount", 0))
            except Exception:
                pass

        return page_draws, page_info

    # first request to discover pagination meta
    page = 1
    last_page = None
    while True:
        url = base_url if "?page=" in base_url else base_url.rstrip("/") + (f"?page={page}" if page > 1 else "")
        try:
            print(f"[debug] [redacted] {page}: {url}")
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            page_draws, page_info = parse_page(r.text)
            print(f"[debug] page {page} parsed draws: {len(page_draws)}")
            if page_info.get("lastPage"):
                last_page = page_info["lastPage"]
        except Exception as e:
            print(f"[warning] fetch/parse failed for page {page}: {e}")
            break

        draws.extend(page_draws)

        oldest_on_page = None
        try:
            dates_on_page = [datetime.fromisoformat(d["date"]).date() for d in page_draws]
            if dates_on_page:
                oldest_on_page = min(dates_on_page)
        except Exception:
            oldest_on_page = None

        if oldest_on_page and oldest_on_page < cutoff:
            print(f"[debug] reached cutoff on page {page} (oldest_on_page={oldest_on_page} < cutoff={cutoff})")
            break

        page += 1
        if last_page and page > last_page:
            break
        if page > 50:
            print("[warning] reached page cap (50), stopping")
            break
        time.sleep(0.25)

    # dedupe by date+numbers (sometimes duplicates across pages) and sort newest-first
    seen = set()
    deduped = []
    for d in draws:
        key = (d["date"], tuple(d.get("main", [])), tuple(d.get("bonus", [])))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(d)

    deduped.sort(key=lambda x: x["date"], reverse=True)
    print(f"[debug] scrape_lotteryguru_fortune_thursday: total parsed draws after paging={len(deduped)}")
    return deduped


def parse_sa_lotto_csv(csv_text):
    """
    Robust parser for South Africa Lotto CSV (handles dd.mm.YYYY and other variants).
    Returns list of {"date": ISOdate, "main": [...], "bonus": [...]}
    """
    draws = []
    if not csv_text:
        return draws

    lines = [ln for ln in csv_text.splitlines() if ln.strip()]
    for line in lines:
        # Split on tabs/commas/spaces; keep tokens
        parts = re.split(r'[\t,]+|\s{2,}|\s+', line.strip())
        if len(parts) < 3:
            continue

        # Attempt to parse date from parts[1] (most files use this)
        date_obj = None
        if len(parts) > 1:
            p = parts[1].strip()
            m_dot = re.match(r'^(\d{1,2})\.(\d{1,2})\.(\d{4})$', p)
            if m_dot:
                try:
                    date_obj = datetime.strptime(p, "%d.%m.%Y").date()
                except Exception:
                    date_obj = None
            else:
                date_obj = try_parse_date_any(p)

        # fallback: try to find a dd.mm.YYYY anywhere on the line
        if not date_obj:
            m_any = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', line)
            if m_any:
                try:
                    date_obj = datetime.strptime(m_any.group(1), "%d.%m.%Y").date()
                except Exception:
                    date_obj = try_parse_date_any(m_any.group(1))

        if not date_obj:
            continue

        # Collect numeric tokens after the date column (ignore draw number at parts[0])
        nums = []
        for token in parts[2:]:
            if not token:
                continue
            m = re.search(r'(\d{1,3})', token)
            if m:
                try:
                    nums.append(int(m.group(1)))
                except Exception:
                    pass

        if not nums:
            continue

        mains = nums[:6]
        bonus = nums[6:7] if len(nums) > 6 else []
        mains, bonus = _enforce_ranges(mains, bonus, "south_africa_lotto")
        _normalize_and_append(draws, date_obj, mains, bonus, page_id="south_africa_lotto")

    return draws


# ---------------- National Lottery API helpers ----------------
def discover_national_lottery_game_id(draw_cfg):
    """
    Try to discover the API game id by scraping the draw-history page for
    links or attributes that reference /draw-game/results/<id>/download
    or data-game-id attributes.
    Returns int game_id or None.
    """
    html_url = draw_cfg.get("html_url")
    if not html_url:
        return None
    try:
        soup = fetch_soup(html_url)
    except Exception:
        return None

    # look for explicit data attributes or hrefs
    for el in soup.find_all(attrs=True):
        for attr, val in el.attrs.items():
            if isinstance(val, str) and re.search(r'/draw-game/results/(\d+)/download', val):
                m = re.search(r'/draw-game/results/(\d+)/download', val)
                if m:
                    try:
                        return int(m.group(1))
                    except Exception:
                        pass
            # some attributes may contain the id directly
            if attr.lower().endswith("gameid") or attr.lower().endswith("game-id") or attr.lower().endswith("data-game-id"):
                try:
                    return int(val)
                except Exception:
                    pass

    # search for anchor hrefs or onclick JS that reference the API path
    for a in soup.find_all("a", href=True):
        m = re.search(r'/draw-game/results/(\d+)/download', a["href"])
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass

    # search script tags for the pattern
    for s in soup.find_all("script"):
        txt = s.string or s.get_text(" ", strip=True)
        if not txt:
            continue
        m = re.search(r'/draw-game/results/(\d+)/download', txt)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
        m2 = re.search(r'gameId["\']?\s*[:=]\s*["\']?(\d+)', txt)
        if m2:
            try:
                return int(m2.group(1))
            except Exception:
                pass

    return None


def fetch_national_lottery_api_csv(game_id, referer_url=None, interval="ONE_EIGHTY"):
    """
    Call the National Lottery API download endpoint for a given game_id.
    Returns CSV/text content or None on failure.
    """
    if not game_id:
        return None
    url = f"{API_BASE_NATIONAL_LOTTERY}/draw-game/results/{game_id}/download"
    headers = {
        "User-Agent": HEADERS.get("User-Agent"),
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.national-lottery.co.uk",
    }
    if referer_url:
        headers["Referer"] = referer_url
    if X_DFE_DEVICE_ID:
        headers["x-dfe-device-id"] = X_DFE_DEVICE_ID

    params = {"interval": interval}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        print(f"[warning] national-lottery API request failed for game_id={game_id}: {e}")
        return None

    ct = r.headers.get("Content-Type", "")
    # If JSON, try to extract CSV text or structured rows
    if "application/json" in ct or r.text.strip().startswith("{"):
        try:
            j = r.json()
        except Exception:
            j = None
        if isinstance(j, dict):
            # common patterns: csv text in a key, or structured rows
            for key in ("csv", "data", "download", "file", "content"):
                if key in j and isinstance(j[key], str) and j[key].strip():
                    return j[key]
            # sometimes rows are provided as list of objects
            if "rows" in j and isinstance(j["rows"], list):
                rows = j["rows"]
                if rows and isinstance(rows[0], dict):
                    out = io.StringIO()
                    writer = csv.DictWriter(out, fieldnames=list(rows[0].keys()))
                    writer.writeheader()
                    for rrow in rows:
                        writer.writerow(rrow)
                    return out.getvalue()
            # fallback: pretty-print JSON as text (parser may still extract numbers)
            return json.dumps(j)
        else:
            return r.text
    # If CSV or octet-stream, return raw text
    if "text/csv" in ct or "application/octet-stream" in ct or r.text.strip().startswith(("Date", "Draw", "DRAW")):
        return r.text
    # fallback: return text anyway
    return r.text


def fetch_csv_for_draw_cfg(draw_cfg):
    """
    Try to fetch CSV for a draw_cfg:
    1) Try draw_cfg['csv_url'] if present.
    2) If that fails and draw_cfg has api_game_id, use it.
    3) Otherwise attempt to discover game id from html page and call API.
    4) If all else fails, fall back to HTML scraping.
    Returns CSV/text or None.
    """
    csv_url = draw_cfg.get("csv_url")
    html_url = draw_cfg.get("html_url")
    page_id = draw_cfg.get("page_id")

    # 1) try csv_url directly
    if csv_url:
        try:
            print(f"[debug] trying csv_url: {csv_url}")
            txt = fetch_url(csv_url)
            # quick sanity check: must contain some digits and date-like tokens
            if txt and (re.search(r'\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4}', txt) or re.search(r'\bDate\b|\bDRAW\b', txt, re.I)):
                return txt
            # if content-type indicates JSON, still return for parse_csv_text to handle
            return txt
        except Exception as e:
            print(f"[debug] csv_url fetch failed: {e}")

    # 2) try api_game_id if provided
    api_game_id = draw_cfg.get("api_game_id")
    if api_game_id:
        print(f"[debug] trying provided api_game_id: {api_game_id}")
        txt = fetch_national_lottery_api_csv(api_game_id, referer_url=html_url)
        if txt:
            return txt

    # 3) attempt to discover game id from html page (National Lottery)
    if html_url:
        print(f"[debug] attempting to discover game id from html page: {html_url}")
        try:
            discovered = discover_national_lottery_game_id(draw_cfg)
            if discovered:
                print(f"[debug] discovered game id: {discovered}")
                txt = fetch_national_lottery_api_csv(discovered, referer_url=html_url)
                if txt:
                    return txt
        except Exception as e:
            print(f"[warning] discovery/fetch via API failed: {e}")

    # 4) last resort: try scraping HTML for draws
    try:
        html_draws = scrape_html(draw_cfg)
        if html_draws:
            # convert to CSV-like text for downstream parsing if needed
            out = io.StringIO()
            writer = csv.writer(out)
            writer.writerow(["date", "main", "bonus"])
            for d in html_draws:
                writer.writerow([d["date"], " ".join(map(str, d.get("main", []))), " ".join(map(str, d.get("bonus", [])))])
            return out.getvalue()
    except Exception:
        pass

    return None


# ---------------- Example main flow (adapt to your existing pipeline) ----------------
def main_fetch_and_parse_all():
    all_draws = {}
    for key, cfg in LOTTERIES.items():
        print(f"[info] processing {key}")
        csv_text = fetch_csv_for_draw_cfg(cfg)
        draws = []
        if csv_text:
            try:
                draws = parse_csv_text(csv_text, page_id=cfg.get("page_id"))
                if not draws:
                    # try SA-specific parser if page_id matches
                    if cfg.get("page_id") in ("sa_lotto", "south_africa_lotto"):
                        draws = parse_sa_lotto_csv(csv_text)
                if not draws:
                    # fallback: try HTML scrape
                    draws = scrape_html(cfg)
            except Exception as e:
                print(f"[warning] parsing failed for {key}: {e}")
                draws = scrape_html(cfg)
        else:
            print(f"[debug] no csv_text for {key}, trying HTML scrape")
            draws = scrape_html(cfg)

        print(f"[info] parsed {len(draws)} draws for {key}")
        all_draws[key] = draws
        time.sleep(0.2)

    # Example: print summary counts
    for k, v in all_draws.items():
        print(f"{k}: {len(v)} draws parsed")

    return all_draws


if __name__ == "__main__":
    # Run the fetch/parse pipeline (this script focuses on fetching/parsing; Firestore save omitted)
    results = main_fetch_and_parse_all()
    # For demonstration, write a small JSON file locally (optional)
    try:
        with open("parsed_draws_summary.json", "w", encoding="utf-8") as fh:
            json.dump({k: len(v) for k, v in results.items()}, fh, indent=2)
        print("[info] wrote parsed_draws_summary.json")
    except Exception as e:
        print(f"[warning] could not write summary file: {e}")
