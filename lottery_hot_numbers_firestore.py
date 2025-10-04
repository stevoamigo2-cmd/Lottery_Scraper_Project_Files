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
# Many official lottery sites or state lottery portals expose CSV downloads, but paths can change.
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

    # --- Australia ---
    "australia_powerball": {
        "html_url": "https://www.lotterywest.wa.gov.au/games/powerball",
        "csv_url": "https://api.lotterywest.wa.gov.au/api/v1/games/5132/results-csv",
        "note": "Official CSV endpoint (Lotterywest API).",
        "source": "Lotterywest download page / API."
    }
}

# ------------ Helpers ------------
def fetch_url(url):
    r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
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
        "%b %d, %Y", "%B %d, %Y",         # "Jan 14, 2025" / "January 14, 2025"
        "%a %d %b %Y", "%A %d %B %Y",     # "Sat 14 Jun 2025" / "Saturday 14 June 2025"
        "%a, %b %d, %Y", "%A, %B %d, %Y"  # "Sat, Jun 14, 2025"
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


def scrape_html(draw_cfg):
    """
    More resilient HTML scraping fallback:
    - Try the original selector
    - If nothing found, search for list items or table rows containing a date & numbers
    """
    url = draw_cfg["html_url"]
    print(f"[debug] Scrape HTML: {url}")
    soup = fetch_soup(url)

    # 1) original specific selector attempt
    selector = f"#draw_history_{draw_cfg['page_id']} ul.list_table_presentation"
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
                draws.append({"date": date_obj.isoformat(), "main": mains, "bonus": bonuses})
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
        # common patterns like "Sat 24 May 2025" or "24/05/2025"
        m = re.search(r'(\d{1,2}\s+\w{3,9}\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\w+\s+\d{1,2},\s*\d{4})', text)
        if m:
            date_match = m.group(1)
        if not date_match:
            # try to find a leading 'Date:' label
            m2 = re.search(r'date[:\s]*([^\|\,\-]{6,20})', text, re.I)
            if m2:
                date_match = m2.group(1)
        if not date_match:
            continue
        date_obj = try_parse_date_any(date_match)
        if not date_obj:
            continue
        # extract numbers (first up to 8)
        nums = [int(x) for x in re.findall(r'\d{1,2}', text)]
        mains = nums[:5]
        bonus = nums[5:8]
        draws.append({"date": date_obj.isoformat(), "main": mains, "bonus": bonus})

    print(f"[debug] scrape_html parsed draws: {len(draws)}")
    return draws

def parse_csv_text(csv_text):
    """
    Robust CSV parser:
    - Strips BOM
    - Sniffs delimiter
    - Uses DictReader for clean headered CSVs (supports English + Spanish header names)
    - Falls back to a row-oriented parser for messy/header-split sheets (like your Spain sheet)
    """
    if not csv_text:
        return []

    # strip BOM if present
    csv_text = csv_text.lstrip('\ufeff\ufeff')

    # quick lines for sniffing
    lines = [ln for ln in csv_text.splitlines() if ln.strip()]
    sample = "\n".join(lines[:20]) if lines else csv_text[:4096]

    # try to detect delimiter
    delimiter = ","
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample)
        delimiter = dialect.delimiter
    except Exception:
        delimiter = "\t" if "\t" in sample else ","

    # Attempt DictReader first (detect English or Spanish header names)
    f = io.StringIO(csv_text)
    reader = csv.DictReader(f, delimiter=delimiter)
    fieldnames = reader.fieldnames or []

    # Recognize common date header keywords (English/Spanish)
    has_date_header = any(any(k in (fn or "").lower() for k in ("date", "fecha", "draw")) for fn in fieldnames)
    # Recognize Spanish combination-style header (COMBINACIÓN GANADORA) or many empty header cells
    looks_like_spanish_sheet = any("combin" in (fn or "").lower() for fn in fieldnames) or (sum(1 for fn in fieldnames if not fn or fn.strip() == "") > 2)

    if has_date_header and not looks_like_spanish_sheet:
        # Clean headered CSV path (DictReader) - extended Spanish keywords supported
        draws = []
        f2 = io.StringIO(csv_text)
        reader2 = csv.DictReader(f2, delimiter=delimiter)
        for row in reader2:
            # find date column (support 'date' and 'fecha' and common alternates)
            date_str = None
            for k in row:
                kl = (k or "").lower()
                if 'date' in kl or 'fecha' in kl or 'draw' in kl:
                    date_str = (row[k] or "").strip()
                    break
            if not date_str:
                # try a few alternates
                for alt in ('draw', 'draw_date', 'date_played', 'fecha'):
                    if alt in row:
                        date_str = (row[alt] or "").strip()
                        break
            if not date_str:
                continue
            date_obj = try_parse_date_any(date_str)
            if not date_obj:
                continue

            mains = []
            bonus = []
            # heuristics: extended to Spanish header words
            for k, v in row.items():
                if not v:
                    continue
                key = (k or "").lower()
                val = (v or "").strip()
                # main picks: english or spanish keywords
                if re.search(r'ball|num|pick|winning|white|combinaci|ganadora|bola|nro|número|numero', key, re.I):
                    found = re.findall(r'\d{1,2}', val)
                    mains.extend([int(x) for x in found])
                # bonus-ish: complementario, reintegro, comp.
                elif re.search(r'bonus|comp|complementar|reint|reintegro|power|mega|joker', key, re.I):
                    found = re.findall(r'\d{1,2}', val)
                    bonus.extend([int(x) for x in found])

            # fallback: if no explicit fields found, scan values left-to-right (same as before)
            if not mains:
                for v in row.values():
                    if v and re.search(r'\d', v):
                        found = re.findall(r'\d{1,2}', v)
                        if len(found) >= 3:
                            mains = [int(x) for x in found[:7]]
                            break

            draws.append({"date": date_obj.isoformat(), "main": mains, "bonus": bonus})
        return draws

    # If this looks like the Spanish Google Sheet (messy headers), parse by rows:
    # assume first column is date, subsequent columns are numbers; optionally final column is Joker (long)
    f3 = io.StringIO(csv_text)
    reader3 = csv.reader(f3, delimiter=delimiter)
    rows = [r for r in reader3 if any((c or "").strip() for c in r)]
    if not rows:
        return []

    # assume first row is header; data starts at second row
    data_rows = rows[1:]
    draws = []
    for row in data_rows:
        if not row:
            continue
        date_str = (row[0] or "").strip()
        date_obj = try_parse_date_any(date_str)
        if not date_obj:
            # try to find date-like cell anywhere in the row
            joined = " ".join(row)
            m = re.search(r'(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})', joined)
            if m:
                date_obj = try_parse_date_any(m.group(1))
            if not date_obj:
                continue
        # take the remaining columns as numbers in order
        tail = [c.strip() for c in row[1:] if c is not None and str(c).strip() != ""]
        # drop trailing joker-like long numeric token if present
        if tail and re.fullmatch(r'\d{6,}', tail[-1]):
            tail = tail[:-1]
        nums = []
        for v in tail:
            found = re.findall(r'\d{1,2}', v)
            nums.extend([int(x) for x in found])
        mains = nums[:6] if len(nums) >= 6 else nums
        bonus = nums[6:8] if len(nums) > 6 else []
        draws.append({"date": date_obj.isoformat(), "main": mains, "bonus": bonus})
    return draws


    # fallback: headerless table parsing (preserves your previous heuristics)
        # fallback: headerless table parsing (preserves your previous heuristics)
    f3 = io.StringIO(csv_text)
    reader3 = csv.reader(f3, delimiter=delimiter)
    draws = []
    for raw_row in reader3:
        if not raw_row:
            continue

        # Normalize: if csv.reader produced a single column (likely whitespace-separated file),
        # split that single cell on whitespace into tokens; otherwise trim empty columns.
        if len(raw_row) == 1:
            tokens = re.split(r'\s+', raw_row[0].strip())
        else:
            tokens = [c.strip() for c in raw_row if c is not None and str(c).strip() != ""]

        if not tokens:
            continue

        # Find first position where three consecutive tokens look like month/day/year or day/month/year
        date_idx = None
        for i in range(len(tokens) - 2):
            if tokens[i].isdigit() and tokens[i+1].isdigit() and tokens[i+2].isdigit():
                # plausible year check (4 digits) or a reasonable month/day/year ordering
                y = int(tokens[i+2])
                m = int(tokens[i])
                d = int(tokens[i+1])
                if 1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
                    date_idx = i
                    month, day, year = m, d, y
                    break
                # try alternate ordering (day, month, year)
                y2 = int(tokens[i+2])
                d2 = int(tokens[i])
                m2 = int(tokens[i+1])
                if 1900 <= y2 <= 2100 and 1 <= m2 <= 12 and 1 <= d2 <= 31:
                    date_idx = i
                    month, day, year = m2, d2, y2
                    break

        if date_idx is None:
            # Not the pattern we expect; fall back to joining and trying to find a date substring
            joined = " ".join(tokens)
            m = re.search(r'(\d{1,2}[\/\.\-]\d{1,2}[\/\.\-]\d{2,4})', joined)
            if not m:
                continue
            # parse date fragment later in the "last-resort" handling; skip this pattern branch
            # (we'll handle in later part of the loop)
        else:
            # game name is everything before date_idx — normalize (remove spaces/punctuation)
            game_raw = " ".join(tokens[:date_idx]).lower()
            game = re.sub(r'[\s\-_]', '', game_raw)

            # build date object
            try:
                date_obj = datetime(year, month, day).date()
            except Exception:
                continue

            # numeric tail is tokens after date_idx+3
            numeric_tail = tokens[date_idx+3:]
            numbers = []
            for n in numeric_tail:
                if re.search(r'\d', str(n)):
                    # allow 1-3 digit tokens (defensive)
                    found = re.findall(r'\d{1,3}', str(n))
                    numbers.extend([int(x) for x in found])

            # GAME_SPECS same as before but make game matching tolerant (we normalized game)
            GAME_SPECS = {
                "powerball": {"main": 5, "bonus": 1},
                "megamillions": {"main": 5, "bonus": 1},
                "euromillions": {"main": 5, "bonus": 2},
                "lotto": {"main": 6, "bonus": 0},
                "thunderball": {"main": 5, "bonus": 1},
                "set-for-life": {"main": 5, "bonus": 1},
                "australia_powerball": {"main": 7, "bonus": 1}, 
                "powerball_au": {"main": 7, "bonus": 1},
                "spain_loterias": {"main": 6, "bonus": 2},
            }
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
                # heuristic fallback
                if len(numbers) == 6:
                    mains = numbers[:5]
                    bonus = numbers[5:]
                elif len(numbers) == 7:
                    mains = numbers[:5]
                    bonus = numbers[5:]
                elif len(numbers) == 8:
                    mains = numbers[:7]
                    bonus = numbers[7:]
                else:
                    mains = numbers[:5]
                    bonus = numbers[5:]
            draws.append({"date": date_obj.isoformat(), "main": mains, "bonus": bonus})
            continue

        # --- existing last-resort parsing when date token wasn't found above ---
        # (keep your original "joined" parsing & date regex code here; unchanged)
        joined = ",".join(tokens)
        date_obj = None
        m = re.search(r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})', joined)
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
        nums = re.findall(r'\d{1,2}', joined)
        if len(nums) >= 6:
            numbers = [int(x) for x in nums[-8:]]
            if len(numbers) >= 6:
                mains = numbers[:5]
                bonus = numbers[5:]
                draws.append({"date": date_obj.isoformat(), "main": mains, "bonus": bonus})
    return draws


def fetch_csv(draw_cfg):
    """
    Try a series of CSV url variants and return parsed draws or [].
    Will attempt different encodings and query param variants.
    """
    csv_url = draw_cfg.get("csv_url")
    variants = []
    if csv_url:
        variants.append(csv_url)
        # try common param variant
        if "?" not in csv_url:
            variants.append(csv_url + "?draws=200")
    # derived variants from html_url
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
            r = requests.get(u, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            # try different encodings: response.encoding or apparent_encoding or utf-8
            enc = r.encoding or getattr(r, "apparent_encoding", None) or "utf-8"
            try:
                txt = r.content.decode(enc, errors="replace")
            except Exception:
                txt = r.content.decode("ISO-8859-1", errors="replace")
            draws = parse_csv_text(txt)
            if draws:
                print(f"[debug] CSV parsed OK from {u} (rows: {len(draws)})")
                return draws
            else:
                # print a short sample for debugging
                sample = txt.splitlines()[:8]
                print(f"[debug] CSV from {u} parsed 0 draws; sample:\n" + "\n".join(sample))
        except Exception as e:
            print(f"[warning] CSV fetch failed for {u}: {e}")
    return []


def filter_recent(draws, days_back):
    cutoff = datetime.utcnow().date() - timedelta(days=days_back)
    return [d for d in draws if datetime.fromisoformat(d["date"]).date() >= cutoff]

def compute_hot(draws, top_n=10):
    mc = Counter()
    bc = Counter()
    for d in draws:
        mc.update(d.get("main", []))
        bc.update(d.get("bonus", []))
    return mc.most_common(top_n), bc.most_common(top_n)

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
        # Still continue — we'll save JSON files locally
        db = None

    results = {}
    for key, cfg in LOTTERIES.items():
        print(f"\n== Processing {key} ==")
        draws = []
        try:
            # prefer CSV when available (more stable than HTML scraping)
            if cfg.get("csv_url"):
                try:
                    draws = fetch_csv(cfg)
                    if draws:
                        print(f"[debug] parsed draws from CSV: {len(draws)}")
                except Exception as e:
                    print(f"[warning] CSV fetch/parse failed for {key}: {e}")
                    draws = []
            # fallback to HTML scraping if CSV empty or not available
            if not draws:
                print("[debug] No draws found by CSV, trying HTML scraping.")
                draws = scrape_html(cfg)
                print(f"[debug] parsed draws from HTML: {len(draws)}")

            recent = filter_recent(draws, DAYS_BACK)
            print(f"[debug] recent draws (last {DAYS_BACK} days): {len(recent)}")
            top_main, top_bonus = compute_hot(recent, top_n=10)

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
