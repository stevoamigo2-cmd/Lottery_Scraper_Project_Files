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

# NOTE: JSON URLs for US multi-jurisdiction games (NY state data)
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
    "megamillions": {
        "json_url": "https://data.ny.gov/resource/h6w8-42p9.json",
        "main_count": 5,
        "bonus_count": 1,
        "page_id": "megamillions",
    },
    "powerball": {
        "json_url": "https://data.ny.gov/resource/5xaw-6ayf.json",
        "main_count": 5,
        "bonus_count": 1,
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

def parse_date_span(text):
    text = text.strip()
    parts = text.split(" ", 1)
    date_only = parts[1] if len(parts) == 2 else text
    for fmt in ("%d %b %Y", "%d %B %Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_only, fmt).date()
        except Exception:
            pass
    return None

def scrape_html(draw_cfg):
    url = draw_cfg["html_url"]
    soup = fetch_soup(url)
    selector = f"#draw_history_{draw_cfg['page_id']} ul.list_table_presentation"
    entries = soup.select(selector)
    draws = []
    for ul in entries:
        spans = ul.select("span.table_cell_block")
        if len(spans) >= 3:
            date_txt = spans[0].get_text(" ", strip=True)
            main_txt = spans[2].get_text(" ", strip=True) if len(spans) >= 3 else ""
            bonus_txt = spans[3].get_text(" ", strip=True) if len(spans) >= 4 else ""
            date_obj = parse_date_span(date_txt)
            if date_obj is None:
                continue
            mains = extract_numbers_from_span(main_txt)
            bonuses = extract_numbers_from_span(bonus_txt)
            draws.append({"date": date_obj.isoformat(), "main": mains, "bonus": bonuses})
    return draws

def parse_csv_text(csv_text):
    # same as your previous robust parser
    GAME_SPECS = {
        "powerball": {"main": 5, "bonus": 1},
        "megamillions": {"main": 5, "bonus": 1},
        "euromillions": {"main": 5, "bonus": 2},
        "lotto": {"main": 6, "bonus": 0},
        "thunderball": {"main": 5, "bonus": 1},
        "set-for-life": {"main": 5, "bonus": 1},
    }

    lines = [ln for ln in csv_text.splitlines() if ln.strip()]
    first_line = lines[0] if lines else ""
    delimiter = "\t" if "\t" in first_line else ","

    f_dict = io.StringIO(csv_text)
    dreader = csv.DictReader(f_dict)
    fieldnames = dreader.fieldnames or []
    if any("date" in (fn or "").lower() for fn in fieldnames):
        draws = []
        f = io.StringIO(csv_text)
        reader = csv.DictReader(f)
        for row in reader:
            date_str = None
            for k in row:
                if 'date' in k.lower():
                    date_str = (row[k] or "").strip()
                    break
            if not date_str:
                continue
            date_obj = None
            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d %b %Y", "%d %B %Y", "%m/%d/%Y"):
                try:
                    date_obj = datetime.strptime(date_str, fmt).date()
                    break
                except Exception:
                    pass
            if not date_obj:
                continue

            nums = []
            bonus = []
            for k, v in row.items():
                if not v:
                    continue
                if re.search(r'ball|num|pick|winning', k, re.I) and not re.search(r'bonus|power|mega|megap', k, re.I):
                    found = re.findall(r'\d{1,2}', v)
                    nums.extend([int(x) for x in found])
                if re.search(r'bonus|power|mega|megap|thunder', k, re.I):
                    found = re.findall(r'\d{1,2}', v)
                    bonus.extend([int(x) for x in found])
            if nums:
                draws.append({"date": date_obj.isoformat(), "main": nums, "bonus": bonus})
        return draws

    # fallback: simple tabular parsing
    f = io.StringIO(csv_text)
    reader = csv.reader(f, delimiter=delimiter)
    draws = []
    for row in reader:
        if not row:
            continue
        joined = ",".join(row)
        nums = re.findall(r'\d{1,2}', joined)
        if len(nums) >= 5:
            draws.append({"date": datetime.utcnow().date().isoformat(), "main": [int(x) for x in nums[:5]], "bonus": [int(x) for x in nums[5:]]})
    return draws

def fetch_csv(draw_cfg):
    csv_url = draw_cfg.get("csv_url")
    if not csv_url:
        return []
    r = requests.get(csv_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    txt = r.content.decode("ISO-8859-1")
    return parse_csv_text(txt)

def fetch_ny_lottery_json(url, main_count=5, bonus_count=1):
    r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    draws = []
    for entry in data:
        date_obj = datetime.strptime(entry['draw_date'], '%Y-%m-%dT%H:%M:%S.%f').date()
        main = [int(entry[f'num{i}']) for i in range(1, main_count+1)]
        if bonus_count == 1:
            bonus = [int(entry.get('powerball') or entry.get('megaplier') or 0)]
        else:
            bonus = []
        draws.append({"date": date_obj.isoformat(), "main": main, "bonus": bonus})
    return draws

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
    try:
        if firebase_admin._apps:
            return firestore.client()
    except Exception:
        pass
    sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
    gac = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if sa_json:
        sa_obj = json.loads(sa_json)
        cred = credentials.Certificate(sa_obj)
        firebase_admin.initialize_app(cred)
    elif gac and os.path.exists(gac):
        cred = credentials.Certificate(gac)
        firebase_admin.initialize_app(cred)
    else:
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
            # NY JSON feeds for Powerball & Mega Millions
            if "json_url" in cfg:
                draws = fetch_ny_lottery_json(cfg["json_url"], cfg.get("main_count",5), cfg.get("bonus_count",1))
                print(f"[debug] parsed draws from JSON: {len(draws)}")
            # prefer CSV when available (UK lotteries)
            elif cfg.get("csv_url"):
                try:
                    draws = fetch_csv(cfg)
                    print(f"[debug] parsed draws from CSV: {len(draws)}")
                except Exception as e:
                    print(f"[warning] CSV fetch/parse failed for {key}: {e}")
            # fallback to HTML scraping if empty
            if not draws and "html_url" in cfg:
                draws = scrape_html(cfg)
                print(f"[debug] parsed draws from HTML: {len(draws)}")

            recent = filter_recent(draws, DAYS_BACK)
            top_main, top_bonus = compute_hot(recent, top_n=10)

            out = {
                "fetched_at": datetime.utcnow().isoformat() + "Z",
                "draws_total": len(draws),
                "draws_recent": len(recent),
                "top_main": [{"number": n, "count": c} for n, c in top_main],
                "top_bonus": [{"number": n, "count": c} for n, c in top_bonus],
            }
            results[key] = out

            fname = f"{key}_hot.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2)
            print(f"[debug] Saved {fname}")

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
