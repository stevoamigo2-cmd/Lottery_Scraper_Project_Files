#!/usr/bin/env python3
# lottery_hot_numbers_with_firestore.py — rewritten to use the National Lottery API CSV for EuroMillions

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

import firebase_admin
from firebase_admin import credentials, firestore

# ------------------- Config -------------------

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LotteryHotBot/1.0)"}
DAYS_BACK = int(os.environ.get("DAYS_BACK", "60"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "15"))

LOTTERIES = {
    # EuroMillions (UK) uses the api-dfe download CSV
    "euromillions": {
        "api_csv_url": "https://api-dfe.national-lottery.co.uk/draw-game/results/33/download?interval=ONE_EIGHTY",
        "html_url": "https://www.national-lottery.co.uk/results/euromillions/draw-history",
        "page_id": "euromillions",
    },
    "lotto": {
        "html_url": "https://www.national-lottery.co.uk/results/lotto/draw-history",
        "csv_url":  "https://www.national-lottery.co.uk/results/lotto/draw-history/csv",
        "page_id": "lotto",
    },
    # Add others similarly...
}

GAME_SPECS = {
    "euromillions": {"main": 5, "bonus": 2},
    "lotto": {"main": 6, "bonus": 0},
}

GAME_RANGES = {
    "euromillions": {"main_max": 50, "bonus_max": 12},
    "lotto": {"main_max": 59, "bonus_max": 0},
}

HOT_TOP_N = {
    "euromillions": {"top_main": 10, "top_bonus": 10},
    "lotto": {"top_main": 10, "top_bonus": 10},
}

# --------------- Helpers ----------------

def fetch_url(url):
    r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text

def try_parse_date_any(text):
    text = (text or "").strip()
    if not text:
        return None

    fmts = [
        "%d %b %Y", "%d %B %Y", "%d/%m/%Y", "%Y-%m-%d",
        "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y",
        "%b %d, %Y", "%B %d, %Y",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            pass

    m = re.search(r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})', text)
    if m:
        for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(m.group(1), fmt).date()
            except Exception:
                pass
    return None

def _enforce_ranges(mains, bonus, page_id=None):
    ranges = GAME_RANGES.get(page_id) or {}
    main_max = ranges.get("main_max", 9999)
    bonus_max = ranges.get("bonus_max", 9999)
    mains = [n for n in mains if 1 <= n <= main_max]
    bonus = [n for n in bonus if 1 <= n <= bonus_max]
    return mains, bonus

def _normalize_and_append(draws_list, date_obj, mains, bonus, page_id=None):
    if isinstance(mains, int):
        mains = [mains]
    if isinstance(bonus, int):
        bonus = [bonus]
    mains = [int(n) for n in mains if isinstance(n, int) and n >= 1]
    bonus = [int(n) for n in bonus if isinstance(n, int) and n >= 1]
    mains, bonus = _enforce_ranges(mains, bonus, page_id)
    draws_list.append({"date": date_obj.isoformat(), "main": mains, "bonus": bonus})

# ------------ CSV FOR API SOURCE ------------

def parse_api_csv(csv_text, page_id=None):
    draws = []
    f = io.StringIO(csv_text)
    reader = csv.reader(f)
    rows = list(reader)
    if not rows:
        return []

    # The API CSV for National Lottery EuroMillions typically has:
    # Date, Winning Numbers, Lucky Stars
    for r in rows:
        if not r:
            continue

        # try to parse first token as date
        date_obj = try_parse_date_any(r[0])
        if not date_obj:
            continue

        # winning numbers + lucky stars
        tokens = re.findall(r"\b\d{1,2}\b", " ".join(r[1:]))
        main_count = GAME_SPECS.get(page_id, {}).get("main", 5)
        bonus_count = GAME_SPECS.get(page_id, {}).get("bonus", 0)

        mains = tokens[:main_count]
        bonus = tokens[main_count:main_count+bonus_count]
        _normalize_and_append(draws, date_obj, mains, bonus, page_id)
    return draws

def fetch_api_csv(draw_cfg):
    url = draw_cfg.get("api_csv_url")
    if not url:
        return []
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        text = r.content.decode(r.encoding or "utf-8", errors="replace")
        return parse_api_csv(text, draw_cfg.get("page_id"))
    except Exception as e:
        print(f"[warning] API CSV fetch failed: {url} — {e}")
        return []

# ------------ STANDARD CSV AND HTML ------------

def parse_csv_text(csv_text, page_id=None):
    # (Standard CSV parser, unchanged from before)
    draws = []
    if not csv_text:
        return draws
    f = io.StringIO(csv_text)
    reader = csv.DictReader(f)
    ball_re = re.compile(r"\b\d{1,2}\b")
    for row in reader:
        date_str = next((row[k] for k in row if "date" in k.lower()), None)
        date_obj = try_parse_date_any(date_str)
        if not date_obj:
            continue
        mains, bonus = [], []
        for k, v in row.items():
            if not v:
                continue
            nums = [int(x) for x in ball_re.findall(v)]
            if "bonus" in k.lower() or "powerball" in k.lower():
                bonus.extend(nums)
            else:
                mains.extend(nums)
        _normalize_and_append(draws, date_obj, mains, bonus, page_id)
    return draws

def fetch_csv(draw_cfg):
    urls = []
    if cfg_csv := draw_cfg.get("csv_url"):
        urls.append(cfg_csv)
        if "?" not in cfg_csv:
            urls.append(cfg_csv + "?draws=200")
    for u in urls:
        try:
            r = requests.get(u, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            txt = r.content.decode(r.encoding or "utf-8", errors="replace")
            out = parse_csv_text(txt, draw_cfg.get("page_id"))
            if out:
                return out
        except Exception:
            pass
    return []

def scrape_html(draw_cfg):
    draws = []
    url = draw_cfg.get("html_url")
    if not url:
        return draws
    try:
        soup = BeautifulSoup(fetch_url(url), "html.parser")
        rows = soup.select("table tr, ul li")
        for r in rows:
            text = r.get_text(" ", strip=True)
            date_obj = try_parse_date_any(text)
            if not date_obj:
                continue
            nums = [int(x) for x in re.findall(r"\b\d{1,2}\b", text)]
            spec = GAME_SPECS.get(draw_cfg.get("page_id"), {})
            main_count = spec.get("main", 5)
            bonus_count = spec.get("bonus", 0)
            mains = nums[:main_count]
            bonus = nums[main_count:main_count+bonus_count]
            _normalize_and_append(draws, date_obj, mains, bonus, draw_cfg.get("page_id"))
    except Exception:
        pass
    return draws

# ------------ FILTER & HOT ---------------

def filter_recent(draws, days_back):
    cutoff = datetime.utcnow().date() - timedelta(days=days_back)
    return [d for d in draws if datetime.fromisoformat(d["date"]).date() >= cutoff]

def compute_hot(draws, top_main_n=10, top_bonus_n=10, page_id=None):
    mc, bc = Counter(), Counter()
    ranges = GAME_RANGES.get(page_id) or {}
    for d in draws:
        mc.update([n for n in d["main"] if 1 <= n <= ranges.get("main_max", 9999)])
        bc.update([n for n in d["bonus"] if 1 <= n <= ranges.get("bonus_max", 9999)])
    return mc.most_common(top_main_n), bc.most_common(top_bonus_n)

# ------------ Firestore ----------------

def init_firestore():
    if firebase_admin._apps:
        return firestore.client()
    sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
    gac = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if sa_json:
        firebase_admin.initialize_app(credentials.Certificate(json.loads(sa_json)))
    elif gac and os.path.exists(gac):
        firebase_admin.initialize_app(credentials.Certificate(gac))
    else:
        firebase_admin.initialize_app()
    return firestore.client()

def save_to_firestore(db, key, out):
    db.collection("lotteries").document(key).set(out)

# ------------ Main Runner ----------------

def run_and_save():
    try:
        db = init_firestore()
    except Exception:
        db = None

    results = {}
    for key, cfg in LOTTERIES.items():
        print(f"== Processing {key} ==")

        # 1) Try API CSV if present
        draws = fetch_api_csv(cfg)

        # 2) Otherwise try regular CSV
        if not draws:
            draws = fetch_csv(cfg)

        # 3) Fallback HTML
        if not draws:
            draws = scrape_html(cfg)

        recent = filter_recent(draws, DAYS_BACK)
        top_main, top_bonus = compute_hot(recent,
                                          HOT_TOP_N.get(key, {}).get("top_main", 10),
                                          HOT_TOP_N.get(key, {}).get("top_bonus", 10),
                                          key)
        out = {
            "fetched_at": datetime.utcnow().isoformat() + "Z",
            "draws_total": len(draws),
            "draws_recent": len(recent),
            "top_main": [{"number": n, "count": c} for n, c in top_main],
            "top_bonus": [{"number": n, "count": c} for n, c in top_bonus],
        }

        with open(f"{key}_hot.json", "w") as f:
            json.dump(out, f, indent=2)

        if db:
            try:
                save_to_firestore(db, key, out)
            except Exception:
                pass

        results[key] = out

    return results

if __name__ == "__main__":
    run_and_save()
