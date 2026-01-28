#!/usr/bin/env python3
# lottery_hot_numbers_with_firestore.py
# PATCHED VERSION – National Lottery uses official API (no scraping)

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

# ------------ Config ------------
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Origin": "https://www.national-lottery.co.uk",
    "Referer": "https://www.national-lottery.co.uk/",
}
DAYS_BACK = int(os.environ.get("DAYS_BACK", "60"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "15"))

# ------------ National Lottery API ------------
NL_GAME_IDS = {
    "lotto": 33,
    "euromillions": 34,
    "thunderball": 36,
    "set-for-life": 52,
}

def fetch_national_lottery_api(game_id, interval="ONE_EIGHTY"):
    url = f"https://api-dfe.national-lottery.co.uk/draw-game/results/{game_id}/download"
    r = requests.get(url, headers=HEADERS, params={"interval": interval}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def parse_national_lottery_api(data, page_id):
    draws = []
    for item in data.get("draws", []):
        date_obj = try_parse_date_any(item.get("drawDate"))
        if not date_obj:
            continue
        nums = item.get("drawnNumbers", {})
        mains = nums.get("main", [])
        bonus = nums.get("bonus", [])
        _normalize_and_append(draws, date_obj, mains, bonus, page_id)
    return draws

# ------------ LOTTERIES CONFIG ------------
LOTTERIES = {
    "euromillions": {"page_id": "euromillions"},
    "lotto": {"page_id": "lotto"},
    "thunderball": {"page_id": "thunderball"},
    "set-for-life": {"page_id": "set-for-life"},

    # Non-UK remain unchanged
    "megamillions": {
        "csv_url": "https://www.texaslottery.com/export/sites/lottery/Games/Mega_Millions/Winning_Numbers/megamillions.csv",
        "page_id": "megamillions",
    },
    "powerball": {
        "csv_url": "https://www.texaslottery.com/export/sites/lottery/Games/Powerball/Winning_Numbers/powerball.csv",
        "page_id": "powerball",
    },
    "ghana_fortune_thursday": {
        "html_url": "https://lotteryguru.com/ghana-lottery-results/gh-fortune-thursday/gh-fortune-thursday-results-history",
        "page_id": "ghana_fortune_thursday",
    },
}

# ------------ GAME SPECS / RANGES ------------
GAME_SPECS = {
    "lotto": {"main": 6, "bonus": 0},
    "euromillions": {"main": 5, "bonus": 2},
    "thunderball": {"main": 5, "bonus": 1},
    "set-for-life": {"main": 5, "bonus": 1},
    "megamillions": {"main": 5, "bonus": 1},
    "powerball": {"main": 5, "bonus": 1},
    "ghana_fortune_thursday": {"main": 5, "bonus": 0},
}

GAME_RANGES = {
    "lotto": {"main_max": 59},
    "euromillions": {"main_max": 50, "bonus_max": 12},
    "thunderball": {"main_max": 39, "bonus_max": 14},
    "set-for-life": {"main_max": 47, "bonus_max": 10},
    "megamillions": {"main_max": 70, "bonus_max": 25},
    "powerball": {"main_max": 69, "bonus_max": 26},
}

# ------------ Helpers (unchanged) ------------
def try_parse_date_any(text):
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%d %b %Y", "%d %B %Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text.strip(), fmt).date()
        except Exception:
            pass
    return None

def _enforce_ranges(mains, bonus, page_id):
    r = GAME_RANGES.get(page_id, {})
    if "main_max" in r:
        mains = [n for n in mains if 1 <= n <= r["main_max"]]
    if "bonus_max" in r:
        bonus = [n for n in bonus if 1 <= n <= r["bonus_max"]]
    return mains, bonus

def _normalize_and_append(draws, date_obj, mains, bonus, page_id):
    mains = [int(n) for n in mains if isinstance(n, int)]
    bonus = [int(n) for n in bonus if isinstance(n, int)]
    mains, bonus = _enforce_ranges(mains, bonus, page_id)
    draws.append({"date": date_obj.isoformat(), "main": mains, "bonus": bonus})

def filter_recent(draws, days_back):
    cutoff = datetime.utcnow().date() - timedelta(days=days_back)
    return [d for d in draws if datetime.fromisoformat(d["date"]).date() >= cutoff]

def compute_hot(draws, top_main_n=10, top_bonus_n=10):
    mc, bc = Counter(), Counter()
    for d in draws:
        mc.update(d.get("main", []))
        bc.update(d.get("bonus", []))
    return mc.most_common(top_main_n), bc.most_common(top_bonus_n)

# ------------ Firestore ------------
def init_firestore():
    if firebase_admin._apps:
        return firestore.client()
    sa = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
    if sa:
        cred = credentials.Certificate(json.loads(sa))
        firebase_admin.initialize_app(cred)
    else:
        firebase_admin.initialize_app()
    return firestore.client()

# ------------ Main ------------
def run_and_save():
    db = init_firestore()
    results = {}

    for key, cfg in LOTTERIES.items():
        print(f"\n== {key.upper()} ==")
        page_id = cfg["page_id"]
        draws = []

        try:
            if page_id in NL_GAME_IDS:
                api_json = fetch_national_lottery_api(NL_GAME_IDS[page_id])
                draws = parse_national_lottery_api(api_json, page_id)
            elif cfg.get("csv_url"):
                r = requests.get(cfg["csv_url"], timeout=REQUEST_TIMEOUT)
                draws = parse_csv_text(r.text, page_id)
            elif page_id == "ghana_fortune_thursday":
                draws = scrape_lotteryguru_fortune_thursday(cfg)

            recent = filter_recent(draws, DAYS_BACK)
            top_main, top_bonus = compute_hot(recent)

            out = {
                "fetched_at": datetime.utcnow().isoformat() + "Z",
                "draws_total": len(draws),
                "draws_recent": len(recent),
                "top_main": [{"number": n, "count": c} for n, c in top_main],
                "top_bonus": [{"number": n, "count": c} for n, c in top_bonus],
            }

            with open(f"{key}_hot.json", "w") as f:
                json.dump(out, f, indent=2)

            db.collection("lotteries").document(key).set(out)
            results[key] = out
            print(f"[ok] {key} saved")

        except Exception as e:
            print(f"[error] {key}: {e}")

    return results

if __name__ == "__main__":
    run_and_save()
