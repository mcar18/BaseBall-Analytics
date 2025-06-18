"""
collect_betting_odds.py

Fetches MLB betting odds (moneyline and spreads) from The Odds API.
Saves as a dated CSV in data/raw/odds/.
"""

import os
import sys
import logging
import requests
import pandas as pd
from datetime import datetime

# ─── Configuration ─────────────────────────────────────────────────────────────
API_KEY    = os.getenv("ODDS_API_KEY")
SPORT_KEY  = "baseball_mlb"
REGIONS    = "us"                   # US bookmakers
MARKETS    = "h2h,spreads"          # head-to-head (moneyline) and spread
DATE_FMT   = "%Y-%m-%dT%H:%M:%SZ"
OUTPUT_DIR = "data/raw/odds"
# ────────────────────────────────────────────────────────────────────────────────

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def fetch_odds() -> pd.DataFrame:
    """
    Calls The Odds API and returns a flattened DataFrame of odds.
    """
    if not API_KEY:
        logging.error("Environment variable ODDS_API_KEY not set.")
        sys.exit(1)

    url = (
        f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}"
        f"/odds?apiKey={API_KEY}&regions={REGIONS}"
        f"&markets={MARKETS}&dateFormat=iso"
    )

    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logging.error(f"API request failed: {e}")
        sys.exit(1)

    records = []
    for game in data:
        match_time = game.get("commence_time")
        home = game["home_team"]
        away = game["away_team"]

        for site in game.get("bookmakers", []):
            site_name = site["title"]
            for market in site.get("markets", []):
                mkt = market["key"]
                for out in market["outcomes"]:
                    records.append({
                        "match_time": match_time,
                        "home_team": home,
                        "away_team": away,
                        "site": site_name,
                        "market": mkt,
                        "outcome": out["name"],
                        "odds": out["price"]
                    })

    df = pd.DataFrame(records)
    # normalize datetime
    df["match_time"] = pd.to_datetime(df["match_time"])
    return df

def save_odds(df: pd.DataFrame):
    """
    Saves DataFrame to CSV named odds_YYYYMMDD.csv in OUTPUT_DIR.
    """
    date_str = datetime.utcnow().strftime("%Y%m%d")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"odds_{date_str}.csv")
    df.to_csv(path, index=False)
    logging.info(f"Saved {len(df)} odds rows to {path}")

def main():
    setup_logging()
    logging.info("Fetching MLB betting odds…")
    df = fetch_odds()
    if df.empty:
        logging.warning("No odds data returned.")
    else:
        save_odds(df)

if __name__ == "__main__":
    main()