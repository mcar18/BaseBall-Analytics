"""
scripts/collect_schedule_with_scores.py

Fetches the MLB schedule for the CURRENT season (including final boxscores) and
saves to data/raw/mlb_schedule_{season}.csv.
"""

import os
import requests
import pandas as pd
from datetime import datetime

# ─── Configuration ─────────────────────────────────────────────────────────────
OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Use current year for schedule
SEASON = datetime.now().year
MLB_API_BASE = "https://statsapi.mlb.com/api/v1"
# ────────────────────────────────────────────────────────────────────────────────

def get_schedule_with_scores(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Pulls schedule between start_date and end_date, including final scores.
    """
    url = f"{MLB_API_BASE}/schedule"
    params = {
        "sportId": 1,
        "startDate": start_date,
        "endDate": end_date,
        "hydrate": "teams,linescore"
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for date_info in data.get("dates", []):
        for game in date_info["games"]:
            home = game["teams"]["home"]
            away = game["teams"]["away"]
            rows.append({
                "gamePk":     game["gamePk"],
                "date":       game["gameDate"],
                "home_team":  home["team"]["name"],
                "away_team":  away["team"]["name"],
                "status":     game["status"]["abstractGameState"],
                "home_score": home.get("score"),
                "away_score": away.get("score")
            })
    return pd.DataFrame(rows)


def main():
    # Define season window
    start = f"{SEASON}-03-01"
    end   = f"{SEASON}-11-01"
    df    = get_schedule_with_scores(start, end)
    filepath = f"{OUTPUT_DIR}/mlb_schedule_{SEASON}.csv"
    df.to_csv(filepath, index=False)
    print(f"[SUCCESS] Wrote schedule with scores to {filepath}")


if __name__ == "__main__":
    main()