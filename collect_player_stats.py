# scripts/collect_team_stats.py

import os
import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict

# Output directory
OUTPUT_PATH = "data/raw/team_stats"
os.makedirs(OUTPUT_PATH, exist_ok=True)

MLB_API_BASE = "https://statsapi.mlb.com/api/v1"

def get_team_ids(season: int) -> List[int]:
    """
    Retrieves team IDs for a given season.
    """
    url = f"{MLB_API_BASE}/teams?sportId=1&season={season}"
    response = requests.get(url)
    response.raise_for_status()

    teams = response.json()["teams"]
    return [team["id"] for team in teams]

def get_team_stats(season: int) -> pd.DataFrame:
    """
    Fetches team season stats for all MLB teams.
    """
    team_ids = get_team_ids(season)
    stats = []

    for team_id in team_ids:
        url = f"{MLB_API_BASE}/teams/{team_id}/stats?season={season}&group=hitting,pitching,fielding"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        for group in data.get("stats", []):
            record = {
                "team_id": team_id,
                "season": season,
                "group": group["group"]["displayName"]
            }
            record.update(group["stats"])
            stats.append(record)

    return pd.DataFrame(stats)

def main():
    season = datetime.now().year
    try:
        df = get_team_stats(season)
        filename = f"{OUTPUT_PATH}/mlb_team_stats_{season}.csv"
        df.to_csv(filename, index=False)
        print(f"[SUCCESS] Saved team stats to {filename}")
    except Exception as e:
        print(f"[ERROR] Failed to collect team stats: {e}")

if __name__ == "__main__":
    main()