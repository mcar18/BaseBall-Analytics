import os
import requests
import pandas as pd
from datetime import datetime
#Test
# Create directory structure
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

# Example: MLB Stats API endpoint
MLB_API_BASE = "https://statsapi.mlb.com/api/v1"

def get_schedule(start_date, end_date):
    """Fetch MLB schedule between dates"""
    url = f"{MLB_API_BASE}/schedule"
    params = {
        "sportId": 1,
        "startDate": start_date,
        "endDate": end_date
    }
    response = requests.get(url, params=params)
    data = response.json()
    games = []

    for date_info in data.get("dates", []):
        for game in date_info["games"]:
            games.append({
                "gamePk": game["gamePk"],
                "date": game["gameDate"],
                "home": game["teams"]["home"]["team"]["name"],
                "away": game["teams"]["away"]["team"]["name"],
                "status": game["status"]["abstractGameState"]
            })

    return pd.DataFrame(games)

# Example usage
if __name__ == "__main__":
    df_schedule = get_schedule("2024-03-28", "2024-10-01")
    df_schedule.to_csv("data/raw/mlb_schedule_2024.csv", index=False)
    print("Saved MLB schedule.")