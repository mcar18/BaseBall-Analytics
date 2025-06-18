"""
collect_team_stats_pybaseball.py

Collects MLB team-level batting and pitching statistics using the pybaseball library.
Saves as CSVs in data/raw/team_stats/.
"""

import os
import logging
from datetime import datetime

import pandas as pd
from pybaseball import team_batting, team_pitching

# ─── Configuration ─────────────────────────────────────────────────────────────
OUTPUT_DIR = "data/raw/team_stats"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# If you want the current year anyway, change to datetime.now().year
DEFAULT_SEASON = datetime.now().year - 1  # last fully completed season
# ────────────────────────────────────────────────────────────────────────────────

def get_team_batting_stats(season: int) -> pd.DataFrame:
    """
    Fetch team batting stats for `season` via pybaseball.
    Returns a DataFrame with an added Season and Stat_Type column.
    """
    df = team_batting(season)
    df["Season"] = season
    df["Stat_Type"] = "Batting"
    return df

def get_team_pitching_stats(season: int) -> pd.DataFrame:
    """
    Fetch team pitching stats for `season` via pybaseball.
    Returns a DataFrame with an added Season and Stat_Type column.
    """
    df = team_pitching(season)
    df["Season"] = season
    df["Stat_Type"] = "Pitching"
    return df

def save_stats(df: pd.DataFrame, stat: str, season: int) -> None:
    """
    Saves `df` to a CSV named team_{stat}_{season}.csv under OUTPUT_DIR.
    """
    path = os.path.join(OUTPUT_DIR, f"team_{stat}_{season}.csv")
    df.to_csv(path, index=False)
    logging.info(f"Saved {stat} stats to {path}")

def main():
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    season = DEFAULT_SEASON
    logging.info(f"Collecting team stats for season {season}")

    # Batting
    try:
        bat_df = get_team_batting_stats(season)
        save_stats(bat_df, "batting", season)
    except Exception as e:
        logging.error(f"Failed to fetch batting stats: {e}")

    # Pitching
    try:
        pit_df = get_team_pitching_stats(season)
        save_stats(pit_df, "pitching", season)
    except Exception as e:
        logging.error(f"Failed to fetch pitching stats: {e}")

if __name__ == "__main__":
    main()