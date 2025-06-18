"""
collect_player_stats_pybaseball.py

Collects MLB player-level batting and pitching statistics using the pybaseball library.
Saves as CSVs in data/raw/player_stats/.
"""

import os
import logging
from datetime import datetime

import pandas as pd
from pybaseball import batting_stats, pitching_stats

# ─── Configuration ─────────────────────────────────────────────────────────────
OUTPUT_DIR = "data/raw/player_stats"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Use last fully completed season by default
SEASON = datetime.now().year - 1

# Qualifiers: minimum plate appearances for batters, minimum innings for pitchers
DEFAULT_BAT_QUAL = 100   # e.g. 100 PA
DEFAULT_PIT_QUAL = 50    # e.g. 50 IP
# ────────────────────────────────────────────────────────────────────────────────

def get_player_batting_stats(season: int, qual: int = DEFAULT_BAT_QUAL) -> pd.DataFrame:
    """
    Fetch player batting stats for `season` via pybaseball.

    Parameters:
        season: Year of the MLB season to fetch.
        qual:   Minimum plate appearances qualifier.

    Returns:
        DataFrame of hitters with Season and Stat_Type columns added.
    """
    df = batting_stats(season, qual=qual)
    df["Season"] = season
    df["Stat_Type"] = "Batting"
    return df

def get_player_pitching_stats(season: int, qual: int = DEFAULT_PIT_QUAL) -> pd.DataFrame:
    """
    Fetch player pitching stats for `season` via pybaseball.

    Parameters:
        season: Year of the MLB season to fetch.
        qual:   Minimum innings pitched qualifier.

    Returns:
        DataFrame of pitchers with Season and Stat_Type columns added.
    """
    df = pitching_stats(season, qual=qual)
    df["Season"] = season
    df["Stat_Type"] = "Pitching"
    return df

def save_stats(df: pd.DataFrame, stat: str, season: int) -> None:
    """
    Saves `df` to a CSV named player_{stat}_{season}.csv under OUTPUT_DIR.
    """
    filename = f"player_{stat}_{season}.csv"
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    logging.info(f"Saved player {stat} stats to {path}")

def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    logging.info(f"Collecting player stats for season {SEASON}")

    # Batting stats
    try:
        bat_df = get_player_batting_stats(SEASON)
        save_stats(bat_df, "batting", SEASON)
    except Exception as e:
        logging.error(f"Failed to fetch batting stats: {e}")

    # Pitching stats
    try:
        pit_df = get_player_pitching_stats(SEASON)
        save_stats(pit_df, "pitching", SEASON)
    except Exception as e:
        logging.error(f"Failed to fetch pitching stats: {e}")

if __name__ == "__main__":
    main()