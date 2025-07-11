"""
scripts/collect_multi_season_stats.py

Collects team and player statistics from multiple seasons (2015–current).
Completely clears the entire multi_season directory each run to avoid duplicates.
"""

import os
import shutil
import pandas as pd
from datetime import datetime
from pybaseball import team_batting, team_pitching, batting_stats, pitching_stats

# ─── Configuration ─────────────────────────────────────────────────────────────
START_YEAR = 2015
END_YEAR   = datetime.now().year - 1  # up through last completed season
DATA_DIR   = "data/raw/multi_season"
TEAM_DIR   = os.path.join(DATA_DIR, "team_stats")
PLAYER_DIR = os.path.join(DATA_DIR, "player_stats")

# Completely remove the multi_season directory to ensure no duplicates
if os.path.exists(DATA_DIR):
    shutil.rmtree(DATA_DIR)

# Recreate clean multi-season directories
os.makedirs(TEAM_DIR, exist_ok=True)
os.makedirs(PLAYER_DIR, exist_ok=True)

# Loop through each season and save stats files
for season in range(START_YEAR, END_YEAR + 1):
    # Team batting
    tb = team_batting(season)
    tb["Season"] = season
    tb.to_csv(os.path.join(TEAM_DIR, f"team_batting_{season}.csv"), index=False)

    # Team pitching
    tp = team_pitching(season)
    tp["Season"] = season
    tp.to_csv(os.path.join(TEAM_DIR, f"team_pitching_{season}.csv"), index=False)

    # Player batting (qualified)
    bat = batting_stats(season, qual=100)
    bat["Season"] = season
    bat.to_csv(os.path.join(PLAYER_DIR, f"player_batting_{season}.csv"), index=False)

    # Player pitching (qualified)
    pit = pitching_stats(season, qual=50)
    pit["Season"] = season
    pit.to_csv(os.path.join(PLAYER_DIR, f"player_pitching_{season}.csv"), index=False)

    print(f"[INFO] Saved multi-season stats for {season}")