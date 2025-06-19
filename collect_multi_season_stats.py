# collect_multi_season_stats.py

import os
import pandas as pd
from datetime import datetime
from pybaseball import team_batting, team_pitching, batting_stats, pitching_stats

YEARS = list(range(2015, datetime.now().year))  # e.g. 2015–2024
DATA_DIR = "data/raw/multi_season"
os.makedirs(f"{DATA_DIR}/team_stats",    exist_ok=True)
os.makedirs(f"{DATA_DIR}/player_stats",  exist_ok=True)

for season in YEARS:
    # Team
    tb = team_batting(season)
    tb["Season"] = season
    tb.to_csv(f"{DATA_DIR}/team_stats/team_batting_{season}.csv", index=False)

    tp = team_pitching(season)
    tp["Season"] = season
    tp.to_csv(f"{DATA_DIR}/team_stats/team_pitching_{season}.csv", index=False)

    # Players (qualified)
    bat = batting_stats(season, qual=100)
    bat["Season"] = season
    bat.to_csv(f"{DATA_DIR}/player_stats/player_batting_{season}.csv", index=False)

    pit = pitching_stats(season, qual=50)
    pit["Season"] = season
    pit.to_csv(f"{DATA_DIR}/player_stats/player_pitching_{season}.csv", index=False)

    print(f"Saved stats for {season}")