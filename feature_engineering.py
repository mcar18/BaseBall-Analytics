"""
scripts/feature_engineering.py

1) Reads schedule, odds, and team_stats from baseball_analytics.db
2) Normalizes team names for a clean join
3) Aggregates moneyline odds into per-game features
4) Computes last-season win_pct for each team
5) Flags home favorites
6) Outputs game_features to CSV and SQLite table
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine

# ─── CONFIG ────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(__file__)
DATA_DIR   = os.path.join(BASE_DIR, "data")
DB_PATH    = os.path.join(DATA_DIR, "baseball_analytics.db")
ODDS_TABLE = "odds"
SCHEDULE   = "schedule"
TEAM_STATS = "team_stats"
PROCESSED  = os.path.join(DATA_DIR, "processed")
os.makedirs(PROCESSED, exist_ok=True)
# ────────────────────────────────────────────────────────────────────────────────

# Map MLB API names → Odds API names
TEAM_NAME_MAP = {
    "Athletics": "Oakland Athletics",
    # add other mappings if needed
}

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S"
    )


def amer_to_imp_prob(odds: float) -> float:
    """American odds → implied probability."""
    if odds > 0:
        return 100 / (odds + 100)
    return -odds / (-odds + 100)


def main():
    setup_logging()
    logging.info("Starting feature engineering…")

    engine = create_engine(f"sqlite:///{DB_PATH}")

    # Load raw tables
    df_sched = pd.read_sql_table(SCHEDULE, engine, parse_dates=["date"])
    df_odds  = pd.read_sql_table(ODDS_TABLE, engine, parse_dates=["match_time"])
    df_ts    = pd.read_sql_table(TEAM_STATS, engine)

    # 1) Rename to match odds schema
    df_sched = df_sched.rename(columns={"home": "home_team", "away": "away_team"})

    # 2) Normalize team names
    df_sched["home_team"] = df_sched["home_team"].replace(TEAM_NAME_MAP)
    df_sched["away_team"] = df_sched["away_team"].replace(TEAM_NAME_MAP)

    # 3) Extract date-only and seasons
    df_sched["match_date"] = df_sched["date"].dt.date
    df_sched["Season"]     = df_sched["date"].dt.year
    df_sched["Prev_Season"] = df_sched["Season"] - 1
    df_odds["match_date"]  = df_odds["match_time"].dt.date

    # 4) Moneyline odds & implied prob
    df_ml = df_odds[df_odds["market"] == "h2h"].copy()
    df_ml["imp_prob"] = df_ml["odds"].apply(amer_to_imp_prob)

    # 5) Aggregate per game
    def agg_odds(group):
        home, away = group.name[0], group.name[1]
        return pd.Series({
            "home_odds_avg": group.loc[group["outcome"] == home, "odds"].mean(),
            "away_odds_avg": group.loc[group["outcome"] == away, "odds"].mean(),
            "home_imp_avg" : group.loc[group["outcome"] == home, "imp_prob"].mean(),
            "away_imp_avg" : group.loc[group["outcome"] == away, "imp_prob"].mean(),
        })

    agg = (
        df_ml
        .groupby(["home_team", "away_team", "match_date"])
        .apply(agg_odds)
        .reset_index()
    )

    # 6) Merge schedule + odds
    df = pd.merge(
        df_sched,
        agg,
        how="inner",
        on=["home_team", "away_team", "match_date"]
    )

    # 7) Last-season win_pct
    ts = df_ts.copy()
    ts["win_pct"] = ts["W"] / (ts["W"] + ts["L"])
    # rename Season column in team_stats to Prev_Season for join
    ts = ts.rename(columns={"Season": "Prev_Season", "Team": "team_name"})
    home_wp = ts[["team_name", "Prev_Season", "win_pct"]].rename(
        columns={"team_name": "home_team", "win_pct": "home_win_pct"}
    )
    away_wp = ts[["team_name", "Prev_Season", "win_pct"]].rename(
        columns={"team_name": "away_team", "win_pct": "away_win_pct"}
    )

    df = df.merge(home_wp, how="left", on=["home_team", "Prev_Season"])
    df = df.merge(away_wp, how="left", on=["away_team", "Prev_Season"])

    # drop Prev_Season helper
    df = df.drop(columns=["Prev_Season"])

    # 8) Flag home favorite
    df["home_favorite"] = df["home_imp_avg"] > df["away_imp_avg"]

    # 9) Final columns
    cols = [
        "gamePk","Season","date","home_team","away_team",
        "home_odds_avg","away_odds_avg",
        "home_imp_avg","away_imp_avg",
        "home_win_pct","away_win_pct",
        "home_favorite"
    ]
    df_final = df[cols]

    # 10) Save outputs
    csv_path = os.path.join(PROCESSED, "game_features.csv")
    df_final.to_csv(csv_path, index=False)
    logging.info(f"Saved features CSV to {csv_path}")

    df_final.to_sql("game_features", engine, if_exists="replace", index=False)
    logging.info("Written 'game_features' table to database.")

if __name__ == "__main__":
    main()