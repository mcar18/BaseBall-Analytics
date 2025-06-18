"""
feature_engineering.py

Builds a game-level feature table by joining schedule, odds, and team_stats.
Fixes:
 - Aligns column names (home/away → home_team/away_team)
 - Joins on match_date (YYYY-MM-DD) instead of full datetime
 - Adds Season to schedule
 - Uses groupby.apply to compute mean odds safely

Writes:
  - data/processed/game_features.csv
  - 'game_features' table in baseball_analytics.db
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine

# ─── Config ────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(__file__)
DATA_DIR   = os.path.join(BASE_DIR, "data")
DB_PATH    = os.path.join(DATA_DIR, "baseball_analytics.db")
ODDS_TABLE = "odds"
SCHEDULE   = "schedule"
TEAM_STATS = "team_stats"
PROCESSED  = os.path.join(DATA_DIR, "processed")
os.makedirs(PROCESSED, exist_ok=True)
# ────────────────────────────────────────────────────────────────────────────────

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S"
    )

def amer_to_imp_prob(odds: float) -> float:
    """
    Converts American odds to implied probability.
    Positive: 100 / (odds + 100)
    Negative: -odds / (-odds + 100)
    """
    if odds > 0:
        return 100 / (odds + 100)
    return -odds / (-odds + 100)

def main():
    setup_logging()
    logging.info("Starting feature engineering…")

    engine = create_engine(f"sqlite:///{DB_PATH}")

    # Load tables
    df_sched = pd.read_sql_table(SCHEDULE, engine, parse_dates=["date"])
    df_odds  = pd.read_sql_table(ODDS_TABLE, engine, parse_dates=["match_time"])
    df_ts    = pd.read_sql_table(TEAM_STATS, engine)

    # 1) Rename schedule columns to match odds
    df_sched = df_sched.rename(columns={"home": "home_team", "away": "away_team"})

    # 2) Extract match_date (date only) and add Season
    df_sched["match_date"] = df_sched["date"].dt.date
    df_sched["Season"]     = df_sched["date"].dt.year

    # 3) Extract match_date in odds
    df_odds["match_date"] = df_odds["match_time"].dt.date

    # 4) Filter to moneyline only and compute implied probabilities
    df_ml = df_odds[df_odds["market"] == "h2h"].copy()
    df_ml["imp_prob"] = df_ml["odds"].apply(amer_to_imp_prob)

    # 5) Aggregate odds by game/date
    def agg_odds(group):
        home = group.name[0]
        away = group.name[1]
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

    # 6) Merge schedule + odds aggregates
    df = pd.merge(
        df_sched,
        agg,
        how="inner",
        on=["home_team", "away_team", "match_date"]
    )

    # 7) Compute season win_pct from team_stats
    ts = df_ts.copy()
    ts["win_pct"] = ts["W"] / (ts["W"] + ts["L"])
    home_wp = ts[["Team", "Season", "win_pct"]].rename(
        columns={"Team":"home_team", "win_pct":"home_win_pct"}
    )
    away_wp = ts[["Team", "Season", "win_pct"]].rename(
        columns={"Team":"away_team", "win_pct":"away_win_pct"}
    )

    df = df.merge(home_wp, how="left", on=["home_team", "Season"])
    df = df.merge(away_wp, how="left", on=["away_team", "Season"])

    # 8) Flag home favorite
    df["home_favorite"] = df["home_imp_avg"] > df["away_imp_avg"]

    # 9) Select & reorder
    cols = [
        "gamePk", "Season", "date",
        "home_team", "away_team",
        "home_odds_avg", "away_odds_avg",
        "home_imp_avg", "away_imp_avg",
        "home_win_pct", "away_win_pct",
        "home_favorite"
    ]
    df_final = df[cols]

    # 10) Save CSV + write to DB
    out_csv = os.path.join(PROCESSED, "game_features.csv")
    df_final.to_csv(out_csv, index=False)
    logging.info(f"Saved features CSV to {out_csv}")

    df_final.to_sql("game_features", engine, if_exists="replace", index=False)
    logging.info("Written 'game_features' table to database.")

if __name__ == "__main__":
    main()