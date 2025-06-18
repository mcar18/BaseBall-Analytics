"""
scripts/load_data_to_db.py

Reads the latest schedule CSV (current season) plus team and player stats
and loads them into a master SQLite DB at data/baseball_analytics.db.
"""

import os
import sys
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# ─── Configuration ─────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH  = DATA_DIR / "baseball_analytics.db"

# Dynamically pick the schedule CSV for the current year
current_year = pd.Timestamp.now().year
SCHEDULE_CSV = DATA_DIR / "raw" / f"mlb_schedule_{current_year}.csv"
TEAM_BATTING_CSV  = DATA_DIR / "raw" / "team_stats" / f"team_batting_{current_year - 1}.csv"
TEAM_PITCHING_CSV = DATA_DIR / "raw" / "team_stats" / f"team_pitching_{current_year - 1}.csv"
PLAYER_BATTING_CSV  = DATA_DIR / "raw" / "player_stats" / f"player_batting_{current_year - 1}.csv"
PLAYER_PITCHING_CSV = DATA_DIR / "raw" / "player_stats" / f"player_pitching_{current_year - 1}.csv"
# ────────────────────────────────────────────────────────────────────────────────

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def file_check(path: Path):
    if not path.exists():
        logging.error(f"Required file not found: {path}")
        sys.exit(1)


def get_engine(db_path: Path):
    return create_engine(f"sqlite:///{db_path}")


def ingest_csv_to_sql(table_name: str, df: pd.DataFrame, engine):
    try:
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        logging.info(f"Ingested {len(df)} rows into '{table_name}'.")
    except SQLAlchemyError as e:
        logging.error(f"Failed to ingest {table_name}: {e}")


def main():
    setup_logging()
    logging.info("Starting database ingestion...")

    # Check files
    for path in [SCHEDULE_CSV, TEAM_BATTING_CSV, TEAM_PITCHING_CSV, PLAYER_BATTING_CSV, PLAYER_PITCHING_CSV]:
        file_check(path)

    engine = get_engine(DB_PATH)
    logging.info("Reading CSV files...")

    df_schedule = pd.read_csv(SCHEDULE_CSV, parse_dates=["date"])
    df_team_bat = pd.read_csv(TEAM_BATTING_CSV)
    df_team_pit = pd.read_csv(TEAM_PITCHING_CSV)
    df_player_bat = pd.read_csv(PLAYER_BATTING_CSV)
    df_player_pit = pd.read_csv(PLAYER_PITCHING_CSV)

    df_team = pd.concat([df_team_bat, df_team_pit], ignore_index=True, sort=False)
    df_player = pd.concat([df_player_bat, df_player_pit], ignore_index=True, sort=False)

    ingest_csv_to_sql("schedule", df_schedule, engine)
    ingest_csv_to_sql("team_stats", df_team, engine)
    ingest_csv_to_sql("player_stats", df_player, engine)

    logging.info(f"Database saved at {DB_PATH}")
    logging.info("Ingestion complete.")


if __name__ == "__main__":
    main()
