"""
load_odds_to_db.py

Reads the latest odds CSV(s) from data/raw/odds/ and loads them into
the 'odds' table in your SQLite database at data/baseball_analytics.db.

If the table exists, it replaces it so you always have the freshest odds.
"""

import os
import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# ─── Configuration ─────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).resolve().parent
DATA_DIR    = BASE_DIR / "data"
DB_PATH     = DATA_DIR / "baseball_analytics.db"
ODDS_DIR    = DATA_DIR / "raw" / "odds"
# ────────────────────────────────────────────────────────────────────────────────

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def find_latest_odds_files(odds_dir: Path):
    """
    Returns a list of all odds CSV files in odds_dir, sorted by name.
    """
    return sorted(odds_dir.glob("odds_*.csv"))

def ingest_odds(df: pd.DataFrame, engine):
    """
    Writes the odds DataFrame to the 'odds' table in the database.
    """
    try:
        df.to_sql("odds", engine, if_exists="replace", index=False)
        logging.info(f"Ingested {len(df)} rows into 'odds' table.")
    except SQLAlchemyError as e:
        logging.error(f"Failed to ingest odds table: {e}")
        raise

def main():
    setup_logging()
    logging.info("Starting odds ingestion into database...")

    # Check DB exists
    if not DB_PATH.exists():
        logging.error(f"Database not found at {DB_PATH}. Run load_data_to_db.py first.")
        return

    # Find odds files
    files = find_latest_odds_files(ODDS_DIR)
    if not files:
        logging.warning(f"No odds CSV files found in {ODDS_DIR}.")
        return

    # You could choose to concatenate multiple days; here we'll use only the latest.
    latest_file = files[-1]
    logging.info(f"Loading odds from {latest_file.name}")

    # Read and ingest
    df = pd.read_csv(latest_file, parse_dates=["match_time"])
    engine = create_engine(f"sqlite:///{DB_PATH}")

    ingest_odds(df, engine)
    logging.info("Odds ingestion complete.")

if __name__ == "__main__":
    main()