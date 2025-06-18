"""
scripts/train_baseline_model.py

Trains a baseline logistic regression model for home-team win probability
using odds-based features only. Drops win_pct features to avoid NaNs.
Evaluates performance and saves the model and diagnostic plots.
"""

import os
import logging
import joblib
import sqlite3

import pandas as pd
import matplotlib.pyplot as plt

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    roc_auc_score,
    brier_score_loss,
    RocCurveDisplay
)
from sklearn.calibration import calibration_curve

# ─── Config ────────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(__file__)
DB_PATH   = os.path.join(BASE_DIR, "data", "baseball_analytics.db")
MODEL_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODEL_DIR, exist_ok=True)
# ────────────────────────────────────────────────────────────────────────────────

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S"
    )


def load_data():
    """
    Loads odds-based features and outcomes from the database.
    """
    conn = sqlite3.connect(DB_PATH)
    df_feat = pd.read_sql("SELECT gamePk, home_odds_avg, away_odds_avg, home_imp_avg, away_imp_avg, home_favorite, date FROM game_features", conn, parse_dates=["date"])
    df_sched = pd.read_sql("SELECT gamePk, home_score, away_score FROM schedule", conn)
    conn.close()

    df = df_feat.merge(df_sched, on="gamePk", how="left")
    df["home_win"] = (df["home_score"] > df["away_score"]).astype(int)
    return df


def train_and_evaluate(df):
    """
    Splits data, trains a logistic regression on odds-only features,
    evaluates metrics, and saves the model and plots.
    """
    features = [
        "home_odds_avg", "away_odds_avg",
        "home_imp_avg",  "away_imp_avg",
        "home_favorite"
    ]

    # Drop rows with any missing odds-based feature or target
    df = df.dropna(subset=features + ["home_win"])

    X = df[features]
    y = df["home_win"]

    # Random split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.3,
        random_state=42
    )

    pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("lr",      LogisticRegression(max_iter=1000, random_state=42))
    ])

    logging.info("Training logistic regression model…")
    pipe.fit(X_train, y_train)

    # predictions
    y_pred = pipe.predict(X_test)
    y_prob = pipe.predict_proba(X_test)[:, 1]

    acc   = accuracy_score(y_test, y_pred)
    auc   = roc_auc_score(y_test, y_prob)
    brier = brier_score_loss(y_test, y_prob)

    logging.info(f"Test Accuracy : {acc:.3f}")
    logging.info(f"Test ROC AUC  : {auc:.3f}")
    logging.info(f"Test Brier    : {brier:.3f}")

    # ROC curve
    RocCurveDisplay.from_predictions(y_test, y_prob)
    plt.title("ROC Curve")
    plt.savefig(os.path.join(MODEL_DIR, "roc_curve.png"))
    plt.clf()

    # Calibration curve
    prob_true, prob_pred = calibration_curve(y_test, y_prob, n_bins=10)
    plt.plot(prob_pred, prob_true, marker='o', linewidth=1)
    plt.plot([0,1], [0,1], "k--", linewidth=1)
    plt.xlabel("Mean Predicted Probability")
    plt.ylabel("Fraction of Positives")
    plt.title("Calibration Curve")
    plt.savefig(os.path.join(MODEL_DIR, "calibration_curve.png"))
    plt.clf()

    # save model
    model_path = os.path.join(MODEL_DIR, "baseline_logreg.joblib")
    joblib.dump(pipe, model_path)
    logging.info(f"Model saved to {model_path}")


def main():
    setup_logging()
    logging.info("Loading data and training model…")
    df = load_data()
    train_and_evaluate(df)
    logging.info("Training and evaluation complete.")

if __name__ == "__main__":
    main()

