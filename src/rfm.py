import pandas as pd
import numpy as np
import os
from datetime import date

PROCESSED_PATH = os.path.join("data", "processed", "clean_transactions.csv")
OUTPUT_PATH = os.path.join("outputs", "rfm_output.csv")

# Segment mapping based on R+F scores  (M is captured in monetary column)
SEGMENT_MAP = {
    (5, 5): "Champions",
    (5, 4): "Champions",
    (4, 5): "Champions",
    (4, 4): "Loyal Customers",
    (3, 5): "Loyal Customers",
    (5, 3): "Loyal Customers",
    (5, 2): "Potential Loyalist",
    (4, 3): "Potential Loyalist",
    (4, 2): "Potential Loyalist",
    (3, 3): "Potential Loyalist",
    (5, 1): "Recent Users",
    (4, 1): "Recent Users",
    (3, 1): "Promising",
    (3, 2): "Promising",
    (2, 5): "Can't Lose Them",
    (2, 4): "Can't Lose Them",
    (1, 5): "Can't Lose Them",
    (1, 4): "Can't Lose Them",
    (2, 3): "Needs Attention",
    (2, 2): "Hibernating",
    (2, 1): "Hibernating",
    (1, 3): "About To Sleep",
    (1, 2): "About To Sleep",
    (1, 1): "Lost",
    (2, 1): "Hibernating",
    (3, 4): "Price Sensitive",
    (3, 5): "Loyal Customers",
}

# Churn-risk segments (used to drive retention campaigns)
CHURN_RISK_SEGMENTS = {"Can't Lose Them", "Needs Attention", "About To Sleep", "Lost"}
HIGH_VALUE_SEGMENTS = {"Champions", "Loyal Customers"}


def compute_rfm(df: pd.DataFrame, snapshot_date: date = None) -> pd.DataFrame:
    """Aggregate transactions into one RFM row per customer."""
    if snapshot_date is None:
        snapshot_date = df["order_date"].max().date()

    snapshot = pd.Timestamp(snapshot_date)

    rfm = (
        df.groupby("customer_id")
        .agg(
            last_purchase_date=("order_date", "max"),
            frequency=("order_id", "nunique"),
            monetary=("line_total", "sum"),
        )
        .reset_index()
    )
    rfm["recency_days"] = (snapshot - rfm["last_purchase_date"]).dt.days
    rfm["monetary"] = rfm["monetary"].round(2)
    return rfm


def score_rfm(rfm: pd.DataFrame) -> pd.DataFrame:
    """Assign 1-5 scores for R, F, M using quintiles."""
    rfm = rfm.copy()

    # Recency: lower days = better score (reverse rank)
    rfm["r_score"] = pd.qcut(rfm["recency_days"], q=5, labels=[5, 4, 3, 2, 1]).astype(int)
    rfm["f_score"] = pd.qcut(rfm["frequency"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5]).astype(int)
    rfm["m_score"] = pd.qcut(rfm["monetary"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5]).astype(int)

    rfm["rfm_score"] = rfm["r_score"].astype(str) + rfm["f_score"].astype(str) + rfm["m_score"].astype(str)
    return rfm


def assign_segments(rfm: pd.DataFrame) -> pd.DataFrame:
    """Map R+F score pairs to human-readable segment labels."""
    rfm = rfm.copy()
    rfm["segment"] = rfm.apply(
        lambda row: SEGMENT_MAP.get((row["r_score"], row["f_score"]), "Needs Attention"),
        axis=1,
    )
    rfm["is_churn_risk"] = rfm["segment"].isin(CHURN_RISK_SEGMENTS)
    rfm["is_high_value"] = rfm["segment"].isin(HIGH_VALUE_SEGMENTS)
    return rfm


def run(path: str = PROCESSED_PATH, output: str = OUTPUT_PATH) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["order_date"])

    rfm = compute_rfm(df)
    rfm = score_rfm(rfm)
    rfm = assign_segments(rfm)

    os.makedirs(os.path.dirname(output), exist_ok=True)
    rfm.to_csv(output, index=False)
    print(f"[rfm] Saved {len(rfm):,} customer records → {output}")

    # Summary
    print("\n=== Segment Distribution ===")
    print(rfm["segment"].value_counts().to_string())
    print(f"\nChurn Risks   : {rfm['is_churn_risk'].sum():,}")
    print(f"High Value    : {rfm['is_high_value'].sum():,}")

    return rfm


if __name__ == "__main__":
    run()
