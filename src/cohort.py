import pandas as pd
import numpy as np
import os

PROCESSED_PATH = os.path.join("data", "processed", "clean_transactions.csv")
OUTPUT_PATH = os.path.join("outputs", "cohort_output.csv")
PIVOT_OUTPUT_PATH = os.path.join("outputs", "cohort_pivot.csv")


def build_cohort(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a tidy dataframe with columns:
      cohort_month | months_since_acquisition | active_customers | cohort_size | retention_rate
    """
    df = df.copy()
    df["order_month"] = df["order_date"].dt.to_period("M")

    # First purchase month per customer = cohort month
    cohort_map = df.groupby("customer_id")["order_month"].min().rename("cohort_month")
    df = df.join(cohort_map, on="customer_id")

    # Months since first purchase
    df["months_since_acquisition"] = (df["order_month"] - df["cohort_month"]).apply(lambda x: x.n)

    # Count unique customers per cohort × period
    cohort_df = (
        df.groupby(["cohort_month", "months_since_acquisition"])["customer_id"]
        .nunique()
        .reset_index()
        .rename(columns={"customer_id": "active_customers"})
    )

    # Cohort size = customers at month 0
    cohort_sizes = (
        cohort_df[cohort_df["months_since_acquisition"] == 0]
        .set_index("cohort_month")["active_customers"]
        .rename("cohort_size")
    )
    cohort_df = cohort_df.join(cohort_sizes, on="cohort_month")
    cohort_df["retention_rate"] = (cohort_df["active_customers"] / cohort_df["cohort_size"]).round(4)

    # Convert Period to string for CSV compatibility
    cohort_df["cohort_month"] = cohort_df["cohort_month"].astype(str)

    return cohort_df


def pivot_cohort(cohort_df: pd.DataFrame) -> pd.DataFrame:
    """Wide-format pivot: rows = cohort months, columns = months_since_acquisition."""
    pivot = cohort_df.pivot_table(
        index="cohort_month",
        columns="months_since_acquisition",
        values="retention_rate",
    )
    pivot.columns = [f"Month_{c}" for c in pivot.columns]
    return pivot.reset_index()


def run(path: str = PROCESSED_PATH, output: str = OUTPUT_PATH) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["order_date"])
    cohort_df = build_cohort(df)

    os.makedirs(os.path.dirname(output), exist_ok=True)
    cohort_df.to_csv(output, index=False)

    pivot = pivot_cohort(cohort_df)
    pivot.to_csv(PIVOT_OUTPUT_PATH, index=False)

    print(f"[cohort] Tidy output   → {output}")
    print(f"[cohort] Pivot output  → {PIVOT_OUTPUT_PATH}")
    print(cohort_df.head(10).to_string(index=False))
    return cohort_df


if __name__ == "__main__":
    run()

