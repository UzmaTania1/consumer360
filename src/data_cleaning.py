import pandas as pd
import numpy as np
import os

RAW_PATH = os.path.join("data", "raw", "transactions.csv")
PROCESSED_PATH = os.path.join("data", "processed", "clean_transactions.csv")


def load_raw(path: str = RAW_PATH) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["order_date"])
    print(f"[load] {len(df):,} rows loaded from {path}")
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    original_len = len(df)

    # Drop rows missing critical fields
    df = df.dropna(subset=["customer_id", "order_id", "order_date", "product_id"])

    # Remove returns / negatives
    df = df[(df["quantity"] > 0) & (df["unit_price"] > 0)]

    # Standardise dtypes
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["product_name"] = df["product_name"].astype(str).str.strip().str.title()
    df["category"] = df["category"].astype(str).str.strip().str.title()
    df["region"] = df["region"].astype(str).str.strip().str.title()

    # Derived column
    df["line_total"] = (df["quantity"] * df["unit_price"]).round(2)

    # Deduplicate exact duplicate rows
    df = df.drop_duplicates()

    print(f"[clean] {original_len - len(df):,} rows removed → {len(df):,} clean rows")
    return df.reset_index(drop=True)


def save(df: pd.DataFrame, path: str = PROCESSED_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"[save] Saved to {path}")


if __name__ == "__main__":
    df = load_raw()
    df = clean(df)
    save(df)
