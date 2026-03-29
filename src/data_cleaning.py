import pandas as pd
import os

def run():
    input_path = "data/raw/transactions.csv"
    output_path = "data/processed/clean_transactions.csv"

    # Load data
    df = pd.read_csv(input_path)
    print(f"[load] {len(df):,} rows loaded from {input_path}")

    # Basic cleaning
    before = len(df)
    df = df.drop_duplicates()
    df = df.dropna()

    # Create line_total column
    df["line_total"] = df["quantity"] * df["unit_price"]

    after = len(df)
    print(f"[clean] {before - after} rows removed -> {after:,} clean rows")

    # Save cleaned data
    os.makedirs("data/processed", exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"[save] Saved to {output_path}")

if __name__ == "__main__":
    run()