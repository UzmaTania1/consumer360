import pandas as pd
import numpy as np
import os
from lifetimes import BetaGeoFitter, GammaGammaFitter
from lifetimes.utils import summary_data_from_transaction_data

PROCESSED_PATH = os.path.join("data", "processed", "clean_transactions.csv")
OUTPUT_PATH = os.path.join("outputs", "clv_output.csv")

PREDICTION_HORIZON_DAYS = 365   # predict CLV over next 12 months
DISCOUNT_RATE = 0.01            # monthly discount rate (≈12% annually)


def build_rfm_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    `lifetimes` requires a specific summary format:
    frequency, recency (days), T (age of customer in days), monetary_value
    """
    snapshot_date = df["order_date"].max()

    summary = summary_data_from_transaction_data(
        df,
        customer_id_col="customer_id",
        datetime_col="order_date",
        monetary_value_col="line_total",
        observation_period_end=snapshot_date,
        freq="D",
    )
    # Keep only repeat buyers for Gamma-Gamma (monetary model requires frequency > 0)
    return summary


def fit_bgnbd(summary: pd.DataFrame) -> BetaGeoFitter:
    bgf = BetaGeoFitter(penalizer_coef=0.001)
    bgf.fit(summary["frequency"], summary["recency"], summary["T"])
    print("[clv] BG/NBD model fitted.")
    return bgf


def fit_gg(summary: pd.DataFrame) -> GammaGammaFitter:
    # Gamma-Gamma only valid for customers with ≥1 repeat purchase
    repeat_buyers = summary[summary["frequency"] > 0]
    ggf = GammaGammaFitter(penalizer_coef=0.001)
    ggf.fit(repeat_buyers["frequency"], repeat_buyers["monetary_value"])
    print("[clv] Gamma-Gamma model fitted.")
    return ggf


def predict_clv(summary: pd.DataFrame, bgf: BetaGeoFitter, ggf: GammaGammaFitter) -> pd.DataFrame:
    repeat_buyers = summary[summary["frequency"] > 0].copy()

    repeat_buyers["predicted_purchases"] = bgf.conditional_expected_number_of_purchases_up_to_time(
        PREDICTION_HORIZON_DAYS,
        repeat_buyers["frequency"],
        repeat_buyers["recency"],
        repeat_buyers["T"],
    ).round(2)

    repeat_buyers["predicted_clv"] = ggf.customer_lifetime_value(
        bgf,
        repeat_buyers["frequency"],
        repeat_buyers["recency"],
        repeat_buyers["T"],
        repeat_buyers["monetary_value"],
        time=PREDICTION_HORIZON_DAYS / 30,   # convert days → months
        discount_rate=DISCOUNT_RATE,
        freq="D",
    ).round(2)

    return repeat_buyers.reset_index()


def run(path: str = PROCESSED_PATH, output: str = OUTPUT_PATH) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["order_date"])

    print("[clv] Building summary data...")
    summary = build_rfm_summary(df)

    bgf = fit_bgnbd(summary)
    ggf = fit_gg(summary)

    result = predict_clv(summary, bgf, ggf)

    os.makedirs(os.path.dirname(output), exist_ok=True)
    result.to_csv(output, index=False)
    print(f"[clv] {len(result):,} CLV predictions saved → {output}")
    print("\n=== Top 10 Customers by Predicted CLV ===")
    print(result.nlargest(10, "predicted_clv")[["customer_id", "frequency", "monetary_value", "predicted_clv"]].to_string(index=False))

    return result


if __name__ == "__main__":
    run()
