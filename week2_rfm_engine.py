import pandas as pd
import numpy as np
import warnings
from datetime import date
import matplotlib.pyplot as plt
import seaborn as sns
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder

warnings.filterwarnings("ignore")

SNAPSHOT_DATE = date.today()
OUTPUT_DIR = "./outputs/"

# 🔥 UPDATED thresholds (IMPORTANT FIX)
MIN_SUPPORT = 0.01        # was 0.02
MIN_CONFIDENCE = 0.10     # was 0.30


# ============================================================
# SYNTHETIC DATA (same as yours)
# ============================================================

def generate_synthetic_data(n_customers=2000, n_orders=15000):
    print("[DEMO MODE] Generating synthetic data...")
    np.random.seed(42)
    rng = pd.date_range("2023-01-01", "2024-12-31", freq="D")

    customers = pd.DataFrame({
        "customer_key": [f"C{i:05d}" for i in range(1, n_customers + 1)],
        "last_purchase_date": np.random.choice(rng, n_customers),
        "total_orders": np.random.randint(1, 40, n_customers),
        "total_revenue": np.round(np.random.exponential(scale=5000, size=n_customers), 2),
    })

    customers["last_purchase_date"] = pd.to_datetime(customers["last_purchase_date"]).dt.date

    products = ["Bread","Milk","Eggs","Rice","Soap","Shampoo","Laptop","Mouse"]
    basket = []
    for i in range(n_orders):
        items = np.random.choice(products, size=np.random.randint(1,4), replace=False)
        for item in items:
            basket.append([f"ORD{i}", item])

    basket_df = pd.DataFrame(basket, columns=["order_id","product_name"])

    print(f"→ {len(customers)} customers, {len(basket_df)} rows")
    return customers, basket_df


# ============================================================
# RFM CALCULATION
# ============================================================

def calculate_rfm(df):
    print("[RFM] Calculating scores...")

    df["recency_days"] = df["last_purchase_date"].apply(lambda x: (SNAPSHOT_DATE - x).days)
    df["frequency"] = df["total_orders"]
    df["monetary"] = df["total_revenue"]

    df["r_score"] = pd.qcut(df["recency_days"].rank(method="first"), 5, labels=[5,4,3,2,1]).astype(int)
    df["f_score"] = pd.qcut(df["frequency"].rank(method="first"), 5, labels=[1,2,3,4,5]).astype(int)
    df["m_score"] = pd.qcut(df["monetary"].rank(method="first"), 5, labels=[1,2,3,4,5]).astype(int)

    return df


# ============================================================
# 🔥 IMPROVED SEGMENT LOGIC
# ============================================================

def assign_segment(row):
    r, f, m = row["r_score"], row["f_score"], row["m_score"]

    # ✅ STRONG Champions condition
    if r >= 4 and f >= 4 and m >= 4:
        return "Champions"

    elif r >= 3 and f >= 3:
        return "Loyal Customers"

    elif r >= 4:
        return "Recent Users"

    elif r <= 2 and f >= 4:
        return "Can't Lose Them"

    elif r <= 2 and f <= 2:
        return "Lost"

    else:
        return "Others"


# ============================================================
# MARKET BASKET (FIXED)
# ============================================================

def market_basket(basket_df):
    print("[MBA] Running Apriori...")

    transactions = basket_df.groupby("order_id")["product_name"].apply(list).tolist()

    te = TransactionEncoder()
    te_array = te.fit_transform(transactions)
    df = pd.DataFrame(te_array, columns=te.columns_)

    frequent_itemsets = apriori(df, min_support=MIN_SUPPORT, use_colnames=True)

    if frequent_itemsets.empty:
        print("⚠ No itemsets found")
        return pd.DataFrame()

    rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=MIN_CONFIDENCE)

    print(f"→ {len(rules)} rules generated")
    return rules


# ============================================================
# EXPORT
# ============================================================

def export(df):
    import os
    
    # ✅ create folder if not exists
    os.makedirs("outputs", exist_ok=True)

    # ✅ correct path
    file_path = "outputs/rfm_output.csv"
    
    df.to_csv(file_path, index=False)

    print(f"✅ File saved at: {file_path}")


# ============================================================
# MAIN
# ============================================================

def run():
    customers, basket = generate_synthetic_data()

    rfm = calculate_rfm(customers)

    rfm["segment"] = rfm.apply(assign_segment, axis=1)

    print(rfm["segment"].value_counts())

    rules = market_basket(basket)

    export(rfm)

    return rfm


if __name__ == "__main__":
    run()