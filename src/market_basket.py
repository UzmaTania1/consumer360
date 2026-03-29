import pandas as pd
import os
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder

PROCESSED_PATH = os.path.join("data", "processed", "clean_transactions.csv")
OUTPUT_PATH = os.path.join("outputs", "association_rules.csv")

# Thresholds — tune these based on your dataset size
MIN_SUPPORT = 0.01        # product pair appears in ≥1% of orders
MIN_CONFIDENCE = 0.20     # 20% of buyers of A also bought B
MIN_LIFT = 1.5            # pair appears 1.5× more than random chance


def build_basket(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a boolean one-hot encoded basket matrix:
    rows = orders, columns = products, values = True/False
    """
    basket = (
        df.groupby(["order_id", "product_name"])["quantity"]
        .sum()
        .unstack(fill_value=0)
        .reset_index()
        .drop(columns=["order_id"])
    )
    # Convert quantities to boolean (bought or not)
    basket_bool = basket > 0
    return basket_bool


def run_apriori(basket: pd.DataFrame) -> pd.DataFrame:
    """Run Apriori and extract association rules."""
    frequent_itemsets = apriori(
        basket,
        min_support=MIN_SUPPORT,
        use_colnames=True,
        max_len=3,           # pairs and triplets only
    )
    if frequent_itemsets.empty:
        print("[basket] No frequent itemsets found. Try lowering MIN_SUPPORT.")
        return pd.DataFrame()

    rules = association_rules(frequent_itemsets, metric="lift", min_threshold=MIN_LIFT)
    rules = rules[rules["confidence"] >= MIN_CONFIDENCE]
    rules = rules.sort_values("lift", ascending=False).reset_index(drop=True)

    # Clean up frozensets for readability
    rules["antecedents"] = rules["antecedents"].apply(lambda x: ", ".join(list(x)))
    rules["consequents"] = rules["consequents"].apply(lambda x: ", ".join(list(x)))

    return rules


def run(path: str = PROCESSED_PATH, output: str = OUTPUT_PATH) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["order_date"])

    print("[basket] Building order-product matrix...")
    basket = build_basket(df)
    print(f"[basket] Matrix shape: {basket.shape} (orders × products)")

    rules = run_apriori(basket)
    if rules.empty:
        return rules

    os.makedirs(os.path.dirname(output), exist_ok=True)
    rules.to_csv(output, index=False)
    print(f"[basket] {len(rules)} rules saved → {output}")
    print("\n=== Top 10 Rules by Lift ===")
    print(rules[["antecedents", "consequents", "support", "confidence", "lift"]].head(10).to_string(index=False))

    return rules


if __name__ == "__main__":
    run()
