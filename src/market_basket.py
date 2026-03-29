import pandas as pd
import os
from mlxtend.frequent_patterns import apriori, association_rules

INPUT_PATH = "data/processed/clean_transactions.csv"
OUTPUT_PATH = "outputs/association_rules.csv"


def load_data():
    df = pd.read_csv(INPUT_PATH)
    return df


def build_basket(df):
    print("[basket] Building order-product matrix...")

    basket = (
        df.groupby(["order_id", "product_name"])["quantity"]
        .sum()
        .unstack()
        .fillna(0)
    )

    # Convert to True/False for Apriori
    basket_bool = basket > 0

    print(f"[basket] Matrix shape: {basket_bool.shape} (orders × products)")
    return basket_bool


def generate_rules(basket_bool):
    print("[basket] Running Apriori...")

    frequent_itemsets = apriori(basket_bool, min_support=0.001, use_colnames=True)

    if frequent_itemsets.empty:
        print("[basket] No frequent itemsets found.")
        return pd.DataFrame()

    rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.01)

    if rules.empty:
        print("[basket] No association rules generated.")
        return pd.DataFrame()

    # Convert frozensets to readable strings
    rules["antecedents"] = rules["antecedents"].apply(lambda x: ", ".join(list(x)))
    rules["consequents"] = rules["consequents"].apply(lambda x: ", ".join(list(x)))

    # Keep only useful columns
    rules = rules[
        ["antecedents", "consequents", "support", "confidence", "lift"]
    ].sort_values(by="lift", ascending=False)

    return rules


def run():
    os.makedirs("outputs", exist_ok=True)

    df = load_data()
    basket_bool = build_basket(df)
    rules = generate_rules(basket_bool)

    if not rules.empty:
        rules.to_csv(OUTPUT_PATH, index=False)
        print(f"[basket] Saved {len(rules)} rules -> {OUTPUT_PATH}")
        print("\n=== Top 10 Association Rules ===")
        print(rules.head(10).to_string(index=False))
    else:
        print("[basket] No rules to save.")


if __name__ == "__main__":
    run()