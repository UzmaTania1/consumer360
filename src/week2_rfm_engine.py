"""
============================================================
CONSUMER360 | PROJECT 1: RETAIL ANALYTICS
WEEK 2: RFM SEGMENTATION ENGINE + MARKET BASKET ANALYSIS
Zaalima Development Pvt. Ltd.
============================================================

Requirements:
    pip install pandas numpy sqlalchemy psycopg2-binary mlxtend lifetimes matplotlib seaborn

Week 2 Deliverables:
  1. Pull cleaned data from SQL (vw_customer_purchase_summary)
  2. Calculate R, F, M scores (1-5 scale per customer)
  3. Assign segment labels (Champions, Hibernating, etc.)
  4. Market Basket Analysis via Association Rule Mining (mlxtend)
  5. Validation: Champion segment = top-spending customers
  6. Export results for Power BI ingestion
"""

import pandas as pd
import numpy as np
import warnings
from datetime import datetime, date
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder

warnings.filterwarnings("ignore")

# ============================================================
# CONFIGURATION
# ============================================================
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "consumer360_db",
    "user":     "your_user",
    "password": "your_password",
}

SNAPSHOT_DATE = date.today()          # RFM recency reference point
OUTPUT_DIR    = "./outputs/"          # CSVs exported here for Power BI
MIN_SUPPORT   = 0.02                  # Market Basket: min 2% transaction frequency
MIN_CONFIDENCE= 0.30                  # Market Basket: min 30% rule confidence


# ============================================================
# SECTION 1: DATABASE CONNECTION + DATA EXTRACTION
# ============================================================

def get_engine():
    """Build SQLAlchemy engine from config."""
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(url, pool_pre_ping=True)


def extract_customer_summary(engine) -> pd.DataFrame:
    """
    Pull the cleaned customer purchase summary from the SQL view
    created in Week 1. This is the base for RFM scoring.
    """
    query = """
        SELECT
            customer_id,
            customer_key,
            full_name,
            email,
            region_name,
            last_purchase_date,
            total_orders,
            total_revenue,
            avg_order_value,
            first_purchase_date
        FROM vw_customer_purchase_summary
        WHERE total_orders > 0
          AND total_revenue > 0
    """
    print("[1/6] Extracting customer purchase summary from SQL...")
    df = pd.read_sql(text(query), engine)
    print(f"      → {len(df):,} customers loaded.")
    return df


def extract_basket_data(engine) -> pd.DataFrame:
    """
    Pull order-level product data for Market Basket Analysis.
    Each row = one item in one order.
    """
    query = """
        SELECT
            fs.order_id,
            dp.product_name,
            dp.category
        FROM fact_sales fs
        JOIN dim_product dp ON fs.product_id = dp.product_id
        ORDER BY fs.order_id
    """
    print("[2/6] Extracting basket data for Market Basket Analysis...")
    df = pd.read_sql(text(query), engine)
    print(f"      → {len(df):,} order-item rows loaded.")
    return df


# ============================================================
# SECTION 2: GENERATE SYNTHETIC DATA (for demo / testing)
# Used when no live DB is connected.
# ============================================================

def generate_synthetic_data(n_customers=2000, n_orders=15000) -> tuple:
    """
    Generates realistic synthetic customer + basket data.
    Remove this in production and use extract_*() functions above.
    """
    print("[DEMO MODE] Generating synthetic data...")
    np.random.seed(42)
    rng = pd.date_range("2023-01-01", "2024-12-31", freq="D")

    # -- Customer summary --
    customers = pd.DataFrame({
        "customer_id":        range(1, n_customers + 1),
        "customer_key":       [f"C{i:05d}" for i in range(1, n_customers + 1)],
        "full_name":          [f"Customer_{i}" for i in range(1, n_customers + 1)],
        "email":              [f"user{i}@email.com" for i in range(1, n_customers + 1)],
        "region_name":        np.random.choice(["North", "South", "East", "West", "Central"], n_customers),
        "last_purchase_date": np.random.choice(rng, n_customers),
        "total_orders":       np.random.randint(1, 40, n_customers),
        "total_revenue":      np.round(np.random.exponential(scale=5000, size=n_customers), 2),
        "avg_order_value":    np.round(np.random.uniform(200, 3000, n_customers), 2),
        "first_purchase_date":np.random.choice(rng[:180], n_customers),
    })
    customers["last_purchase_date"]  = pd.to_datetime(customers["last_purchase_date"]).dt.date
    customers["first_purchase_date"] = pd.to_datetime(customers["first_purchase_date"]).dt.date

    # -- Basket data --
    products = [
        "Bread", "Butter", "Milk", "Eggs", "Rice", "Dal",
        "Shampoo", "Soap", "Toothpaste", "Chips",
        "Laptop", "Mouse", "Keyboard", "Headphones",
        "Shirt", "Jeans", "Shoes", "Watch",
    ]
    basket_rows = []
    order_ids = [f"ORD{j:06d}" for j in range(1, n_orders + 1)]
    for oid in order_ids:
        items = np.random.choice(products, size=np.random.randint(1, 6), replace=False)
        for item in items:
            basket_rows.append({"order_id": oid, "product_name": item})
    basket_df = pd.DataFrame(basket_rows)

    print(f"      → {len(customers):,} synthetic customers, {len(basket_df):,} basket rows.")
    return customers, basket_df


# ============================================================
# SECTION 3: RFM SCORING ENGINE
# ============================================================

def calculate_rfm_scores(df: pd.DataFrame, snapshot_date=None) -> pd.DataFrame:
    """
    Core RFM calculation:
      - Recency  : Days since last purchase (lower = better)
      - Frequency: Total number of distinct orders
      - Monetary : Total revenue generated

    Each metric is scored 1-5 using quintiles.
    Final RFM score = weighted composite (equal weight here).
    """
    print("[3/6] Calculating RFM scores...")
    if snapshot_date is None:
        snapshot_date = date.today()

    df = df.copy()
    df["last_purchase_date"] = pd.to_datetime(df["last_purchase_date"]).dt.date

    # Raw R, F, M values
    df["recency_days"]  = df["last_purchase_date"].apply(
        lambda x: (snapshot_date - x).days
    )
    df["frequency"]     = df["total_orders"].astype(int)
    df["monetary"]      = df["total_revenue"].astype(float)

    # --- Score each on 1-5 scale using quintile cuts ---
    # Recency: LOWER days = better -> rank reversed
    df["r_score"] = pd.qcut(df["recency_days"].rank(method="first"),
                            q=5, labels=[5, 4, 3, 2, 1]).astype(int)
    df["f_score"] = pd.qcut(df["frequency"].rank(method="first"),
                            q=5, labels=[1, 2, 3, 4, 5]).astype(int)
    df["m_score"] = pd.qcut(df["monetary"].rank(method="first"),
                            q=5, labels=[1, 2, 3, 4, 5]).astype(int)

    # Composite RFM score string (e.g., "555" = champion)
    df["rfm_score"] = (
        df["r_score"].astype(str) +
        df["f_score"].astype(str) +
        df["m_score"].astype(str)
    )

    # Numeric RFM score (avg of the three)
    df["rfm_numeric"] = (
        df["r_score"] * 0.33 +
        df["f_score"] * 0.33 +
        df["m_score"] * 0.34
    ).round(2)

    print(f"      → RFM scores computed for {len(df):,} customers.")
    return df


# ============================================================
# SECTION 4: SEGMENT LABELLING
# ============================================================

SEGMENT_RULES = {
    # segment_name       : (r_min, r_max, fm_min, fm_max)
    # FM average used as proxy for F+M combined score
    "Champions":          (4, 5, 4, 5),
    "Loyal Customers":    (3, 5, 3, 5),
    "Potential Loyalist": (3, 5, 2, 3),
    "Recent Users":       (4, 5, 1, 2),
    "Promising":          (3, 4, 1, 2),
    "Needs Attention":    (2, 3, 3, 4),
    "About To Sleep":     (2, 3, 1, 3),
    "Can't Lose Them":    (1, 2, 4, 5),
    "Hibernating":        (1, 2, 2, 3),
    "Price Sensitive":    (4, 5, 1, 1),
    "Lost":               (1, 2, 1, 2),
}


def assign_segment(row: pd.Series) -> str:
    """
    Assigns a customer to a segment based on their R and FM scores.
    Uses an ordered priority list so a customer gets the highest-value
    matching label (Champions checked first, Lost checked last).
    """
    r  = row["r_score"]
    fm = round((row["f_score"] + row["m_score"]) / 2)

    for segment, (r_min, r_max, fm_min, fm_max) in SEGMENT_RULES.items():
        if r_min <= r <= r_max and fm_min <= fm <= fm_max:
            return segment
    return "Other"


def label_segments(df: pd.DataFrame) -> pd.DataFrame:
    """Apply segment labels to all customers."""
    print("[4/6] Assigning customer segments...")
    df = df.copy()
    df["segment"] = df.apply(assign_segment, axis=1)
    summary = df["segment"].value_counts()
    print("      → Segment distribution:")
    for seg, cnt in summary.items():
        print(f"         {seg:<25} {cnt:>6,} customers")
    return df


def validate_champions(df: pd.DataFrame):
    """
    VALIDATION CHECK (Week 2 Critical Review):
    Confirm 'Champions' segment contains genuinely top-spending customers.
    """
    print("\n[VALIDATION] Champions vs. Overall Revenue Distribution")
    print("-" * 55)

    champs = df[df["segment"] == "Champions"]
    others = df[df["segment"] != "Champions"]

    champ_avg_rev  = champs["monetary"].mean()
    others_avg_rev = others["monetary"].mean()
    champ_pct      = len(champs) / len(df) * 100
    champ_rev_pct  = champs["monetary"].sum() / df["monetary"].sum() * 100

    print(f"  Champions     : {len(champs):>6,} customers ({champ_pct:.1f}% of base)")
    print(f"  Avg Revenue   : ₹{champ_avg_rev:>10,.2f}  (Champions)")
    print(f"  Avg Revenue   : ₹{others_avg_rev:>10,.2f}  (All Others)")
    print(f"  Revenue Share : {champ_rev_pct:.1f}% of total revenue from Champions")

    if champ_avg_rev > others_avg_rev * 2:
        print("  ✅ PASS - Champions are genuine top-spenders (2x+ avg revenue).")
    else:
        print("  ⚠️  WARNING - Champions may need re-calibration (less than 2x premium).")
    print()


# ============================================================
# SECTION 5: MARKET BASKET ANALYSIS
# ============================================================

def run_market_basket_analysis(
    basket_df: pd.DataFrame,
    min_support: float = MIN_SUPPORT,
    min_confidence: float = MIN_CONFIDENCE
) -> pd.DataFrame:
    """
    Association Rule Mining using the Apriori algorithm (mlxtend).

    Steps:
      1. Pivot basket data into a boolean order-product matrix
      2. Run Apriori to find frequent itemsets
      3. Generate association rules (antecedent -> consequent)
      4. Sort by lift (strength of association)

    Returns: DataFrame of rules sorted by lift descending.
    """
    print("[5/6] Running Market Basket Analysis (Apriori)...")

    # Build a list of transactions: each element = list of products in one order
    transactions = (
        basket_df.groupby("order_id")["product_name"]
        .apply(list)
        .tolist()
    )

    # Encode into boolean matrix
    te = TransactionEncoder()
    te_array = te.fit_transform(transactions)
    basket_matrix = pd.DataFrame(te_array, columns=te.columns_)

    print(f"      → Basket matrix: {basket_matrix.shape[0]:,} orders × {basket_matrix.shape[1]} products")

    # Apriori: find frequent itemsets
    frequent_itemsets = apriori(
        basket_matrix,
        min_support=min_support,
        use_colnames=True
    )
    print(f"      → {len(frequent_itemsets):,} frequent itemsets found (support ≥ {min_support:.0%})")

    if len(frequent_itemsets) == 0:
        print("      ⚠️  No frequent itemsets. Try lowering MIN_SUPPORT.")
        return pd.DataFrame()

    # Generate association rules
    rules = association_rules(
        frequent_itemsets,
        metric="confidence",
        min_threshold=min_confidence
    )
    rules = rules.sort_values("lift", ascending=False).reset_index(drop=True)

    # Clean up frozenset display
    rules["antecedents_str"] = rules["antecedents"].apply(lambda x: ", ".join(sorted(x)))
    rules["consequents_str"] = rules["consequents"].apply(lambda x: ", ".join(sorted(x)))

    print(f"      → {len(rules):,} rules generated (confidence ≥ {min_confidence:.0%})")
    print("\n  🔝 Top 10 Association Rules by Lift:")
    print(f"  {'Antecedent':<20} {'→ Consequent':<20} {'Support':>8} {'Confidence':>10} {'Lift':>8}")
    print("  " + "-" * 70)
    for _, r in rules.head(10).iterrows():
        print(f"  {r['antecedents_str']:<20} → {r['consequents_str']:<20} "
              f"{r['support']:>8.3f} {r['confidence']:>10.3f} {r['lift']:>8.3f}")
    return rules


# ============================================================
# SECTION 6: EXPORT OUTPUTS FOR POWER BI
# ============================================================

def export_outputs(rfm_df: pd.DataFrame, rules_df: pd.DataFrame):
    """
    Export final processed DataFrames to CSV files
    ready for ingestion into Power BI (Week 3).
    """
    import os
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"[6/6] Exporting outputs to '{OUTPUT_DIR}'...")

    # --- RFM Customer Segments ---
    rfm_export_cols = [
        "customer_id", "customer_key", "full_name", "email",
        "region_name", "recency_days", "frequency", "monetary",
        "r_score", "f_score", "m_score", "rfm_score", "rfm_numeric",
        "segment", "last_purchase_date", "first_purchase_date",
        "avg_order_value"
    ]
    rfm_path = OUTPUT_DIR + "rfm_customer_segments.csv"
    rfm_df[rfm_export_cols].to_csv(rfm_path, index=False)
    print(f"      ✅ RFM segments → {rfm_path}  ({len(rfm_df):,} rows)")

    # --- Segment Summary for Executive Dashboard ---
    seg_summary = rfm_df.groupby("segment").agg(
        customer_count=("customer_id", "count"),
        avg_recency=("recency_days", "mean"),
        avg_frequency=("frequency", "mean"),
        avg_monetary=("monetary", "mean"),
        total_revenue=("monetary", "sum"),
    ).round(2).reset_index()
    seg_summary["revenue_pct"] = (
        seg_summary["total_revenue"] / seg_summary["total_revenue"].sum() * 100
    ).round(2)
    seg_path = OUTPUT_DIR + "rfm_segment_summary.csv"
    seg_summary.to_csv(seg_path, index=False)
    print(f"      ✅ Segment summary → {seg_path}")

    # --- Market Basket Rules ---
    if not rules_df.empty:
        rules_path = OUTPUT_DIR + "market_basket_rules.csv"
        rules_export = rules_df[[
            "antecedents_str", "consequents_str",
            "support", "confidence", "lift", "leverage", "conviction"
        ]].rename(columns={
            "antecedents_str": "if_customer_buys",
            "consequents_str": "they_also_buy"
        })
        rules_export.to_csv(rules_path, index=False)
        print(f"      ✅ Basket rules  → {rules_path}  ({len(rules_export):,} rules)")

    print("\n  Power BI Instructions:")
    print("  1. Open Power BI Desktop → Get Data → Text/CSV")
    print("  2. Load: rfm_customer_segments.csv (for RFM Matrix visual)")
    print("  3. Load: rfm_segment_summary.csv   (for executive KPI cards)")
    print("  4. Load: market_basket_rules.csv   (for product recommendation table)")


# ============================================================
# SECTION 7: VISUALIZATIONS (optional, for review & validation)
# ============================================================

def plot_rfm_matrix(rfm_df: pd.DataFrame):
    """Scatter plot: Recency vs Monetary, colored by segment."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    fig.suptitle("Consumer360 — RFM Analysis", fontsize=16, fontweight="bold", y=1.02)

    # ---- Plot 1: Segment distribution bar chart ----
    seg_counts = rfm_df["segment"].value_counts()
    colors = plt.cm.Set3(np.linspace(0, 1, len(seg_counts)))
    axes[0].barh(seg_counts.index, seg_counts.values, color=colors)
    axes[0].set_title("Customer Count by Segment", fontsize=13)
    axes[0].set_xlabel("Number of Customers")
    for i, v in enumerate(seg_counts.values):
        axes[0].text(v + 5, i, str(v), va="center", fontsize=9)

    # ---- Plot 2: RFM Score Heatmap (R vs F, avg M as color) ----
    heatmap_data = (
        rfm_df.groupby(["r_score", "f_score"])["monetary"]
        .mean()
        .unstack(fill_value=0)
    )
    sns.heatmap(
        heatmap_data, ax=axes[1], cmap="YlOrRd",
        annot=True, fmt=".0f", linewidths=0.5,
        cbar_kws={"label": "Avg Monetary (₹)"}
    )
    axes[1].set_title("Avg Revenue: Recency Score vs Frequency Score", fontsize=13)
    axes[1].set_xlabel("Frequency Score (1=Low, 5=High)")
    axes[1].set_ylabel("Recency Score (1=Long ago, 5=Recent)")

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR + "rfm_analysis_plot.png", dpi=150, bbox_inches="tight")
    print(f"      📊 Plot saved → {OUTPUT_DIR}rfm_analysis_plot.png")
    plt.show()


# ============================================================
# MAIN PIPELINE ORCHESTRATOR
# ============================================================

def run_pipeline(use_synthetic: bool = True):
    """
    Main orchestration function.
    Set use_synthetic=False and configure DB_CONFIG to run
    against the actual PostgreSQL database from Week 1.
    """
    print("=" * 60)
    print("  CONSUMER360 | Week 2 Pipeline Starting")
    print(f"  Snapshot Date: {SNAPSHOT_DATE}")
    print("=" * 60)

    if use_synthetic:
        # --- DEMO / TEST MODE ---
        customers_df, basket_df = generate_synthetic_data()
    else:
        # --- PRODUCTION MODE: Connect to Week 1 PostgreSQL DB ---
        engine = get_engine()
        customers_df = extract_customer_summary(engine)
        basket_df    = extract_basket_data(engine)

    # Step 1: Calculate RFM scores
    rfm_df = calculate_rfm_scores(customers_df, snapshot_date=SNAPSHOT_DATE)

    # Step 2: Assign segment labels
    rfm_df = label_segments(rfm_df)

    # Step 3: Validation check (Week 2 Critical Review Point)
    validate_champions(rfm_df)

    # Step 4: Market Basket Analysis
    rules_df = run_market_basket_analysis(basket_df)

    # Step 5: Export for Power BI
    export_outputs(rfm_df, rules_df)

    # Step 6: Visualizations
    try:
        import os
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        plot_rfm_matrix(rfm_df)
    except Exception as e:
        print(f"  (Plot skipped: {e})")

    print("\n" + "=" * 60)
    print("  ✅  Week 2 Pipeline Complete.")
    print(f"  📁  Outputs saved to: {OUTPUT_DIR}")
    print("=" * 60)

    return rfm_df, rules_df


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    # Change use_synthetic=False and fill DB_CONFIG for production
    rfm_results, basket_rules = run_pipeline(use_synthetic=True)

    # Quick preview
    print("\n--- Sample RFM Output (5 rows) ---")
    preview_cols = ["customer_key", "recency_days", "frequency",
                    "monetary", "r_score", "f_score", "m_score", "segment"]
    print(rfm_results[preview_cols].head().to_string(index=False))
