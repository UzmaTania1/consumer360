# consumer360-customer-segmentation
End-to-end Customer Segmentation &amp; CLV Engine using RFM Analysis, Cohort Analysis, and Market Basket Analysis with automated data pipeline and Power BI dashboard.
This project analyzes customer behavior using RFM (Recency, Frequency, Monetary) analysis to segment customers into different groups like Champions, At Risk, and Hibernating.

## 📁 Project Structure

```
consumer360/
│
├── data/
│   ├── raw/                    # Drop raw transaction CSVs here
│   └── processed/              # Auto-generated clean data
│
├── notebooks/
│   └── analysis.ipynb          # Exploratory analysis & model validation
│
├── src/
│   ├── data_cleaning.py        # Cleanse & validate raw transactions
│   ├── rfm.py                  # RFM scoring + segment labelling
│   ├── cohort.py               # Monthly cohort retention matrix
│   ├── market_basket.py        # Apriori association rule mining
│   └── clv.py                  # BG/NBD + Gamma-Gamma CLV prediction
│
├── sql/
│   └── queries.sql             # SQL views for extraction & cohort (MySQL/PostgreSQL)
│
├── dashboard/
│   └── powerbi.pbix            # Power BI dashboard (connect to outputs/)
│
├── outputs/
│   ├── rfm_output.csv          # RFM scores + segment per customer
│   ├── cohort_output.csv       # Tidy retention data
│   ├── cohort_pivot.csv        # Wide pivot for heatmap
│   ├── association_rules.csv   # Market basket rules
│   └── clv_output.csv          # Predicted CLV per customer
│
├── logs/                       # Auto-generated pipeline logs
├── pipeline.py                 # ⚡ Weekly automation runner
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Add your data
Place your raw transactions CSV in `data/raw/transactions.csv`.

**Required columns:**
| Column | Type | Description |
|--------|------|-------------|
| `order_id` | string | Unique order identifier |
| `customer_id` | string | Unique customer identifier |
| `order_date` | date | Purchase date (YYYY-MM-DD) |
| `product_id` | string | Product identifier |
| `product_name` | string | Product display name |
| `category` | string | Product category |
| `quantity` | int | Units purchased |
| `unit_price` | float | Price per unit |
| `region` | string | Geographic region |
| `country` | string | Country |

### 3. Run the full pipeline
```bash
python pipeline.py
```

### 4. Run a single step
```bash
python pipeline.py --step rfm
python pipeline.py --step cohort
python pipeline.py --step basket
python pipeline.py --step clv
```

---

## 📊 Features

### Core Metrics
- Weekly/monthly **sales trends** over time
- **Top products** by revenue and volume
- **Revenue breakdown** by region and country

### RFM Segmentation
Customers are scored 1–5 on Recency, Frequency, and Monetary value, then mapped to 11 segments:

| Segment | Description | Action |
|---------|-------------|--------|
| 🏆 Champions | Best customers — buy often, recently, high spend | Reward & upsell |
| 💛 Loyal Customers | Regular buyers, strong history | Loyalty program |
| 🌱 Potential Loyalist | Recent buyers with growth potential | Nurture |
| 🆕 Recent Users | Just purchased, low frequency | Onboard well |
| 🤞 Promising | Moderate recency, low frequency | Engage |
| ⚠️ Can't Lose Them | High past value but gone quiet | Urgent win-back |
| 😴 Needs Attention | Declining engagement | Re-engagement campaign |
| 💤 Hibernating | Low R+F, moderate M | Low-cost reactivation |
| 😴 About To Sleep | About to churn | Discount offer |
| ❌ Lost | Lowest R+F scores | Suppression list |
| 💰 Price Sensitive | High frequency, low spend | Bundle offers |

**Churn Risk flag** (`is_churn_risk = True`): Can't Lose Them, Needs Attention, About To Sleep, Lost  
**High Value flag** (`is_high_value = True`): Champions, Loyal Customers

### Cohort Analysis
Monthly retention matrix — tracks what % of each acquisition cohort returns in subsequent months. Output is both tidy (for line charts) and pivoted (for heatmap).

### Market Basket Analysis
Uses the Apriori algorithm to find product association rules. Example output:
```
Bread → Butter   |  support: 0.12  |  confidence: 0.65  |  lift: 2.3
```

### Predictive CLV (Customer Lifetime Value)
Uses the **BG/NBD model** (purchase frequency prediction) combined with the **Gamma-Gamma model** (spend prediction) from the `lifetimes` library to estimate expected revenue per customer over the next 12 months.

---

## 🔄 Weekly Automation

Schedule `pipeline.py` to run weekly:

**Linux/Mac (cron):**
```bash
0 6 * * 1  cd /path/to/consumer360 && python pipeline.py
```

**Windows Task Scheduler:** Point to `pipeline.py`, run every Monday at 6 AM.

**GitHub Actions:** See `.github/workflows/weekly_pipeline.yml` (add your data source credentials as secrets).

---

## 📈 Power BI Dashboard

Connect Power BI Desktop to the `outputs/` folder CSVs:
1. `rfm_output.csv` → RFM segment treemap + churn risk table
2. `cohort_pivot.csv` → Retention heatmap
3. `association_rules.csv` → Market basket rules table
4. `clv_output.csv` → CLV distribution + top customer leaderboard

Set **scheduled refresh** in Power BI Service to weekly (Monday morning, after pipeline runs).

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Extraction | SQL (MySQL / PostgreSQL) |
| Processing | Python, Pandas, NumPy |
| RFM & Cohort | Python / Pandas |
| Market Basket | mlxtend (Apriori) |
| CLV Prediction | lifetimes (BG/NBD + Gamma-Gamma) |
| Visualisation | Power BI |



