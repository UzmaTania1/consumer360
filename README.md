# consumer360-customer-segmentation
End-to-end Customer Segmentation &amp; CLV Engine using RFM Analysis, Cohort Analysis, and Market Basket Analysis with automated data pipeline and Power BI dashboard.
This project analyzes customer behavior using RFM (Recency, Frequency, Monetary) analysis to segment customers into different groups like Champions, At Risk, and Hibernating.

## 🛠️ Tools & Technologies
- Python (Pandas, NumPy)
- SQL
- Matplotlib / Seaborn

## 📊 Features
- Data cleaning and preprocessing
- RFM score calculation
- Customer segmentation
- Basic visualization

## 🎯 Business Use Case
Helps businesses identify high-value customers and take action to reduce customer churn.

## 🚀 Future Improvements
- Power BI Dashboard
- Market Basket Analysis
- Customer Lifetime Value (CLV)


consumer360/
├── data/raw/          ← .gitkeep
├── data/processed/    ← .gitkeep
├── notebooks/         ← .gitkeep
├── src/
│   ├── data_cleaning.py
│   ├── rfm.py
│   ├── cohort.py
│   ├── market_basket.py
│   └── clv.py         ← was 100% missing before
├── sql/queries.sql
├── dashboard/         ← .gitkeep
├── outputs/           ← .gitkeep
├── logs/              ← .gitkeep
├── pipeline.py
├── requirements.txt
└── README.md
