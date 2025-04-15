# 📊 Financial Statement Variance and Risk Assessment

A batch-processing ETL and Machine Learning project that leverages the Financial Modeling Prep (FMP) API to assess companies' financial health over time by analyzing quarterly statement variances. The system classifies companies into risk categories and visualizes key financial insights through interactive dashboards.

---

## 📌 Problem Statement

Tracking changes in financial health is essential for analysts, investors, and financial institutions to identify potentially risky companies. However, analyzing fluctuations in financial statement metrics manually across quarters is inefficient and error-prone.

This project automates the process using:
- **ETL pipelines** to extract and process financial data,
- **ML models** to classify company risk,
- and **data visualizations** to support strategic decision-making.

---

## 🎯 Objectives

- Automate extraction of quarterly financial statements (Income, Balance Sheet, Cash Flow)
- Calculate quarter-over-quarter (QoQ) variance in key metrics (e.g., Revenue, Net Income, Operating Cash Flow)
- Use variance trends and financial ratios to predict risk category (Low, Medium, High)
- Visualize historical trends, risk scores, and health indicators via dashboards

---

## 🛠 Tech Stack

| Component         | Technology                     |
|------------------|--------------------------------|
| API              | [FMP Financial API](https://site.financialmodelingprep.com/developer/docs/) |
| Language         | Python                         |
| ETL Framework    | Pandas, SQLAlchemy, Airflow (optional) |
| Data Storage     | PostgreSQL / SQLite            |
| ML Framework     | scikit-learn / XGBoost         |
| Visualization    | Tableau / Power BI / Plotly Dash |
| Deployment       | Batch Jobs (Cron/Airflow)      |

---

## 🔁 Workflow

### 1. Extract
- Fetch quarterly statements for selected companies from the FMP API:
  - Income Statement
  - Balance Sheet
  - Cash Flow Statement

### 2. Transform
- Clean and standardize data
- Calculate period-over-period variance for:
  - Revenue
  - Net Income
  - EBITDA
  - Operating Cash Flow
  - Total Debt, etc.
- Derive financial ratios (e.g., debt-to-equity, current ratio)

### 3. Load
- Store transformed data in a SQL database for longitudinal access

### 4. Machine Learning
- Train classification model (e.g., Decision Tree, SVM, XGBoost)
- Label company risk: **Low**, **Medium**, **High**
- Features: financial variances, ratios, sector

### 5. Visualization
- Build dashboards to explore:
  - Company-level risk scores
  - Variance trendlines
  - Historical financial performance

---

## 📈 Sample Use Cases

- Identify companies with deteriorating financial health
- Support due diligence for investment analysts
- Highlight early warning signals for credit risk

---

## 📂 Project Structure

