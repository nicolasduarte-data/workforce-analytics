# Workforce Analytics — HR Operations Pipeline

> End-to-end people analytics pipeline — messy HR data in, compensation equity insights out, full SQL showcase throughout.

**[Live Dashboard → Tableau Public](https://public.tableau.com/app/profile/nicol.s.duarte/viz/Book1_17785524067300/Dashboard1)**

---

## What This Is

Analyzed a synthetic HR operations dataset (419 employees, 2,728 service tickets, 9 departments) to surface compensation equity gaps and HR service delivery inefficiencies. Built end-to-end in Python + DuckDB with a full SQL showcase (window functions, CTEs, multi-table JOINs). Clean output exported to Tableau Public for interactive visualization.

---

## 3 Findings

1. **Compensation equity gap** — Customer Success, Marketing, Finance, and HR sit 11–15% below salary band midpoint. A structural pay risk, not random variance.
2. **Resolution time asymmetry** — HR Technology tickets take a median 43 days to resolve vs. 10 days for Policy & Benefits. Category matters more than assignee.
3. **Manager coverage gaps** — Engineering and Customer Success have the highest absolute count of employees without a manager. Executive has the worst proportional gap (100%).

---

## Architecture

```
Raw messy CSVs (ServiceNow-style export simulation)
        │
        ▼
┌─────────────────────┐
│  DuckDB             │  ← SQL layer: 8 detection queries
│  Ingestion + Load   │    (orphan FKs, dupes, SCD2, outliers...)
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  pandas             │  ← Python layer: 8 cleaning operations
│  Cleaning Pipeline  │    (dedup, tag, normalize, flag)
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  DuckDB             │  ← SQL showcase: CTEs, window functions,
│  Analysis           │    multi-table JOINs, aggregations
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  Tableau Public     │  ← 3-panel dashboard, published + public
│  Dashboard          │
└─────────────────────┘
```

---

## Stack

| Layer | Tool |
|-------|------|
| Language | Python 3.11 |
| Data manipulation | pandas |
| SQL / query layer | DuckDB |
| Visualization | matplotlib / seaborn |
| Notebook | Jupyter |
| Dashboard | Tableau Public |

---

## Project Structure

```
workforce-analytics/
├── data/
│   ├── raw/                  ← 3 deliberately messy CSVs
│   └── clean/                ← cleaned output + dashboard flat files
├── notebooks/
│   ├── 02-ingestion-cleaning.ipynb   ← DuckDB load + 8 SQL detection queries + pandas cleaning
│   ├── 03-analysis.ipynb             ← 3 findings with CTEs, window functions, charts
│   └── 04-dashboard-prep.ipynb      ← flat file build for Tableau
├── src/
│   └── generate_data.py      ← synthetic dataset generator (SEED=42, deterministic)
├── docs/
│   └── dashboard.png         ← published dashboard screenshot
└── README.md
```

---

## How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate raw data (already included — only needed if regenerating)
python src/generate_data.py

# 3. Run notebooks in order
jupyter notebook notebooks/02-ingestion-cleaning.ipynb
jupyter notebook notebooks/03-analysis.ipynb
jupyter notebook notebooks/04-dashboard-prep.ipynb
```

Clean CSVs will be written to `data/clean/`. The Tableau dashboard consumes `dashboard_data.csv` and `tickets_analysis.csv` from that folder.

---

## Dataset Design

3 tables, 8 HR-specific messiness patterns injected at generation:

| Messiness | What it simulates |
|-----------|------------------|
| Orphan position_ids in tickets | Org restructure mid-year |
| Duplicate employees | Rehires creating duplicate keys |
| Multiple effective_dates per position | SCD2 — band rebanding exercise |
| Legacy category values | Category taxonomy refresh mid-year |
| Inconsistent country codes | Cross-system integration artifact |
| Close dates before open dates | Manual data entry errors |
| NULL manager_id (~8% of employees) | Vacant positions, interim assignments |
| Salary outliers within band | Off-cycle adjustments |

---

## Part of

**hrds — HR Data Science Portfolio** · Track B (tech-company people analytics buyers)

| Sub-project | Signal | Status |
|-------------|--------|--------|
| **workforce-analytics** (this repo) | SQL + Python pipeline on messy HR data | ✅ Live |
| retention-prediction | Behavioral churn prediction | 🔜 In progress |
| feedback-classifier | LLM-based feedback triage | 🔜 Planned |
