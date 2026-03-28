# 📊 Polish Tech-Market Intelligence Pipeline
**An end-to-end ELT system providing real-time analytics on the 2026 Polish Job Market.**

---

## 🚀 Executive Summary
This project is a production-grade data engineering pipeline that automates the extraction, transformation, and visualization of tech job listings across Poland. By integrating the **Modern Data Stack**, it transforms raw, unstructured API data into actionable business intelligence, tracking hiring velocity, salary transparency, and regional demand.

### [🔗 View Live Interactive Dashboard](#) (*https://f5b2716a.us1a.app.preset.io/superset/dashboard/9/?native_filters_key=iz6EqSPlWSQ*)

---

## 🏗️ System Architecture
The pipeline follows a modular ELT (Extract, Load, Transform) pattern designed for scalability and observability.

> **Architecture Flow:** > `Adzuna API` ➡️ `Python (Requests)` ➡️ `Airflow (Orchestration)` ➡️ `Snowflake (Data Warehouse)` ➡️ `dbt (Transformation)` ➡️ `Preset (BI)`

**(Place your Architecture Diagram/Image here)**

---

## 🛠️ Tech Stack & Tools
| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Ingestion** | **Python (Requests)** | Robust REST API extraction with error handling. |
| **Orchestration** | **Apache Airflow (Cosmos)** | Automated DAG scheduling and dependency management. |
| **Data Warehouse** | **Snowflake** | Multi-layer storage (Bronze/Silver/Gold) for high-performance querying. |
| **Transformation** | **dbt (Data Build Tool)** | Modular SQL modeling with version control and data testing. |
| **Visualization** | **Preset (Apache Superset)** | Advanced BI dashboarding with custom SQL analytics. |
| **CI/CD** | **GitHub Actions** | Automated pipeline testing and headless environment deployment. |

---

## 💡 Key Market Insights
* **Regional Dominance:** **Mazowieckie, Małopolskie, and Dolnośląskie** represent the "Big 3" tech hubs, accounting for over 60% of job volume.
* **Seniority Distribution:** Using custom SQL classifiers, the pipeline identifies the split between Junior, Mid, and Senior-level demand.
* **Work Model Analysis:** Automated text parsing tracks the shift between **Fully Remote**, **Hybrid**, and **On-site** roles in real-time.
* **Data Freshness:** Maintains a **< 24-hour SLA**, ensuring stakeholders see the most current hiring pulse in Poland.

---

## 🛠️ Engineering Challenges & Solutions

### **1. The "Ghost Data" Problem (Salary Sparsity)**
**Challenge:** A significant portion of listings lack explicit salary bands, threatening to skew market averages.  
**Solution:** I developed a **"Salary Transparency Index"** KPI. Instead of discarding nulls, the pipeline tracks the ratio of transparent vs. hidden salaries, turning a data limitation into a valuable market-behavior insight for recruiters.

### **2. Headless CI/CD Debugging (SerializedDAG Error)**
**Challenge:** During GitHub Actions runs, the `dags test` command triggered an `AttributeError` because the `SerializedDAG` object lacks the Jinja environment required by standard Slack Notifiers.  
**Solution:** I authored an **environment-aware callback wrapper** that detects the GitHub runner environment. This allows for clean, passing tests in CI/CD while preserving full alerting capabilities in the production environment.

---
## 🛠️ Data Governance & Observability
* **Automated Alerting:** Implemented Slack Webhook integration. The pipeline 'phones home' on every run, providing instant visibility into job success or failure.
* **Medallion Architecture:** Data is organized in Snowflake across three distinct layers:
    1. **RAW:** Immutable landing zone for API JSON responses.
    2. **SILVER:** Cleaned, filtered, and deduplicated relational data.
    3. **GOLD:** Business-level Marts optimized for Preset BI performance.


---
## 🧬 dbt Lineage & Data Quality
**(Place your dbt Lineage Graph screenshot here)**

* **Staging Layer:** Standardizing raw API JSON into clean, typed relational schemas.
* **Mart Layer:** Creating analytics-ready tables optimized for BI consumption.
* **Tests:** Automated `not_null` and `unique` constraints ensure zero "orphan" records reach the dashboard.

---

## 🏃 How to Run Locally
1. **Clone:** `git clone https://github.com/mladbago/adzuna-job-market-elt`
2. **Config:** Create a `.env` file with `SNOWFLAKE_CONN`, `ADZUNA_APP_ID`, `ADZUNA_APP_KEY`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
3. **Start Airflow:** `astro dev start`
4. **Build Models:** `dbt build`

---

### 🚀 What's Next?
The pipeline is currently stable. My next phase involves implementing **Regex-based Skill Extraction** (Python, dbt, SQL, etc.) from job descriptions to build a real-time "Tech Stack Demand" heatmap.