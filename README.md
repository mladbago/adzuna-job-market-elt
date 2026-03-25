# 🇵🇱 Adzuna Poland: Job Market Analytics

---

## 🌟 Project Goal
The goal of this project is to build a modern data pipeline that monitors the **Polish Job Market** in real-time. By extracting data from the **Adzuna API**, this system provides a clear, organized view of where the jobs are, who is hiring, and what the average salaries look like across different Polish cities and voivodeships.

## 🔄 How the Data Flows
1.  **Extraction:** We pull the latest job postings for Poland directly from the Adzuna API.
2.  **Storage:** Raw data is safely stored in **Snowflake**, a high-performance cloud data warehouse.
3.  **Transformation:** Using **dbt**, we clean up "messy" API data—standardizing city names (like "warszawa" → "Warszawa"), fixing salary formats, and organizing everything into easy-to-read tables.
4.  **Analysis:** The final data is ready for professional tools like **Preset** or **Tableau** to create charts and dashboards.

## 🛠️ Tools Used
* **Python (`uv`):** For fast and reliable project setup.
* **Snowflake:** Our central "brain" for storing and querying data.
* **dbt (Data Build Tool):** For turning raw data into polished professional tables.
* **Adzuna API:** Our source for live job market data in Poland.

## ✅ Key Features
* **Clean Data:** Automatically fixes typos and inconsistent formatting from the API.
* **Quality First:** Includes automated "health checks" to ensure the data is accurate before it reaches the dashboard.
