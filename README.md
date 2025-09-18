
📝 Project Overview

This project is developed as part of the Data Engineer YH program at STI in course Data Warehosuing
.
Our task was to build a modern data stack pipeline for a fictional HR agency that regularly analyzes job ads from Arbetsförmedlingen when filtering on job categorical fields "Pedagogik", "Transport, distribution, lager", "säkerhet och bevakning".

The system enables Talent Acquisition Specialists to:

Automatically extract job ads from the Jobtech API

Transform and structure the data using a dimensional model

Analyze the data in an interactive Streamlit dashboard

--------------------------------

⚙️ Tech Stack

Component	Technology	Purpose
Data ingestion	dlt
	Load job ads from Jobtech API to Snowflake
Data warehouse	Snowflake	Central data warehouse
Transformation	dbt	Build staging, warehouse, and mart models
Dashboard	Streamlit (Python)	Interactive visualizations & KPIs
Version control	Git + GitHub	Collaboration & versioning

--------------------------

🧩 Data Architecture

We designed a dimensional data model organized into four layers in dbt:

src_* — raw source staging models (directly from Jobtech API via dlt)

dim_* — dimension tables with surrogate keys (employer, job_details, occupation, auxilliary_attributes)

fct_job_ads — fact table with surrogate keys to all dimensions

mart_jobs — business-friendly mart view for dashboard analysis

The data flow is:

Jobtech API → dlt → Snowflake (src) → dbt (dim/fct/mart) → Streamlit dashboard

------------------------------

📊 Dashboard Features

Our Streamlit dashboard allows Talent Acquisition Specialists to:

View number top lists of job ads by region, occupational group and employer

Number of vacancies

Number of ads per company

-----------------------------

Getting Started
1. Clone the repo
git clone https://github.com/AlejandroHiroshima/Data_warehouse_grupp4_DE24.git

2. Install dependencies
uv pip install -r requirements.txt

3. Setup dbt

Create your profiles.yml and point to your Snowflake account

Run:

dbt deps
dbt run

4. Run the dashboard
cd dashboard_streamlit
streamlit run dashboard.py


----------------------------------

📌 Notes

No credentials are stored in the repo

.env is used to load Snowflake credentials locally

See load_job_ads.py for data ingestion and models/ for dbt models

------------------------------------

Enjoy the dashboard / Erik, Alexander and Eyoub 😎 🚀



