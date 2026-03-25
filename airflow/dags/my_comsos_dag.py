from datetime import datetime
from pathlib import Path
from airflow.sdk import dag, task 
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
import os 
import requests
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier

DBT_PROJECT_PATH = Path("/usr/local/airflow/dags/dbt_project")

DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"


on_failure_notifier = SlackWebhookNotifier(
    slack_webhook_conn_id="slack_conn",
    text="❌ *DAG Failed!* \n*DAG:* {{ dag.dag_id }} \n*Execution Time:* {{ dag_run.logical_date }}"
)

on_success_notifier = SlackWebhookNotifier(
    slack_webhook_conn_id="slack_conn",
    text="✅ *DAG Succeeded!* \n*Project:* Adzuna ELT Pipeline \n*Status:* All data moved to Snowflake."
)

profile_config = ProfileConfig(
    profile_name="adzuna_test",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default", 
        profile_args={"database": "ADZUNA_DB", "schema": "STAGING"},
    ),
)

@dag(
    start_date=datetime(2026, 3, 25),
    catchup=False,
    tags=['test', 'dbt'],
    on_failure_callback=[on_failure_notifier],
    on_success_callback=[on_success_notifier]
)
def test_dbt_standalone():
    
    dbt_test_run = DbtTaskGroup(
        group_id="adzuna_dbt_models",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        render_config=RenderConfig(
            select=["path:models"], 
        ),
    )

    @task
    def fetch_adzuna_jobs(page: int = 1): 
        APP_ID = os.getenv("ADZUNA_APP_ID")
        APP_KEY = os.getenv("ADZUNA_APP_KEY")
        BASE_URL = os.getenv("BASE_URL")

        if not APP_ID or not APP_KEY: 
            raise ValueError("Adzuna API credentials are not set in environment variables.")
        
        base_url = f"https://api.adzuna.com/v1/api/jobs/pl/search/{page}"

        params = {
            "app_id": APP_ID,
            "app_key": APP_KEY,
            "results_per_page": 50,
            "max_days_old": 1,
            "content-type": "application/json"
        }

        response = requests.get(base_url, params=params)
        response.raise_for_status() 
        data = response.json()

        found_count = len(data.get('results',[]))
        print(f"Successfully retrieved {found_count} job listings.")
        return data 
    fetch_adzuna_jobs() >> dbt_test_run

test_dbt_standalone()