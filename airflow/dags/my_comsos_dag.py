from datetime import datetime
from pathlib import Path
from airflow.sdk import dag, task 
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
import os 
import requests
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
import time
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json 
from cosmos.constants import TestBehavior, LoadMode
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

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
        profile_args={"database": "ADZUNA_DB", "schema": "DEV"},
    ),
)

@dag(
    start_date=datetime(2026, 3, 25),
    catchup=False,
    tags=['test', 'dbt'],
    on_failure_callback=[on_failure_notifier],
    on_success_callback=[on_success_notifier]
)
def testing_pipeline():
    
    #Tests1
    dbt_test_run = DbtTaskGroup(
        group_id="adzuna_dbt_models",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.BUILD,
            select=["path:models", "path:snapshots", "path:tests"]
        ),
    )
    
    @task()
    def fetch_all_adzuna_pages():
        """Fetches all pages of job postings for the last 24 hours."""
        APP_ID = os.getenv("ADZUNA_APP_ID")
        APP_KEY = os.getenv("ADZUNA_APP_KEY")
        RESULTS_PER_PAGE = 50
        
        all_jobs = []
        current_page = 1
        total_count = None
        
        while True:
            url = f"https://api.adzuna.com/v1/api/jobs/pl/search/{current_page}"
            params = {
                'app_id': APP_ID,
                'app_key': APP_KEY,
                'results_per_page': RESULTS_PER_PAGE,
                'max_days_old': 1,
                'content-type': 'application/json'
            }

            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if total_count is None:
                total_count = data.get('count', 0)
                print(f"Total jobs found for today: {total_count}")
            
            page_results = data.get('results', [])
            all_jobs.extend(page_results)
            
            if not page_results or len(all_jobs) >= total_count:
                break
            
            current_page += 1
            # used for testing 
            if current_page == 3: 
                break

            # used for testing 
            print('Fetched first things')
            time.sleep(2.5)

        print(f"Successfully fetched {len(all_jobs)} jobs across {current_page} pages.")
        return all_jobs
    
    @task()
    def upload_to_s3(jobs_list: list, ds=None):
        """Uploads the full list of jobs to S3 with Hive-style partitioning."""
        year, month, day = ds.split('-')
        
        s3_hook = S3Hook(aws_conn_id='aws_s3_default')
        bucket_name = 'adzuna-raw-data-dev'
        s3_key = f"raw_jsons/year={year}/month={month}/day={day}/adzuna_jobs.json"
        
        final_data = {
            "count": len(jobs_list),
            "results": jobs_list,
            "ingested_at": datetime.utcnow().isoformat()
        }
        
        s3_hook.load_string(
            string_data=json.dumps(final_data),
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )

        clean_key = s3_key.replace("raw_jsons/", "")
        return clean_key
    
    @task()
    def load_s3_to_snowflake_raw(s3_key: str):
        """
        Executes a COPY INTO command in Snowflake using the SnowflakeHook.
        """
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

        sql = f"""
            COPY INTO adzuna_db.raw.jobs_raw (raw_content, file_name)
            FROM (
                SELECT $1, metadata$filename
                FROM @adzuna_db.raw.adzuna_s3_stage/{s3_key}
            )
            FILE_FORMAT = (FORMAT_NAME = adzuna_db.raw.adzuna_json_format)
        """
        hook.run(sql)
        print(f"Successfully triggered Snowflake load for: {s3_key}")

    jobs = fetch_all_adzuna_pages() 
    s3_path = upload_to_s3(jobs)

    snowflake_load_task = load_s3_to_snowflake_raw(s3_path)
    snowflake_load_task >> dbt_test_run

testing_pipeline()