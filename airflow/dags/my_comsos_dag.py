from datetime import datetime
from pathlib import Path
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


DBT_PROJECT_PATH = Path("/usr/local/airflow/dags/dbt_project")

DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"

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

test_dbt_standalone()