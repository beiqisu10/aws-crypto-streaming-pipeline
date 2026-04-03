import os
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from datetime import datetime, timedelta

# 1. Update the base environment variables (Exclude sensitive password)
DBT_ENV_TEMPLATE = {
    'DB_HOST': 'crypto-dw-workgroup.996704094671.us-east-1.redshift-serverless.amazonaws.com',
    'DB_NAME': 'cryptodb',
    'DB_USER': 'admin',
    'DBT_LOG_PATH': '/tmp/dbt_logs',
    'DBT_TARGET_PATH': '/tmp/dbt_target',
}

default_args = {
    'owner': 'sbq',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# 2. Updated execution function: Now fetches secrets internally
def run_dbt_command(command, env_vars, full_refresh_arg='false'):
    import subprocess
    import os
    import sys
    import json
    import boto3
    from pathlib import Path
    from botocore.exceptions import ClientError
    
    # --- FETCH SECRETS FROM AWS ---
    secret_name = "crypto-redshift-credentials"
    region_name = "us-east-1"
    
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        creds = json.loads(get_secret_value_response['SecretString'])
        # Inject the password into the environment variables for dbt profiles.yml
        env_vars['DB_PASSWORD'] = creds['password']
        print("✅ Successfully retrieved Redshift password from Secrets Manager")
    except Exception as e:
        print(f"❌ Failed to retrieve secrets: {e}")
        raise e

    # Update system environment
    os.environ.update(env_vars)
    project_dir = "/usr/local/airflow/dags/crypto_dbt"
    
    venv_bin_dir = Path(sys.executable).parent
    dbt_executable = str(venv_bin_dir / "dbt")
    
    dbt_cmd = [dbt_executable, command, "--profiles-dir", "."]
    
    is_full_refresh = str(full_refresh_arg).lower() == 'true'
    if is_full_refresh and command == 'run':
        dbt_cmd.append("--full-refresh")
    
    try:
        result = subprocess.run(
            dbt_cmd,
            cwd=project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=True
        )
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(e.output)
        raise

with DAG(
    'crypto_dbt_workflow_v3',
    default_args=default_args,
    description='Run dbt models with credentials from AWS Secrets Manager',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    params={"full_refresh": "false"} 
) as dag:

    # 1. Task: Run dbt models
    dbt_run = PythonVirtualenvOperator(
        task_id='dbt_run_isolated',
        python_callable=run_dbt_command,
        op_args=['run', DBT_ENV_TEMPLATE],
        op_kwargs={
            'full_refresh_arg': "{{ dag_run.conf.get('full_refresh', 'false') }}"
        },
        # Note: boto3 is needed in the venv to fetch secrets
        requirements=["dbt-redshift==1.7.7", "boto3"], 
        system_site_packages=False,
    )

    # 2. Task: Run dbt data tests
    dbt_test = PythonVirtualenvOperator(
        task_id='dbt_test_isolated',
        python_callable=run_dbt_command,
        op_args=['test', DBT_ENV_TEMPLATE],
        op_kwargs={
            'full_refresh_arg': 'false'
        },
        requirements=["dbt-redshift==1.7.7", "boto3"],
        system_site_packages=False,
    )

    dbt_run >> dbt_test