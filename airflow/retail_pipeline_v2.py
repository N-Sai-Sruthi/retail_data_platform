from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email
from datetime import timedelta


ALERT_EMAIL = ["mohithm887@gmail.com"]



def notify_failure(context):
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    log_url = task_instance.log_url

    subject = f"🚨 FAILED: {task_id}"

    html_content = f"""
    <h2>❌ Task Failed</h2>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Execution Time:</b> {execution_date}</p>

    <h3>🔍 Logs</h3>
    <p><a href="{log_url}">View Airflow Logs</a></p>
    """

    send_email(
        to=ALERT_EMAIL,
        subject=subject,
        html_content=html_content
    )



def notify_success(context):
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']

    subject = f"✅ DAG SUCCESS: {dag_id}"

    html_content = f"""
    <h2>✅ Pipeline Completed Successfully</h2>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Execution Time:</b> {execution_date}</p>
    """

    send_email(
        to=ALERT_EMAIL,
        subject=subject,
        html_content=html_content
    )



default_args = {
    'owner': 'airflow',
    'retries': 2,  # updated
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_failure
}



with DAG(
    dag_id='retail_data_pipeline_v2',
    default_args=default_args,
    schedule_interval='0 */6 * * *',  # every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=['databricks', 'etl']
) as dag:

    
    bronze = DatabricksRunNowOperator(
        task_id='bronze',
        job_id=465776152739474
    )

    dq_bronze = DatabricksRunNowOperator(
        task_id='dq_bronze',
        job_id=320867661852947
    )

    silver = DatabricksRunNowOperator(
        task_id='silver',
        job_id=358185141951899
    )

    dq_silver = DatabricksRunNowOperator(
        task_id='dq_silver',
        job_id=710328246544673
    )

    gold = DatabricksRunNowOperator(
        task_id='gold',
        job_id=525753609956255
    )

    dq_gold = DatabricksRunNowOperator(
        task_id='dq_gold',
        job_id=228359298950546
    )

    snowflake_load = DatabricksRunNowOperator(
        task_id='snowflake_load',
        job_id=850025400569496
    )


    success_alert = EmptyOperator(
        task_id='final_success_alert',
        on_success_callback=notify_success
    )


    bronze >> dq_bronze >> silver >> dq_silver >> gold >> dq_gold >> snowflake_load >> success_alert