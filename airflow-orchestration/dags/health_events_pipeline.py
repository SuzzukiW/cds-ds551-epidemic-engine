from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

def handle_kafka_ingestion_failure(context):
    task_instance = context['task_instance']
    subject = f"Kafka Ingestion Task Failed: {task_instance.task_id}"
    body = f"The Kafka ingestion task {task_instance.task_id} failed. Please investigate."
    send_email(to=Variable.get('failure_email'), subject=subject, html_content=body)

def handle_spark_analysis_failure(context):
    task_instance = context['task_instance']
    subject = f"Spark Analysis Task Failed: {task_instance.task_id}"
    body = f"The Spark analysis task {task_instance.task_id} failed. Please investigate."
    send_email(to=Variable.get('failure_email'), subject=subject, html_content=body)

def handle_data_visualization_failure(context):
    task_instance = context['task_instance']
    subject = f"Data Visualization Task Failed: {task_instance.task_id}"
    body = f"The data visualization task {task_instance.task_id} failed. Please investigate."
    send_email(to=Variable.get('failure_email'), subject=subject, html_content=body)

def success_callback(context):
    subject = "Pipeline Completed Successfully"
    body = "The health events data pipeline has completed successfully."
    send_email(to=Variable.get('success_email'), subject=subject, html_content=body)

def data_quality_check(**context):
    # TODO
    # perform data quality checks here
    # raise an exception or return an error if data quality issues are detected
    pass

def send_slack_notification(**context):
    slack_webhook_token = Variable.get('slack_webhook_token')
    slack_channel = Variable.get('slack_channel')
    slack_message = f":large_green_circle: The health events data pipeline has completed successfully!"

    success_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_connection',
        webhook_token=slack_webhook_token,
        channel=slack_channel,
        username='Airflow',
        message=slack_message
    )
    return success_alert.execute(context=context)

def send_failure_slack_notification(context):
    slack_webhook_token = Variable.get('slack_webhook_token')
    slack_channel = Variable.get('slack_channel')
    task_instance = context['task_instance']
    slack_message = f":red_circle: Task Failed: {task_instance.task_id}"

    failure_alert = SlackWebhookOperator(
        task_id='slack_failure_notification',
        http_conn_id='slack_connection',
        webhook_token=slack_webhook_token,
        channel=slack_channel,
        username='Airflow',
        message=slack_message
    )
    return failure_alert.execute(context=context)

def cleanup_data():
    # perform cleanup tasks on remote server
    cleanup_command = 'rm -rf /path/to/data/*'
    return f'ssh user@remote_host "{cleanup_command}"'

def send_summary_email(**context):
    subject = "Data Pipeline Summary"
    body = "Here is a summary of the data pipeline:\n\n"
    body += f"- Kafka Ingestion: {context['ti'].xcom_pull(task_ids='kafka_ingestion')}\n"
    body += f"- Spark Analysis: {context['ti'].xcom_pull(task_ids='spark_analysis')}\n"
    body += f"- Data Visualization: {context['ti'].xcom_pull(task_ids='data_visualization')}\n"

    email_task = EmailOperator(
        task_id='send_summary_email',
        to=Variable.get('summary_email'),
        subject=subject,
        html_content=body
    )
    email_task.execute(context=context)

def backfill_data():
    # perform backfilling of historical data
    backfill_command = 'spark-submit --class DataBackfill spark-jobs/data-backfill.jar'
    return backfill_command

with DAG(
    'health_events_pipeline',
    default_args=default_args,
    description='Health Events Data Pipeline',
    schedule_interval='@daily',
    catchup=False,
    on_success_callback=success_callback,
    on_failure_callback=send_failure_slack_notification,
) as dag:

    start_task = DummyOperator(task_id='start')

    # kafka data ingestion task
    kafka_ingestion = BashOperator(
        task_id='kafka_ingestion',
        bash_command='python kafka-server/consumer.py',
        on_failure_callback=handle_kafka_ingestion_failure,
        execution_timeout=timedelta(minutes=30),
    )

    # load data into Spark task
    load_data_to_spark = BashOperator(
        task_id='load_data_to_spark',
        bash_command='spark-submit spark-explore/data/eda.ipynb',
        execution_timeout=timedelta(minutes=60),
    )

    # data quality check task
    data_quality_check_task = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
        provide_context=True,
    )

    # spark analysis task
    spark_analysis = SparkSubmitOperator(
        task_id='spark_analysis',
        conn_id='spark_default',
        application='spark-analytics/local_model.py',
        on_failure_callback=handle_spark_analysis_failure,
        execution_timeout=timedelta(hours=2),
    )

    # data backfilling task
    data_backfill = BashOperator(
        task_id='data_backfill',
        bash_command=backfill_data(),
        execution_timeout=timedelta(hours=3),
    )

    # data visualization task
    data_visualization = BashOperator(
        task_id='data_visualization',
        bash_command='python data-visualization/visualizations.py',
        on_failure_callback=handle_data_visualization_failure,
        execution_timeout=timedelta(minutes=45),
    )

    # Slack notification task
    slack_notification = PythonOperator(
        task_id='slack_notification',
        python_callable=send_slack_notification,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # data cleanup task
    cleanup_task = SSHOperator(
        task_id='cleanup_task',
        ssh_conn_id='remote_server_connection',
        command=cleanup_data(),
    )

    # summary email task
    summary_email_task = PythonOperator(
        task_id='summary_email_task',
        python_callable=send_summary_email,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    end_task = DummyOperator(task_id='end')

    # set task dependencies
    start_task >> kafka_ingestion >> load_data_to_spark >> data_quality_check_task >> spark_analysis >> data_backfill >> data_visualization >> slack_notification >> cleanup_task >> summary_email_task >> end_task

    # external task sensor
    external_task_sensor = ExternalTaskSensor(
        task_id='external_task_sensor',
        external_dag_id='external_dag',
        external_task_id='external_task',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        poke_interval=60,
        timeout=1800,
        on_failure_callback=handle_task_failure,
    )

    # make the pipeline wait for the external task to complete
    external_task_sensor >> start_task