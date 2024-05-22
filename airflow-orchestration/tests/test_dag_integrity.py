# test_dag_integrity.py
import pytest

def test_dag_loaded(dag_bag):
    dag = dag_bag.get_dag('health_events_pipeline')
    assert dag_bag.import_errors == {}  # no errors while loading DAGs
    assert dag is not None  # DAG is successfully loaded

def test_contain_tasks(dag_bag):
    dag = dag_bag.get_dag('health_events_pipeline')
    tasks = {task.task_id: task for task in dag.tasks}
    expected_tasks = {
        'start', 'kafka_ingestion', 'load_data_to_spark', 'data_quality_check',
        'spark_analysis', 'data_backfill', 'data_visualization',
        'slack_notification', 'cleanup_task', 'summary_email_task', 'end',
        'external_task_sensor'
    }
    assert set(tasks) == expected_tasks

def test_dependencies_of_kafka_ingestion_task(dag_bag):
    dag = dag_bag.get_dag('health_events_pipeline')
    kafka_ingestion_task = dag.get_task('kafka_ingestion')
    upstream_task_ids = set(task.task_id for task in kafka_ingestion_task.upstream_list)
    downstream_task_ids = set(task.task_id for task in kafka_ingestion_task.downstream_list)
    assert upstream_task_ids == {'start', 'external_task_sensor'}
    assert downstream_task_ids == {'load_data_to_spark'}
