from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.hive.sensors.metastore_partition import MetastorePartitionSensor
from airflow.providers.amazon.aws.operators.eks import EksPodOperator

# Default arguments for the DAG
default_args = {
    'owner': 'GaiaData',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'migration_test',
    default_args=default_args,
    description='DAG to monitor Hive metastore partitions and run something on EKS',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test']
) as dag:

    check_hive_partition = MetastorePartitionSensor(
        task_id='check_hive_partition',
        schema='zuora',
        table='product_rate_plan_charge_tiers_v1',
        partition_name='snapshot_day={{ ds }}', 
        mysql_conn_id='hive_metastore',
        poke_interval=60,
        timeout=600
    )

    start_pod = EksPodOperator(
        task_id="start_pod",
        pod_name="test_pod",
        cluster_name="test",
        image="amazon/aws-cli:latest",
        cmds=["sh", "-c", "echo Test Airflow; date"],
        labels={"demo": "hello_world"},
        get_logs=True,
        on_finish_action="keep_pod",
    )

    # Define task dependencies
    check_hive_partition >> start_pod
