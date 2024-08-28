from datetime import datetime
import logging

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 9, 3, 10, 0),
    "retries": 1,
}

dag = DAG(
    "cassandra_streaming_dag",
    default_args=default_args,
    description="DAG to run Spark streaming job",
    schedule_interval="@daily",
    catchup=False,
)

run_spark_job = SparkSubmitOperator(
    task_id="run_spark_streaming_job",
    application="./dags/jobs/spark_stream.py",
    packages="com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    conf={
        "spark.cassandra.connection.host": "cassandra",
    },
    dag=dag,
)

run_spark_job
