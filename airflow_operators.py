from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator


def airflow_practice():
    return " This is a Python function."

default_args = {
    "owner" : "abdullahi",
    "email" : "ayantayoabdullah@gmail.com",
    "retries" : 5,
    "retry_delay" : timedelta(minutes= 5),
    "email_on_retry" : False,
    "email_on_failure" : False
}

with DAG("airflow_operators", schedule= "@daily", catchup=False, start_date= datetime(2023, 8, 27), default_args= default_args) as dag:
    start_task = EmptyOperator(
        task_id = "start_pipeline"
    )
    python_operator = PythonOperator(
        task_id = "python_function",
        python_callable= airflow_practice
    )
    send_email_notification= EmailOperator(
        task_id="send__email",
        to= "ayantayoabdullah@gmail.com",
        subject="Airflow EmailOperator",
        html_content="<h2>Success/Failure?"   
    )
    end_task = EmptyOperator(
        task_id = "end_pipeline"
    )

    start_task >> python_operator >> send_email_notification >> end_task
