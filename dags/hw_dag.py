import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonVirtualenvOperator

path = os.path.expanduser('~/airflow_hw')
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)


# <YOUR_IMPORTS>

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

def pipeline_op():
    from modules.pipeline import pipeline
    pipeline()

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    pipeline = PythonVirtualenvOperator(
        task_id='pipeline',
        python_callable=pipeline_op,
        requirements=["pandas==2.2.2",
                      "scikit-learn==1.5.2",
                      "dill==0.3.8"]
    )
    # <YOUR_CODE>

