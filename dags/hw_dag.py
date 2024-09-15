import datetime as dt
import os
import sys
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator

# каталог с проектом монтируется в docker-compose.yaml
path = '/opt/airflow/airflow_hw'

# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path

# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.pipeline import pipeline
from modules.predict import predict

args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    pipeline = PythonOperator(
        dag=dag,
        task_id='pipeline',
        python_callable=pipeline,
    )
    predict = PythonOperator(
        dag=dag,
        task_id='predict',
        python_callable=predict,
    )
    pipeline >> predict
