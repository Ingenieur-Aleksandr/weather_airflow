import datetime
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from etl.etl_functions import lat, lng, transform_data, load_data

args = {
    'owner': 'In_genie_ur',
    'start_date': datetime.datetime(2023, 1, 1),
    'provide_context': True
}

key_wwo = Variable.get("KEY_API_WWO")
print(key_wwo)
start_hour = 1
horizont_hours = 48


def extract_data(**kwargs):
    ti = kwargs['ti']
    # next hour weather
    response = requests.get(
        'http://api.worldweatheronline.com/premium/v1/weather.ashx',
        params={
            'q': '{},{}'.format(lat, lng),
            'tp': '1',
            'num_of_days': 2,
            'format': 'json',
            'key': key_wwo
        },
        headers={
            'Authorization': key_wwo
        }
    )

    # in case of success go to next step
    if response.status_code == 200:
        json_data = response.json()
        print(json_data)

        ti.xcom_push(key='weather_wwo_json', value=json_data)


with DAG('load_weather_wwo', description='load_weather_wwo', schedule_interval='*/1 * * * *', catchup=False,
         default_args=args) as dag:  # 0 * * * *   */1 * * * *
    extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data)
    transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data)
    load_data = PythonOperator(task_id='load_data', python_callable=load_data)

    extract_data >> transform_data >> load_data
