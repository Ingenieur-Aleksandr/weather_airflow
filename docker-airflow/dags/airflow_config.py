import datetime
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
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
    create_cloud_table = PostgresOperator(
        task_id="create_clouds_value_table",
        postgres_conn_id="database_PG",
        sql="""
                                CREATE TABLE IF NOT EXISTS clouds_value (
                                clouds FLOAT NOT NULL,
                                date_to TIMESTAMP NOT NULL,
                                date_from TIMESTAMP NOT NULL,
                                processing_date TIMESTAMP NOT NULL);
                            """,
    )
    insert_in_table = PostgresOperator(
        task_id="insert_clouds_table",
        postgres_conn_id="database_PG",
        sql=[f"""INSERT INTO clouds_value VALUES(
                             {{{{ti.xcom_pull(key='weather_wwo_df', task_ids=['transform_data'])[0].iloc[{i}]['cloud_cover']}}}},
                            '{{{{ti.xcom_pull(key='weather_wwo_df', task_ids=['transform_data'])[0].iloc[{i}]['date_to']}}}}',
                            '{{{{ti.xcom_pull(key='weather_wwo_df', task_ids=['transform_data'])[0].iloc[{i}]['date_from']}}}}',
                            '{{{{ti.xcom_pull(key='weather_wwo_df', task_ids=['transform_data'])[0].iloc[{i}]['processing_date']}}}}')
                            """ for i in range(5)]
    )


    extract_data >> transform_data >> create_cloud_table >> insert_in_table
