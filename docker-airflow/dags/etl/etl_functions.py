import os
import pandas as pd
import time
import datetime

start_hour = 1
horizont_hours = 48

lat = 55.75
lng = 37.62
moscow_timezone = 3
local_timezone = 3


def transform_data(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key='weather_wwo_json', task_ids=['extract_data'])[0]

    start_moscow = datetime.datetime.utcnow() + datetime.timedelta(hours=moscow_timezone)
    start_station = datetime.datetime.utcnow() + datetime.timedelta(hours=local_timezone)
    end_station = start_station + datetime.timedelta(hours=horizont_hours)

    date_list = []
    value_list = []
    weather_data = json_data['data']['weather']
    for weather_count in range(len(weather_data)):
        temp_date = weather_data[weather_count]['date']
        hourly_values = weather_data[weather_count]['hourly']
        for i in range(len(hourly_values)):
            date_time_str = '{} {:02d}:00:00'.format(temp_date, int(hourly_values[i]['time']) // 100)
            date_list.append(date_time_str)
            value_list.append(hourly_values[i]['cloudcover'])

    res_df = pd.DataFrame(value_list, columns=['cloud_cover'])
    # time prediction (local for considerable point)
    res_df["date_to"] = date_list
    res_df["date_to"] = pd.to_datetime(res_df["date_to"])
    # 48 interval
    res_df = res_df[res_df['date_to'].between(start_station, end_station, inclusive=True)]
    # prediction time Moscow
    res_df["date_to"] = res_df["date_to"] + datetime.timedelta(hours=moscow_timezone - local_timezone)
    res_df["date_to"] = res_df["date_to"].dt.strftime('%Y-%m-%d %H:%M:%S')
    # request time Moscow
    res_df["date_from"] = start_moscow
    res_df["date_from"] = pd.to_datetime(res_df["date_from"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    # responding time
    res_df["processing_date"] = res_df["date_from"]
    print(res_df.head())
    ti.xcom_push(key='weather_wwo_df', value=res_df)


def load_data(**kwargs):
    ti = kwargs['ti']
    res_df = ti.xcom_pull(key='weather_wwo_df', task_ids=['transform_data'])[0]
    print(res_df.head())
    print([x for x in res_df.iloc[0]])
