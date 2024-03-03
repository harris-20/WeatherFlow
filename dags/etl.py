from uuid import uuid4
from json import loads, dumps
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from utils.api import fetch_data
from utils.database import MySQLHandler

default_args = {
   'owner': 'pratik'
}


def extract():
    return dumps(fetch_data())

def enrich_datetime(ti):
    weather_array = loads(ti.xcom_pull(task_ids='extract'))

    to_return = []
    for station_data in weather_array:
        series = pd.Series(station_data)
        series["timestamp"] = int(series["timestamp"])
        series["sunset_ts"] = int(series["sunset_ts"])
        series["sunrise_ts"] = int(series["sunrise_ts"])

        series["recorded_datetime_local"] = datetime.fromtimestamp(series["timestamp"] + series["timezone"])
        series["sunset_datetime_local"] = datetime.fromtimestamp(series["sunset_ts"] + series["timezone"])
        series["sunrise_datetime_local"] = datetime.fromtimestamp(series["sunrise_ts"] + series["timezone"])

        series.drop(["timestamp", "sunset_ts", "sunrise_ts"], inplace=True)

        month = int(series["recorded_datetime_local"].strftime("%m"))
        if 1 <= month <= 3:
            series["recorded_quarter_local"] = "Q1"
        elif 4 <= month <= 6:
            series["recorded_quarter_local"] = "Q2"
        elif 7 <= month <= 9:
            series["recorded_quarter_local"] = "Q3"
        elif 10 <= month <= 12:
            series["recorded_quarter_local"] = "Q4"

        if 3 <= month <= 5:
            series["recorded_season_local"] = "Spring"
        elif 6 <= month <= 8:
            series["recorded_season_local"] = "Summer"
        elif 9 <= month <= 11:
            series["recorded_season_local"] = "Fall"
        elif month == 12 or 1 <= month <= 2:
            series["recorded_season_local"] = "Winter"
        
        series["recorded_weekday_local"] = series["recorded_datetime_local"].strftime("%A")
        series["recorded_month_local"] = series["recorded_datetime_local"].strftime("%B")
        series["recorded_year_local"] = series["recorded_datetime_local"].year

        series["daylight_duration_delta"] = series["sunset_datetime_local"] - series["sunrise_datetime_local"]
        series["daylight_duration_minutes"] = series["daylight_duration_delta"].seconds // 60
        series.drop(["daylight_duration_delta"], inplace=True)

        series["recorded_date_local"] = series["recorded_datetime_local"].strftime('%Y-%m-%d')
        series["recorded_datetime_local"] = series["recorded_datetime_local"].strftime('%Y-%m-%d %H:%M:%S')
        series["sunset_datetime_local"] = series["sunset_datetime_local"].strftime('%Y-%m-%d %H:%M:%S')
        series["sunrise_datetime_local"] = series["sunrise_datetime_local"].strftime('%Y-%m-%d %H:%M:%S')

        to_return.append(series.to_dict())
    return dumps(to_return)

def add_calc_attributes(ti):
    weather_array = loads(ti.xcom_pull(task_ids='extract'))

    to_return = []
    for station_data in weather_array:
        series = pd.Series(station_data)
        new_attributes = {}
        new_attributes["dew_point"] = round(series["temp"] - ((100 - series["humidity"]) / 5), 2)

        series["temp_F"] = (series["temp"] * 1.8) + 32
        new_attributes["heat_index"] = 0.5 * (series["temp_F"] + 61.0 + ((series["temp_F"] - 68.0) * 1.2) + (series["humidity"] * 0.094))
        new_attributes["heat_index"] = round(((new_attributes["heat_index"] - 32) * 5) / 9, 2)
        to_return.append(new_attributes)
    return dumps(to_return)

def merge_transform(ti):
    enriched_data = loads(ti.xcom_pull(task_ids='enrich_datetime'))
    new_attributes = loads(ti.xcom_pull(task_ids='add_calc_attributes'))

    print(enriched_data)
    print(new_attributes)

    result = [{**enrich_data, **new_data} for enrich_data, new_data in zip(enriched_data, new_attributes)]
    return dumps(result)

def load(ti):
    result = loads(ti.xcom_pull(task_ids='merge_transform'))

    cxn = MySQLHandler()
    cxn.connect()

    for row in result:
        record_id, time_id, parameter_id, temp_id, heat_index_id = str(uuid4()), str(uuid4()), str(uuid4()), str(uuid4()), str(uuid4()) 

        w_fact = f"""INSERT INTO W_FACT(record_id, station_id, time_id, parameter_id, temp_id, heat_index_id)
                     VALUES ('{record_id}', {row['station_id']}, '{time_id}', '{parameter_id}', '{temp_id}', '{heat_index_id}')"""
        
        w_param_dim = f"""INSERT INTO W_PARAM_DIM(parameter_id, humidity, pressure, visibility, cloudiness, dew_point, wind_speed, wind_direction)
                          VALUES ('{parameter_id}', '{row['humidity']}', '{row['pressure']}',
                          '{row['visibility']}', '{row['cloudiness']}', '{row['dew_point']}', {row['wind_speed']}, {row['wind_direction']})"""
        
        w_time_dim = f"""INSERT INTO W_TIME_DIM(time_id, record_datetime, record_date, record_month, record_year, record_quarter, record_season, record_weekday)
                         VALUES ('{time_id}', '{row['recorded_datetime_local']}', '{row['recorded_date_local']}', '{row['recorded_month_local']}',
                         '{row['recorded_year_local']}', '{row['recorded_quarter_local']}', '{row['recorded_season_local']}', '{row['recorded_weekday_local']}')"""
        
        w_temp_dim = f"""INSERT INTO W_TEMP_DIM(temp_id, temp, temp_range_min, temp_range_max, feels_like)
                         VALUES ('{temp_id}', '{row['temp']}', '{row['temp_min']}', '{row['temp_max']}', '{row['feels_like']}')"""
        
        w_heat_dim = f"""INSERT INTO W_HEAT_INDEX_DIM(heat_index_id, heat_index, heat_index_category, description)
                         VALUES ('{heat_index_id}', '{row['heat_index']}', 1, '')"""
        
        cxn.execute_batch([w_fact, w_param_dim, w_time_dim, w_temp_dim, w_heat_dim])
    cxn.close()

with DAG(
    dag_id = 'weatherflow_etl',
    default_args = default_args,
    start_date = datetime.now() + timedelta(minutes=15),
    schedule_interval = timedelta(minutes=5),
    template_searchpath = '/home/pratik/weatherflow/dags',
    catchup=False,
    tags = ['etl']
) as dag:
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    enrich_datetime_task = PythonOperator(
        task_id='enrich_datetime',
        python_callable=enrich_datetime
    )

    add_calc_attributes_task = PythonOperator(
        task_id='add_calc_attributes',
        python_callable=add_calc_attributes
    )

    merge_transform_task = PythonOperator(
        task_id='merge_transform',
        python_callable=merge_transform
    )

    store_to_db_task = PythonOperator(
        task_id='load',
        python_callable=load
    )

extract_task >> [enrich_datetime_task, add_calc_attributes_task] >> merge_transform_task >> store_to_db_task
