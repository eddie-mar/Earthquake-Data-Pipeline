from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import date, datetime, timedelta

from clean_historical import data_cleaning

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='earthquake_pipeline',
    default_args=default_args,
    description='Data Pipeline for Earthquake Data',
    schedule_interval='@monthly', 
    start_date=datetime(2024, 7, 1),
    catchup=True,
) as dag:
    
    def extract_monthly(**kwargs):
        from datetime import date

        import os
        import pandas as pd
        import requests

        try:
            start = date(kwargs['start'])
            end = date(kwargs['end'])
        except Exception as e:
            print(f'Error {e}')
            return

        response = requests.get(f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start}&endtime={end}')
        if not response.ok:
            message =  f'Failed extraction for start = {start} -> pointer = {end}\n\tError {response.status_code}: {response.text}'
            print(message)
            with open('output/error.txt', 'a') as f:
                f.write(message + '\n')
            return
        
        response = response.json()['features']
        df_to_enter = []

        for entry in response:
            try:
                data = [
                    entry['properties']['place'],
                    entry['properties']['time'],
                    entry['properties']['mag'],
                    entry['geometry']['coordinates'][1],
                    entry['geometry']['coordinates'][0],
                    entry['geometry']['coordinates'][2],
                    entry['properties']['alert'],
                    entry['properties']['tsunami'],
                    entry['properties']['tz'],
                    entry['properties']['type']
                ]
            except Exception as e:
                print(f'Error {e}')
                print(entry)
                with open('output/error.txt', 'a') as f:
                    f.write(f'Data ingestion failure. Error {e} for start = {start} -> pointer = {end}\n')
                continue
            df_to_enter.append(data)
        
        df = pd.DataFrame(df_to_enter, columns=['place', 'time', 'magnitude', 'latitude', 'longitude', 'depth', 'alert', 'tsunami', 'tz', 'type'])
        
        path = 'output/csv_files/'
        os.makedirs(path, exist_ok=True)
        df.to_csv(f'{path}earthquake-data-{start.year}-{start.month}', index=False)

        success_message = f'Successful extraction for start = {start} -> pointer = {end}'
        print(success_message)
        with open('output/success.txt', 'a') as f:
            f.write(success_message + '\n')

    # to continue
