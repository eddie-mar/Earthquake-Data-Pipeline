from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import date, datetime, timedelta

from add_region import add_country_region
from clean_historical import data_cleaning

import os

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
            start = date.fromisoformat(kwargs['start'])
            end = date.fromisoformat(kwargs['end'])
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

        return f'{path}earthquake-data-{start.year}-{start.month}'
        

    def process_region(**kwargs):
        ti = kwargs['ti']
        path = ti.xcom_pull(task_ids='extract_earthquake_monthly_data')

        try:
            start = date.fromisoformat(kwargs['start'])
            end = date.fromisoformat(kwargs['end'])
        except Exception as e:
            print(f'Error {e}')
            return

        data_wth_region = add_country_region(
            csv_file=f'{path}earthquake-data-{start.year}-{start.month}',
            world_boundaries=kwargs['world_boundaries'],
            path_to_save=f'{path}earthquake-data-wth-countries-{start.year}-{start.month}'
        )

        logs = f'output/logs_add_region_monthly.txt'
        if not os.path.exists(logs):
            with open(logs, 'w') as f:
                f.write('ADD COUNTRY AND REGION MONTHLY DATA PROCESS LOGS\n')
        
        success_message = f'Successfuly processed country data and region for {start.year}-{start.month}'
        print(success_message)
        with open(logs, 'a') as f:
            f.write(success_message + '\n')

        return data_wth_region


    def clean_data(**kwargs):
        ti = kwargs['ti']
        data_wth_region = ti.xcom_pull(task_ids='process_data_region')

        try:
            start = date.fromisoformat(kwargs['start'])
            end = date.fromisoformat(kwargs['end'])
        except Exception as e:
            print(f'Error {e}')
            return
        
        data_cleaning(
            filename=data_wth_region,
            partitions=0,
            path=f'output/parquet/batch-{start.year}-{start.month}',
            min_date=start,
            max_date=end
        )

        logs_cleaning = f'output/logs_data_cleaning.txt'
        if not os.path.exists(logs_cleaning):
            with open(logs_cleaning, 'w') as f:
                f.write('DATA CLEANING LOGS')
        
        success_message = f'Successfuly cleaned raw earthquake data for {start.year}-{start.month}'
        print(success_message)
        with open(logs_cleaning, 'a') as f:
            f.write(success_message + '\n')

        return f'output/parquet/batch-{start.year}-{start.month}'


    # tasks
    extract_task = PythonOperator(
        task_id='extract_earthquake_monthly_data',
        python_callable=extract_monthly
    )

    region_task = PythonOperator(
        task_id='process_data_region',
        python_callable=process_region,
        provide_context=True
    )

    clean_data_task = PythonOperator(
        task_id='run_pyspark_cleaning',
        python_callable=clean_data,
        provide_context=True
    )

    upload_task = BashOperator(
        task_id='upload_to_GCS',
        bash_commands="""
        gsutil -m cp -r {{}} {{}}
        """
    )

    dbt_task = BashOperator(
        task_id='dbt_run',
        bash_commands="""
        
        """
    )


    extract_task >> region_task >> clean_data_task >> upload_task >> dbt_task
    # to polish codes, to be continued