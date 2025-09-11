from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import date, datetime, timedelta

from pipeline.add_region import add_country_region, WORLD_BOUNDARIES
from pipeline.clean_historical import data_cleaning, OUTPUT_PARQUET_DIR_PARENT
from pipeline.extract_historical import ERROR_FILE, SUCCESS_FILE, OUTPUT_DIR, LOGS_DIR, BASE_DIR

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
    start_date=datetime(2025, 8, 1),
    catchup=True,
) as dag:
    DBT_PROJECT_DIR = os.path.join(BASE_DIR, 'dbt_files')

    def extract_monthly(**kwargs):
        import pandas as pd
        import requests

        start = kwargs['data_interval_start'].date()
        end = kwargs['data_interval_end'].date() - timedelta(days=1)
        
        response = requests.get(f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start}&endtime={end}')
        if not response.ok:
            message =  f'Failed extraction for start = {start} -> pointer = {end}\n\tError {response.status_code}: {response.text}'
            print(message)
            with open(ERROR_FILE, 'a') as f:
                f.write(message + '\n')
            raise Exception(f'Request data error. Check logs at error.txt for {start} -> {end}')
            # raise failure for airflow to stop workflow. must not return Null since airflow will interpret it to be succesful and proceed with next tasks
        
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
                with open(ERROR_FILE, 'a') as f:
                    f.write(f'Data ingestion failure. Error {e} for start = {start} -> pointer = {end}\n')
                continue
            df_to_enter.append(data)
        
        df = pd.DataFrame(df_to_enter, columns=['place', 'time', 'magnitude', 'latitude', 'longitude', 'depth', 'alert', 'tsunami', 'tz', 'type'])
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        path = os.path.join(OUTPUT_DIR, f'batch-earthquake-data-{start.year}-{start.month:02d}.csv')

        df.to_csv(path, index=False)

        success_message = f'Successful extraction for start = {start} -> pointer = {end}'
        print(success_message)
        with open(SUCCESS_FILE, 'a') as f:
            f.write(success_message + '\n')

        return path
        

    def process_region(**kwargs):
        ti = kwargs['ti']
        path = ti.xcom_pull(task_ids='extract_earthquake_monthly_data')

        start = kwargs['data_interval_start'].date()
        
        # use the non-chunks version of add_region since monthly data is max at 20k counts, assume a normal programmer pc can run lol
        data_wth_region = add_country_region(
            csv_file=path,
            world_boundaries=kwargs['world_boundaries'],
            path_to_save=os.path.join(OUTPUT_DIR, f'batch-earthquake-data-processed-{start.year}-{start.month:02d}.csv')
        )

        logs = os.path.join(LOGS_DIR, 'logs_add_region_batch.txt')
        if not os.path.exists(logs):
            with open(logs, 'w') as f:
                f.write('ADD COUNTRY AND REGION MONTHLY DATA PROCESS LOGS\n')
        
        success_message = f'Successfuly processed country data and region for {start.year}-{start.month:02d}'
        print(success_message)
        with open(logs, 'a') as f:
            f.write(success_message + '\n')

        return data_wth_region


    def clean_data(**kwargs):
        ti = kwargs['ti']
        data_wth_region = ti.xcom_pull(task_ids='process_data_region')

        start = kwargs['data_interval_start'].date()
        end = kwargs['data_interval_end'].date() - timedelta(days=1)
        
        path_to_save = os.path.join(OUTPUT_PARQUET_DIR_PARENT, f'batch-{start.year}-{start.month:02d}')

        data_cleaning(
            data_source=data_wth_region,
            partitions=0,
            path=path_to_save,
            min_date=start,
            max_date=end
        )

        logs = os.path.join(LOGS_DIR, 'logs_data_cleaning.txt')
        if not os.path.exists(logs):
            with open(logs, 'w') as f:
                f.write('DATA CLEANING LOGS\n')
        
        success_message = f'Successfuly cleaned raw earthquake data for {start.year}-{start.month:02d}'
        print(success_message)
        with open(logs, 'a') as f:
            f.write(success_message + '\n')

        return {
            'local_dir': path_to_save,
            'bucket_dir': f'{start.year}-{start.month:02d}'
        }


    # tasks
    extract_task = PythonOperator(
        task_id='extract_earthquake_monthly_data',
        python_callable=extract_monthly
    )

    process_task = PythonOperator(
        task_id='process_data_region',
        python_callable=process_region,
        provide_context=True,
        op_kwargs={'world_boundaries': WORLD_BOUNDARIES}
    )

    clean_data_task = PythonOperator(
        task_id='run_pyspark_cleaning',
        python_callable=clean_data,
        provide_context=True
    )

    upload_to_bucket_task = BashOperator(
        task_id='upload_to_GCS',
        bash_command="""
            gsutil -m cp -r {{ ti.xcom_pull(task_ids='run_pyspark_cleaning')['local_dir'] }}/*.parquet \
                gs://{{ var.value.gcs_bucket }}/monthly/{{ ti.xcom_pull(task_ids='run_pyspark_cleaning')['bucket_dir'] }}/
            """
    )

    load_to_bq_task = GCSToBigQueryOperator(
        task_id='load_to_stg',
        bucket='{{ var.value.gcs_bucket }}',
        source_objects=["monthly/{{ ti.xcom_pull(task_ids='run_pyspark_cleaning')['bucket_dir'] }}/*"],
        destination_project_dataset_table='{{ var.value.project }}.{{ var.value.stg_dataset }}.stg_earthquake_data_monthly',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND'
    )

    dbt_task = BashOperator(
        task_id='dbt_run',
        bash_command=f"""
            export DBT_PROJECT={ '{{ var.value.project }}' } && \
            export DBT_DATASET={ '{{ var.value.dataset }}' } && \
            export DBT_KEYFILE={ '{{ var.value.keyfile }}' } && \
            dbt deps --project-dir { DBT_PROJECT_DIR } --profiles-dir { DBT_PROJECT_DIR } && \
            dbt run --project-dir { DBT_PROJECT_DIR } --profiles-dir { DBT_PROJECT_DIR }
                """
    )

    extract_task >> process_task >> clean_data_task >> upload_to_bucket_task >> load_to_bq_task >> dbt_task