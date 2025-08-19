from datetime import date, timedelta
from pprint import pprint

import argparse
import os
import pandas as pd
import requests
import time

COUNT_URL = 'https://earthquake.usgs.gov/fdsnws/event/1/count?'
QUERY_URL = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&'

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOGS_DIR = os.path.join(BASE_DIR,'output', 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
ERROR_FILE = os.path.join(LOGS_DIR, 'error.txt')
SUCCESS_FILE = os.path.join(LOGS_DIR, 'success.txt')

OUTPUT_DIR = os.path.join(BASE_DIR, 'output', 'csv_files')
os.makedirs(OUTPUT_DIR, exist_ok=True)
DATA_FILE = os.path.join(OUTPUT_DIR, 'historical-earthquake-data-raw.csv')

def generate_timedelta(start, days=15, error_file=ERROR_FILE, url=COUNT_URL):
    try:
        end = start + timedelta(days=days)
    except OverflowError:
        message = f'Overflow Error at start = {start} and endtime = {end}'
        print(message)
        with open(error_file, 'a') as f:
            f.write(message + '\n')
        return ('error', days // 2)
    
    count_query = requests.get(f'{url}starttime={start}&endtime={end}')
    if not count_query.ok:
        message = f'Count query error {count_query.status_code} at start = {start} and endtime = {end}'
        print(message)
        with open(ERROR_FILE, 'a') as f:
            f.write(message + '\n')
        return ('error', days)
    count_query = int(count_query.text)

    if count_query > 20000:
        return ('ok', days // 2)     # >20k count is found, thus, return the previous value days/2 that is <20k
    else:
        return generate_timedelta(start, days*2, error_file, url)   # Recursive call until 20k count is found
    

def extract_historical(columns, csv_file, start_date, end_date, error_file=ERROR_FILE, success_file=SUCCESS_FILE):
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    pointer = start

    # Data extraction per API is limited to 20k counts per request (base on documentation)
    # In making request, we shall make a request first on the count to ensure we will not get pass the limit and create an error
    # Initial years has low counts, based on exploration, 1500-1949 and 1949-1965 has 20k, the next years have greater
    while start <= end:
        if pointer.year < 1949:
            pointer = date.fromisoformat('1949-01-01')
        elif pointer.year < 1965:
            pointer = date.fromisoformat('1965-01-01')
        elif (end - start) < timedelta(days=30):    # When nearing end, just set pointer to end
            pointer = end
        else:
            # Finding pointer with 20k counts, start counting at 15 days
            # delta = ('type', days)
            delta = generate_timedelta(start=start, days=15, error_file=error_file, url=COUNT_URL)
            if delta[0] == 'error':
                # When error, move start to 1 day
                start = start + timedelta(days=delta[1])
                continue
            pointer = start + timedelta(days=delta[1])
        
        query_response = requests.get(f'{QUERY_URL}starttime={start}&endtime={pointer}')
        if not query_response.ok:
            message = f'Failed extraction for start = {start} -> pointer = {pointer}\n\tError {query_response.status_code}: {query_response.text}'
            print(message)
            with open(error_file, 'a') as f:
                f.write(message + '\n')
            
            start = pointer + timedelta(days=1)     # repeat process +1 day
            time.sleep(5)
            continue

        response = query_response.json()['features']
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
                pprint(entry)
                with open(error_file, 'a') as f:
                    f.write(f'Data ingestion failure. Error {e} for start = {start} -> pointer = {pointer}\n')
                continue
            df_to_enter.append(data)
        
        df = pd.DataFrame(df_to_enter, columns=columns)
        df.to_csv(csv_file, index=False, header=False, mode='a')

        success_message = f'Successful extraction for start = {start} -> pointer = {pointer}'
        print(success_message)
        with open(success_file, 'a') as f:
            f.write(success_message + '\n')
        
        start = pointer + timedelta(days=1)
        time.sleep(5)
        # According to documentation, requests must be made every 60s but during testing, its ok to continuous request. sleep for 5s to not hugely flood request


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Earthquake Data Extraction')
    parser.add_argument('--start_date', required=False, default='1500-01-01', help='Date in ISO format to start extraction')
    parser.add_argument('--end_date', required=False, default='2025-06-30', help='Date in ISO format to end extraction')

    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date

    # initiate files
    columns = ['place', 'time', 'magnitude', 'latitude', 'longitude', 'depth', 'alert', 'tsunami', 'tz', 'type']
    df = pd.DataFrame(columns=columns)

    if not os.path.exists(DATA_FILE):
        df.to_csv(DATA_FILE, index=False)

    if not os.path.exists(ERROR_FILE):
        with open(ERROR_FILE, 'w') as f:
            f.write('Errors\n')

    if not os.path.exists(SUCCESS_FILE):
        with open(SUCCESS_FILE, 'w') as f:
            f.write('Successfully extracted dates\n')

    extract_historical(columns, DATA_FILE, start_date, end_date, ERROR_FILE, SUCCESS_FILE)


