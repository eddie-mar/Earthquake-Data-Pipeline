from datetime import date, timedelta
from pprint import pprint

import argparse
import os
import pandas as pd
import requests
import time

os.makedirs('output', exist_ok=True)

COUNT_URL = 'https://earthquake.usgs.gov/fdsnws/event/1/count?'
QUERY_URL = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&'
ERROR_FILE = 'output/error.txt'
SUCCESS_FILE = 'output/success.txt'
DATA = 'earthquake-data-historical.csv'

def generate_timedelta(start, days=15):
    try:
        end = start + timedelta(days=days)
    except OverflowError:
        return ('end', days // 2)
    
    count_query = requests.get(f'{COUNT_URL}starttime={start}&endtime={end}')
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
        return generate_timedelta(start, days=days*2)   # Recursive call until 20k count is found
    

def extract_historical(columns, csv_file, start_date='1900-01-01', end_date='2025-06-30'):
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    pointer = start

    # Data extraction per API is limited to 20k counts per request (base on documentation)
    # In making request, we shall make a request first on the count to ensure we will not get pass the limit and create an error
    # Initial years has low counts, based on exploration, 1900-1950 and 1950-1965 has 20k, the next years have greater
    while start <= end:
        if pointer.year < 1950:
            pointer = date.fromisoformat('1950-01-01')
        elif pointer.year < 1965:
            pointer = date.fromisoformat('1965-01-01')
        elif (end - start) < timedelta(days=30):    # When nearing end, just set pointer to end
            pointer = end
        else:
            # Finding pointer with 20k counts, start counting at 15 days
            # delta = ('type', days)
            delta = generate_timedelta(start=start, days=15)
            if delta[0] == 'error':
                # When error, move start to 1 day
                start = start + timedelta(days=delta[1])
                continue
            pointer = start + timedelta(days=delta[1])
        
        query_response = requests.get(f'{QUERY_URL}starttime={start}&endtime={pointer}')
        if not query_response.ok:
            message = f'Failed extraction for start = {start} -> pointer = {pointer}\n\tError {query_response.status_code}: {query_response.text}'
            print(message)
            with open(ERROR_FILE, 'a') as f:
                f.write(message + '\n')
            
            start = pointer + timedelta(days=1)
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
                with open(ERROR_FILE, 'a') as f:
                    f.write(f'Data ingestion failure. Error {e} for start = {start} -> pointer = {pointer}\n')
                continue
            df_to_enter.append(data)
        
        df = pd.DataFrame(df_to_enter, columns=columns)
        df.to_csv(csv_file, index=False, header=False, mode='a')

        success_message = f'Successful extraction for start = {start} -> pointer = {pointer}'
        print(success_message)
        with open(SUCCESS_FILE, 'a') as f:
            f.write(success_message + '\n')
        
        start = pointer + timedelta(days=1)
        time.sleep(5)
        # According to documentation, requests must be made every 60s but during testing, its ok to continuous request. sleep for 5s to not hugely flood request


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Earthquake Data Extraction')
    parser.add_argument('--start_date', required=False, default='1900-01-01', help='Date in ISO format to start extraction')
    parser.add_argument('--end_date', required=False, default='2025-06-30', help='Date in ISO format to end extraction')

    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date

    # initiate files
    columns = ['place', 'time', 'magnitude', 'latitude', 'longitude', 'depth', 'alert', 'tsunami', 'tz', 'type']
    df = pd.DataFrame(columns=columns)

    path = 'output/csv_files/'
    os.makedirs(path, exist_ok=True)
    if not os.path.exists(f'{path}{DATA}'):
        df.to_csv(f'{path}{DATA}', index=False)

    if not os.path.exists(ERROR_FILE):
        with open(ERROR_FILE, 'w') as f:
            f.write('Errors\n')

    if not os.path.exists(SUCCESS_FILE):
        with open(SUCCESS_FILE, 'w') as f:
            f.write('Successfully extracted dates\n')

    extract_historical(columns, f'{path}{DATA}', start_date, end_date)


