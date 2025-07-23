from datetime import date, timedelta
from pprint import pprint

import pandas as pd
import requests
import time

COUNT_URL = 'https://earthquake.usgs.gov/fdsnws/event/1/count?'
QUERY_URL = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&'
ERROR_FILE = 'error.txt'
SUCCESS_FILE = 'success.txt'
DATA = 'earthquake-data.csv'

def generate_timedelta(start, days=15):
    try:
        end = start + timedelta(days=days)
    except OverflowError:
        return ('end', days / 2)
    
    count_query = requests.get(f'{COUNT_URL}starttime={start}&endtime={end}')
    if not count_query.ok:
        message = f'Count query error {count_query.status_code} at start = {start} and endtime = {end}'
        print(message)
        with open(ERROR_FILE, 'a') as f:
            f.write(message + '\n')
        return ('error', days)
    count_query = int(count_query.text)

    if count_query > 20000:
        return ('ok', days / 2)     # >20k count is found, thus, return the previous value days/2 that is <20k
    else:
        return generate_timedelta(start, days=days*2)   # Recursive call until 20k count is found
    

def extract_historical(columns, csv_file):
    start = date.fromisoformat('1900-01-01')
    end = date.fromisoformat('2025-06-30')

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
                start = start + timedelta(days=1)
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
    # initiate files
    columns = ['place', 'time', 'magnitude', 'lat', 'long', 'depth', 'alert', 'tsunami', 'tz', 'type']
    df = pd.DataFrame(columns=columns)
    df.to_csv(DATA, index=False)

    with open(ERROR_FILE, 'w') as f:
        f.write('Errors\n')

    with open(SUCCESS_FILE, 'w') as f:
        f.write('Successfully extracted dates\n')

    extract_historical(columns, DATA)


