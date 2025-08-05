import argparse
import geopandas as gpd
import json
import os
import pandas as pd

from shapely.geometry import Point
from tqdm import tqdm

def add_country_region(csv_file, world_boundaries, path_to_save, log_file):
    def get_country_from_place(place, country_list):
        place = str(place).lower()
        for country in country_list:
            if country in place:
                return country
        return None

    # read world boundaries data file
    world = gpd.read_file(world_boundaries)

    # load list of countries and region into dataframe
    world_data = world[['ADMIN', 'REGION_UN']].to_json()
    world_dict = json.loads(world_data)
    world_df = pd.DataFrame.from_dict(world_dict, orient='columns')
    world_df.columns = ['place_country', 'region']
    world_df['country_lower'] = world_df['place_country'].str.lower()  
    country_list = world_df['country_lower'].tolist()  

    # count total rows of original data
    with open(csv_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1
        print(f'{total_rows} total rows for processing.')
    
    partial_rows = 0

    # iterate dataframe to chunks for lower memory usage
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)
    first = True

    while True:
        try:
            df = next(df_iter)
            partial_rows += len(df)
            print(f'Processing {len(df)} chunks of data . .')

            geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
            gdf_points = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')

            gdf_with_country = gpd.sjoin(gdf_points, world[['geometry', 'ADMIN', 'REGION_UN']].rename(columns={
                'ADMIN': 'country',
                'REGION_UN': 'region'
            }), how='left', predicate='within')
            print('Spatial join done with dataframe and world points')
            # dataframe with country generated. still a lot of null countries, we will fill those based on the place name that contains a country    
            
            # apply function above to get country from place names
            # generate dataframe first of data with null countries
            with_null_df = gdf_with_country[gdf_with_country.country.isnull()].copy()
            with_null_df['get_country'] = with_null_df['place'].apply(lambda x: get_country_from_place(x, country_list))
            print('Inferred country from place columns')

            # merge dataframe with world_df to get region
            with_null_df = with_null_df.reset_index().merge(world_df, how='left', left_on='get_country', right_on='country_lower').set_index('index')

            # fill into official dataframe
            print('Filling inferred data into null values')
            gdf_with_country['country'] = gdf_with_country['country'].fillna(with_null_df['place_country'])
            gdf_with_country['region'] = gdf_with_country['region'].fillna(with_null_df['region_y'])
            gdf_with_country = gdf_with_country.drop(columns=[col for col in ['geometry', 'index_right'] if col in gdf_with_country.columns])
            gdf_with_country.to_csv(path_to_save, index=False, mode='w' if first else 'a', header=first)
            first = False
            print(f'Saved dataframe into csv {path_to_save}')

            message1 = f'Successfully processed {len(df)} chunks of data country and region'
            message2 = f'{partial_rows} / {total_rows} done processing'
            print(f'{message1}\n\t{message2}')
            with open(log_file, 'a') as f:
                f.write(f'{message1}\n\t{message2}\n')

        except StopIteration:
            print('Data processessing completed')
            break
    

if __name__ == '__main__':
    output_path = 'output/csv_files/'
    csv_file_path = f'{output_path}earthquake-data-historical.csv'
    path_to_save = f'{output_path}/earthquake-data-wth-countries.csv'
    world_boundaries = 'world-boundaries/ne_10m_admin_0_countries.shp'
    log_file = 'output/logs_add_region_historical.txt'

    parser = argparse.ArgumentParser(description='Generate country and region for earthquake data')
    parser.add_argument('--earthquake_data_source', required=False, default=csv_file_path, help='Path to get initial earthquake-data')
    parser.add_argument('--path_to_save', required=False, default=path_to_save, help='Path to save result')
    parser.add_argument('--world_boundaries', required=False, default=world_boundaries, help='Path to get world boundaries data')
    parser.add_argument('--logs', required=False, default=log_file, help='Where to write processing logs')

    args = parser.parse_args()

    earthquake_data_source = args.earthquake_data_source
    world_boundaries_data = args.world_boundaries
    save_file = args.path_to_save
    logs = args.logs

    if not os.path.exists(earthquake_data_source):
        raise Exception('Initial earthquake data not found. Please generate data first e.g. extract_historical.py')
    if not os.path.exists(world_boundaries_data):
        raise Exception('World boundary data not found. Download data first, refer to documentation')
    
    save_file_parent = os.path.dirname(save_file)
    if not os.path.exists(save_file_parent):
        os.makedirs(save_file_parent)

    # add log file
    if not os.path.exists(logs):
        with open(logs, 'w') as f:
            f.write('ADD COUNTRY AND REGION PROCESS LOGS\n')

    add_country_region(earthquake_data_source, world_boundaries_data, save_file, logs)