import argparse
import geopandas as gpd
import json
import os
import pandas as pd

from shapely.geometry import Point
from tqdm import tqdm

def add_country_region(csv_file, world_boundaries, path_to_save):
    df = pd.read_csv(csv_file)

    geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
    gdf_points = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')

    world = gpd.read_file(world_boundaries)

    gdf_with_country = gpd.sjoin(gdf_points, world[['geometry', 'ADMIN', 'REGION_UN']].rename(columns={
        'ADMIN': 'country',
        'REGION_UN': 'region'
    }), how='left', predicate='within')
    # dataframe with country generated. still a lot of null countries, we will fill those based on the place name that contains a country

    # load list of countries and region into dataframe
    world_data = world[['ADMIN', 'REGION_UN']].to_json()
    world_dict = json.loads(world_data)
    world_df = pd.DataFrame.from_dict(world_dict, orient='columns')
    world_df.columns = ['place_country', 'region']
    world_df['country_lower'] = world_df['place_country'].str.lower()

    def get_country_from_place(place, country_list):
        place = str(place).lower()
        for country in country_list:
            if country in place:
                return country
        return None
    
    # apply function above to get country from place names
    # generate dataframe first of data with null countries
    with_null_df = gdf_with_country[gdf_with_country.country.isnull()].copy()
    country_list = world_df['country_lower'].tolist()
    with_null_df['get_country'] = with_null_df['place'].apply(lambda x: get_country_from_place(x, country_list))
    
    # merge dataframe with world_df to get region
    with_null_df = with_null_df.reset_index().merge(world_df, how='left', left_on='get_country', right_on='country_lower').set_index('index')

    # fill into official dataframe
    gdf_with_country['country'] = gdf_with_country['country'].fillna(with_null_df['place_country'])
    gdf_with_country['region'] = gdf_with_country['region'].fillna(with_null_df['region_y'])
    gdf_with_country.drop(columns=['geometry', 'index_right']).to_csv(path_to_save, index=False)

    print(f'Successfully processed and added country and region to earthquake data. File saved in {path_to_save}')


if __name__ == '__main__':
    output_path = 'output/csv_file/'
    csv_file_path = f'{output_path}earthquake-data-historical.csv'
    path_to_save = f'{output_path}/earthquake-data-wth-countries.csv'
    world_boundaries = 'world-boundaries/ne_10m_admin_0_countries.shp'

    parser = argparse.ArgumentParser(description='Generate country and region for earthquake data')
    parser.add_argument('--earthquake_data_source', required=False, default=csv_file_path, help='Path to get initial earthquake-data')
    parser.add_argument('--path_to_save', required=False, default=path_to_save, help='Path to save result')
    parser.add_argument('--world_boundaries', required=False, default=world_boundaries, help='Path to get world boundaries data')

    args = parser.parse_args()

    earthquake_data_source = args.earthquake_data_source
    world_boundaries_data = args.world_boundaries
    save_file = args.path_to_save

    if not os.path.exists(earthquake_data_source):
        raise Exception('Initial earthquake data not found. Please generate data first e.g. extract_historical.py')
    if not os.path.exists(world_boundaries_data):
        raise Exception('World boundary data not found. Download data first, refer to documentation')
    
    save_file_parent = os.path.dirname(save_file)
    if not os.path.exists(save_file_parent):
        os.makedirs(save_file_parent)

    add_country_region(earthquake_data_source, world_boundaries_data, save_file)

