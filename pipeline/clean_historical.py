import argparse
import pyspark

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp

def data_cleaning(filename, partitions, path, min_date, max_date):
    spark = SparkSession.builder.appName('earthquake-data-cleaning').getOrCreate()
    df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(filename)

    if partitions == 0:
        df.write.parquet(path, mode='overwrite')
    else:
        df.repartition(partitions).write.parquet(path, mode='overwrite')

    df = spark.read.parquet(path)

    df = df.withColumn('earthquake_datetime', from_unixtime(col('time')/1000)). \
        withColumn('earthquake_datetime', to_timestamp('earthquake_datetime'))
    df_clean = (df
        .select('place', 'earthquake_datetime', 'magnitude', 'latitude', 'longitude', 'depth', 'country', 'region', 'alert', 'tsunami', 'type')
        .filter(
            (col('magnitude') >= -1) &
            (col('magnitude') <= 10) &
            (col('magnitude').isNotNull())
        )
        .filter(
            (col('latitude') >= -90) &
            (col('latitude') <= 90)
        )
        .filter(
            (col('longitude') >= -180) &
            (col('longitude') <= 180)
        )
        .filter(
            (col('earthquake_datetime') >= min_date) &
            (col('earthquake_datetime') <= max_date)
        )
        .dropDuplicates(subset=['place', 'earthquake_datetime'])
        .na.fill({'depth': 0})
            )
    
    if partitions == 0:
        df = df_clean.write.parquet(path, mode='overwrite')
        print(f'Data cleaned and saved into {path}')
    else:
        df = df_clean.coalesce(partitions).write.parquet(path, mode='overwrite')
        print(f'Data cleaned, repartitioned, and saved into {path}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Earthquake Data Cleaning')

    parser.add_argument('--filename', required=True, default='output/csv_files/earthquake-data-wth-countries.csv', help='Filename for the csv containing earthquake data')
    parser.add_argument('--partitions', required=False, default=0, help='Number of parquet file partitions')
    parser.add_argument('--path', required=True, default='output/parquet/historical/', help='Path to save parquet files')

    args = parser.parse_args()

    filename = args.filename
    partitions = int(args.partitions)
    path = args.path
    min_date = datetime.fromisoformat('1500-01-01')
    max_date = datetime.fromisoformat('2025-07-31')

    data_cleaning(filename, partitions, path, min_date, max_date)