import argparse
import pyspark

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp

def data_cleaning(filename, partitions, path):
    spark = SparkSession.builder.appName('earthquake-data-cleaning').getOrCreate()
    df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(filename)
    df = df.withColumn('earthquake_datetime', from_unixtime(col('time')/1000)). \
        withColumn('earthquake_datetime', to_timestamp('earthquake_datetime'))
    df_clean = df.select('place', 'earthquake_datetime', 'magnitude', 'latitude', 'longitude', 'depth', 'alert', 'tsunami', 'type'). \
        filter((df.magnitude >= -1) & (df.magnitude <= 10) & (df.magnitude.isNotNull())). \
        filter((df.latitude >= -90) & (df.latitude <= 90)). \
        filter((df.longitude >= -180) & (df.longitude <= 180)). \
        dropDuplicates(subset=['place', 'earthquake_datetime']). \
        na.fill({'depth': 0})
    
    if partitions == 0:
        df = df_clean.write.parquet(f'output/{path}')
    else:
        df = df_clean.repartition(partitions).write.parquet(f'output/{path}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Earthquake Data Cleaning')

    parser.add_argument('--filename', required=True, default='output/earthquake-data.csv', help='Filename for the csv containing earthquake data')
    parser.add_argument('--partitions', required=True, default=0, help='Number of parquet file partitions')
    parser.add_argument('--path', required=True, default='historical/', help='Path to save parquet files')

    args = parser.parse_args()

    filename = args.filename
    partitions = args.partitions
    path = args.path

    data_cleaning(filename, partitions, path)