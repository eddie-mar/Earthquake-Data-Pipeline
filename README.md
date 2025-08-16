# Earthquake-Data-Pipeline

This is an ETL project that prepares earthquake data from 1500 until present 2025. It extracts data from
 USGS API and cleans it with pandas and apache spark. The data is loaded into GCP and pulled into dbt for data modeling.

 ## Features

 <p align="center">
     <img width="75%" src="assets/earthquake-data-infrastructure.png" alt="code infrastructure">
 </p>

 - Extracted earthquake data from 1500 until 2025 from [US Geological Survey](https://earthquake.usgs.gov/fdsnws/event/1/) API
 - Used geopandas to fill country and region data from raw coordinates
 - Used jupyter notebooks and pandas to explore data and find inconsistencies, then cleaned using spark for efficient handling of huge data
 - Containerized extraction using docker
 - Created data infrastructure with terraform and uploaded data into GCP BigQuery
 - Data modeling done using dbt 
<br>

 ### Data Extraction

The extraction script can be found in [extract_historical.py](pipeline/extract_historical.py). The data was extracted from the [USGS API](https://earthquake.usgs.gov/fdsnws/event/1/). There is a limitation for the API call where only 20000 counts of data can be process or the request will
 result into an error. So, the flow of the script is done where a recursive check for the counts is first done using the API endpoint: https://earthquake.usgs.gov/fdsnws/event/1/count?starttime={start}&endtime={end}. After the 20k counts is confirmed, 
 the start and end time where used in making a request for the actual earthquake data in the endpoint: 
 https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start}&endtime={pointer}. This is done in a while loop
  until the date for the final data is reached. Exception handling were done in case there will be a failed request where 
  the dates that failed are recorded in a log file, to be used for making a request again. The raw data is saved into a csv file.
<br>

### Data Transformation

The data transformation was done in two steps and script which are [add_region_chunks.py](pipeline/add_region_chunks.py) and 
[clean_historical.py](pipeline/clean_historical.py) for adding region from raw coordinates and cleaning the raw data, respectively. 
<br><br>
For adding country and region, a [shapefile](pipeline/world-boundaries/ne_10m_admin_0_countries.shp) that contains world countries geometric boundaries is needed. The raw coordinates in the earthquake data is spatially joined with the shapefile to determine the
 country of the raw location. The reading of the shapefile and sjoin is done with geopandas. After the spatial join, there are still
  null countries observed, looking at the place column of the data, some data contains country place. This was used to infer and fill those null data. The data was processed per chunks since the transformation process is heavy and the memory isn't sufficient for the 4 million data to be processed all at once. The script for processing all data at once is also present [here](pipeline/add_region.py). The shapefile was downloaded in this [url](https://www.naturalearthdata.com/downloads/110m-cultural-vectors/). For some reason, the file cannot be extracted using curl or wget. It can be downloaded by clicking the link under Admin o - Countries download countries.
<br><br>
Jupyter notebooks are available where I explored the data first. [world-boundaries.ipynb](data_exploration_notebooks/world-boundaries.ipynb) contains the testing i have done for the shapefile. [earthquake-data-exploration.ipynb](data_exploration_notebooks/earthquake-data-exploration.ipynb) and [earthquake-data-cleaning.ipynb](data_exploration_notebooks/earthquake-data-cleaning.ipynb) are also available where I experimented with exploring the data to decide how I should clean it. The sample results can also be seen to see the changes.
The process for cleaning data is done in [clean_historical.py](pipeline/clean_historical.py). Pyspark is used for cleaning the data
 since we are dealing with huge number of rows (about 4 million). The output is saved into a parquet format.

### Containerization and Usage

Docker was used to containerized the scripts. The dockerfile and requirements.txt is present in the [pipeline/](pipeline/) folder. 
The build's endpoint is a bash terminal. The scripts are run in each line. 
Follow these commands to build and run the scripts (assuming you have a running docker engine):

```bash
docker build -t earthquake-pipeline .
docker run -it \
    -v $(pwd)/Earthquake-Data-Pipeline/pipeline/output:/app/output \
    earthquake-pipeline bash
python3 extract_historical.py \
    --start_date 1500-01-01 \
    --end_date 2025-07-31
python3 add_region_chunks.py \
    --earthquake_data_source output/csv_files/earthquake-data-historical.csv
python3 clean_historical.py \
    --filename output/csv_files/earthquake-data-wth-countries.csv \
    --partitions 4 \
    --path output/parquet/historical/
```

### Loading Data into GCP and BigQuery

The infrastructure was created using terraform. The configuration files can be found [here](terraform/). Install terraform first based on your operating system, then edit the configuration files based on your project. 
```bash
terraform init
terraform plan
terraform apply
```
An active GCP account must be present. Google Cloud SDK is also used to upload files into GCS Buckets. The resulting parquet files are uploaded into GCS Buckets using gsutil command.
```bash
gsutil -m cp -r pipeline/output/parquet/historical/*.parquet gs://earthquake_data_buckets/historical
```

Then, the parquet files are loaded into an external table which is then used to create the staging table for our data in BigQuery.
```bash
CREATE OR REPLACE EXTERNAL TABLE `earthquake-data-467123.earthquake_stg_data.external_earthquake_data_historical`
OPTIONS (
    format='PARQUET',
    uris=['gs://earthquake_data_buckets/historical/*']
);

CREATE OR REPLACE TABLE `earthquake-data-467123.earthquake_stg_data.stg_earthquake_data_historical`
AS SELECT * FROM `earthquake-data-467123.earthquake_stg_data.external_earthquake_data_historical`;

CREATE OR REPLACE TABLE `earthquake-data-467123.earthquake_stg_data.stg_earthquake_data_monthly`
AS SELECT * FROM `earthquake-data-467123.earthquake_stg_data.external_earthquake_data_historical` 
WHERE 1=0;
```

An empty monthly table was also initialized.

### Data Modeling

<p align="center">
     <img width="75%" src="assets/dbt-lineage.png" alt="code infrastructure">
 </p>

After the data was loaded into the data warehouse, dbt is used to transform and create data models. The staging and fact tables are found in [dbt_files/models/core](dbt_files/models/core/). The staging model standardized naming and data types. In the fact table, a column named 'severity' was created to classify earthquake strength. Event year and decade was also generated which will be used for analytical models. 
<br><br>
The analytical models can be found in [dbt_files/models/analytics/](dbt_files/models/analytics/). Some models that can be found are countries' earthquake stats per decade, yearly earthquake statistics and changes, and most dangerous earthquake recorded based on alert level.