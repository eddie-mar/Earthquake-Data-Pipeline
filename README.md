# Earthquake-Data-Pipeline

This is an ETL project that prepares earthquake data from 1500 until present 2025. It extracts data from
 USGS API and cleans it with pandas and apache spark. The data is loaded into GCP and pulled into dbt for data modeling.

 ## Features

 <p align="center">
     <img width="75%" src="assets/earthquake-data.png" alt="code infrastructure">
 </p>

 - Extracted earthquake data from 1500 until 2025 from [US Geological Survey](https://earthquake.usgs.gov/fdsnws/event/1/) API
 - Used geopandas to fill country and region data from raw coordinates
 - Used jupyter notebooks and pandas to explore data and find inconsistencies, then cleaned using spark for efficient handling of huge data
 - Containerized extraction using docker
 - Created data infrastructure with terraform and uploaded data into GCP BigQuery
 - Data modeling done using dbt 

 ## Usage

 ### Data Extraction

The extraction script can be found in pipeline/extract_historical.py. The data was extracted from the [USGS API](https://earthquake.usgs.gov/fdsnws/event/1/). There is a limitation for the API call where only 20000 counts of data can be process or the request will
 result into an error. So, the flow of the script is done where a recursive check for the counts is first done using the API endpoint: https://earthquake.usgs.gov/fdsnws/event/1/count?starttime={start}&endtime={end}. After the 20k counts is confirmed, 
 the start and end time where used in making a request for the actual earthquake data in the endpoint: 
 https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start}&endtime={pointer}. This is done in a while loop
  until the date for the final data is reached. Exception handling were done in case there will be a failed request where 
  the dates that failed are recorded in a log file, to be used for making a request again. The raw data is saved into a csv file.

  ### Data Transformation

  to continue . .