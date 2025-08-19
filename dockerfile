FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    vim \
    gnupg \
    lsb-release \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt 

# airflow
RUN pip install "apache-airflow==2.9.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.12.txt"
RUN pip install apache-airflow-providers-google

# dbt
RUN pip install dbt-bigquery==1.8.2

# google cloud sdk
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    apt-get update && apt-get install -y google-cloud-cli

ENV PYTHONPATH="/app:${PYTHONPATH}"

COPY . .




