# Cloud func code: load file from(GCS-BQ)

import os
from google.cloud import bigquery 

def gcs_to_bigquery(data,context):
    # get gcs details
    bucket_name = data['bucket']
    file_name = data['name'] # sales.csv
    source_uri = f'gs://{bucket_name}/{file_name}'

    # get bq details 
    dataset_id = 'TAsk_1'
    table_name = file_name.split('.')[0]+'Table' # salesTable
    table_id = f'{dataset_id}.{table_name}'

    # Construct a BigQuery client object.
    client = bigquery.Client()

    file_extension = file_name.split('.')[-1] #csv

    if file_extension == 'csv':
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
        )

    elif file_extension == 'json':
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        )

    elif file_extension == 'parquet':
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
        )

    load_job = client.load_table_from_uri(
        source_uri, 
        table_id, 
        job_config=job_config
    )

    load_job.result()  # Waits for the job to complete.

    return f'successfully the loading job for {file_name} to {table_id}'

    
