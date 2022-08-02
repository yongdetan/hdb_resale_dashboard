import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime,timedelta
from scripts import gcp, hdb

PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'hdb_resale_market') #same name as the one created in terraform
DATA_TITLE = 'hdb' 

def update_checker(latest_date_api, latest_date_dl):
    try: 
        if (datetime.strptime(latest_date_api, '%Y-%m').month - datetime.strptime(latest_date_dl, '%Y-%m').month >= 1):
            return True
        else:
            return False
    except:
        return True

default_args = {
    'owner': 'yongde',
    'start_date': datetime(2022, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG( 
    'hdb_etl_gbq_dag',
    default_args=default_args,
    description='Resale HDB ETL Pipeline',
    schedule_interval='0 12 2 * *', # run dag on 2nd of every month at 12pm 
    catchup=False,
)

check_for_new_data = ShortCircuitOperator(dag=dag,
    task_id='check_for_new_data',
    python_callable=update_checker,
    op_kwargs={      
        'latest_date_api':f'{hdb.latest_date_api()}',
        'latest_date_dl':f'{gcp.retrieve_latest_data_gcs(BUCKET, DATA_TITLE, "json")}'})

ingest_data_to_gcs = PythonOperator(dag=dag,
    task_id='ingest_data_to_gcs',
    python_callable=hdb.extract_data,
    op_kwargs={
        'latest_date_api':f'{hdb.latest_date_api()}',
        'latest_date_dl':f'{gcp.retrieve_latest_data_gcs(BUCKET, DATA_TITLE, "json")}',
        'folder':f'{DATA_TITLE}',
        'bucket':f'{BUCKET}'})

extract_and_transform_gcs = PythonOperator(dag=dag,
    task_id='extract_and_transform_data_from_gcs',
    python_callable=hdb.transform_data,
    op_kwargs={ 
        'bucket':f'{BUCKET}'})

gcs_to_bq = GCSToBigQueryOperator(
    dag=dag,
    task_id='gcs_to_bq',
    bucket=BUCKET,
    source_objects="{{ti.xcom_pull(task_ids='extract_and_transform_data_from_gcs', key='shortened_transformed_data_uri')}}",
    destination_project_dataset_table=f'{BIGQUERY_DATASET}.{DATA_TITLE}',
    source_format='PARQUET',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND',   
    time_partitioning = {'type':'MONTH', 'field':'month'}, #partition table with the month column with monthly 
    cluster_fields = ['town', 'flat_model', 'flat_type'], #clustering columns with high cardinality
    autodetect=True, 
    )

check_for_new_data >> ingest_data_to_gcs >> extract_and_transform_gcs >> gcs_to_bq


