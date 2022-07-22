import requests
import pandas as pd
import gcsfs 
from scripts import gcp
from pyspark.sql import SparkSession

API_ENDPOINT = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?'
API_PARAMETERS = {'resource_id': '9a0bf149-308c-4bd2-832d-76c8e6cb47ed'} ##Resource ID for MAS' API for Domestic Interest Rates

def latest_date_api():
    #Retrieve latest date api to perform incremental extraction
    API_PARAMETERS['sort'] = 'end_of_day desc' 
    response = requests.get(API_ENDPOINT,params=API_PARAMETERS)
    df = pd.DataFrame(response.json()['result']['records'])
    return df['end_of_day'].iloc[0]

def extract_data(latest_date_api, latest_date_dl, folder, bucket, ti): #ti is task instance from airflow   
    offset = 100 #100 is used because it is the maximum number of data that is retrieved
    API_PARAMETERS['sort'] = 'end_of_day asc'

    if (latest_date_dl != ''):
        API_PARAMETERS['between[end_of_day]'] = f'{latest_date_dl},{latest_date_api}'   

    response = requests.get(API_ENDPOINT,params=API_PARAMETERS)
    main_df = pd.DataFrame(response.json()['result']['records'])
    totalData = int(response.json()['result']['total'])

    while offset < totalData:
        API_PARAMETERS['offset'] = offset
        response = requests.get(API_ENDPOINT,params=API_PARAMETERS) 
        temp_df = pd.DataFrame(response.json()['result']['records']) #temp_df refers to the temporary dataframe that was created to store the extracted api data
        main_df = pd.concat([main_df,temp_df])
        offset += 100 

    raw_data_uri = f'gs://{bucket}/{folder}/raw/{min(main_df["end_of_day"])}_{max(main_df["end_of_day"])}.json' #first date and last date as name of file
    main_df.to_json(raw_data_uri, index=False, orient='split') 
    ti.xcom_push(key='raw_data_uri', value=raw_data_uri)

def transform_data(ti, bucket):
    spark = SparkSession.builder.appName('Resale_Market').getOrCreate()
    raw_data_uri = ti.xcom_pull(key='raw_data_uri', task_ids='ingest_data_to_gcs')
    transformed_data_uri = raw_data_uri.replace('raw','transformed').replace('json','parquet')
    df_pyspark = spark.createDataFrame(pd.read_json(raw_data_uri, orient='split')) 

    #Drop irrelevant columns
    df_pyspark = df_pyspark.drop('aggregate_volume', 'calculation_method', 'commercial_bills_3m', 'highest_transaction', 'interbank_12m',
    'interbank_1m','interbank_1w', 'interbank_2m','interbank_3m','interbank_6m', 'interbank_overnight', 'lowest_transaction',
    'on_rmb_facility_rate', 'preliminary','published_date','sgs_repo_overnight_rate','sor_average','standing_facility_borrow',
    'standing_facility_deposit','usd_sibor_3m', 'timestamp')

    #Drop null values
    df_pyspark = df_pyspark.na.drop()

    #Drop duplicates
    df_pyspark = df_pyspark.distinct()
     
    #Changing the 'end_of_day' column datatype to date
    df_pyspark = df_pyspark.withColumn('end_of_day', df_pyspark['end_of_day'].cast('date'))

    #Convert datatypes in the pyspark dataframe to the equivalent in GCP 
    df_pyspark = gcp.convert_datatype(df_pyspark)

    df_pyspark.toPandas().to_parquet(transformed_data_uri) 
    shortened_transformed_data_uri = transformed_data_uri.replace(f'gs://{bucket}/','')
    ti.xcom_push(key='shortened_transformed_data_uri', value=shortened_transformed_data_uri) 