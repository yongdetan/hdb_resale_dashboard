import io
import math
import requests
import pandas as pd
import json
from scripts import gcp
from dateutil.relativedelta import relativedelta
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

def latest_date_api():
    #Retrieve latest date api to perform incremental extraction
    api_endpoint = 'https://data.gov.sg/api/action/datastore_search'
    api_parameters = {
        'resource_id': 'f1765b54-a209-4718-8d38-a39237f502b3',
        'sort':'month desc',
        'limit': 1
    }   
    response = requests.get(api_endpoint,params=api_parameters)
    df = pd.DataFrame(response.json()['result']['records'])
    return df['month'].iloc[0]

def extract_data(latest_date_api, latest_date_dl, folder, bucket, ti):   
    if (latest_date_dl == ''):
        #Extracting the entire dataset from the API instead of retrieving them batch by batch for maximum efficiency 
        api_endpoint = 'https://data.gov.sg/api/action/resource_show'
        api_parameters = {'id': 'f1765b54-a209-4718-8d38-a39237f502b3'}   
        response = requests.get(api_endpoint,params=api_parameters) 
        url = response.json()['result']['url']
        data_file = requests.get(url).content

        #Reading the dataset that was extracted from the API and drop any data that are produced this month since the month has yet to end
        df = pd.read_csv(io.StringIO(data_file.decode('utf-8')))
        currentMonth, currentYear = datetime.now().strftime('%m'), datetime.now().strftime('%Y')
        date = f'{currentYear}-{currentMonth}'
        df = df[df['month'] != date] #filter out the current month data
        
        raw_data_uri = f'gs://{bucket}/{folder}/raw/{min(df["month"])}_{max(df["month"])}.json'
        df.to_json(raw_data_uri, index=False, orient='split') #keep data as raw as possible
        ti.xcom_push(key='raw_data_uri', value=raw_data_uri)

    else:
        #Setting up the filters dict so that the program will specifically extract new data
        latest_date_api = datetime.strptime(latest_date_api, '%Y-%m')
        latest_date_dl = datetime.strptime(latest_date_dl, '%Y-%m')
        no_of_months = math.floor((latest_date_api-latest_date_dl).days / 30) 

        new_dates = []
        for months in range(no_of_months):
            new_date = datetime.strftime(latest_date_dl + relativedelta(months=months),'%Y-%m')
            currentMonth, currentYear = datetime.now().strftime('%m'), datetime.now().strftime('%Y')
            current_date_str = f'{currentYear}-{currentMonth}'
            if (new_date != current_date_str):
                new_dates.append(new_date)

        filters = {'month':new_dates} #dict for filtering the api data. Even though it says 'month', it actually consists of year and month
        api_endpoint = 'https://data.gov.sg/api/action/datastore_search'
        api_parameters = {
            'resource_id': 'f1765b54-a209-4718-8d38-a39237f502b3',
            'filters': json.dumps(filters), #use dump to convert dict to string format
            'limit': 500
        }   

        offset = 500 #offset must be equivalent to limit
        response = requests.get(api_endpoint,params=api_parameters)
        main_df = pd.DataFrame(response.json()['result']['records'])
        totalData = int(response.json()['result']['total'])

        while offset < totalData:
            api_parameters['offset'] = offset
            response = requests.get(api_endpoint,params=api_parameters) 
            temp_df = pd.DataFrame(response.json()['result']['records'])
            main_df = pd.concat([main_df,temp_df])
            offset += 500 

        raw_data_uri = f'gs://{bucket}/{folder}/raw/{min(main_df["month"])}_{max(main_df["month"])}.json'
        main_df.to_json(raw_data_uri, index=False, orient='split') 
        ti.xcom_push(key='raw_data_uri', value=raw_data_uri)

def transform_data(ti, bucket):
    spark = SparkSession.builder.appName('Resale_Market').getOrCreate()
    raw_data_uri = ti.xcom_pull(key='raw_data_uri', task_ids='ingest_data_to_gcs')
    transformed_data_uri = raw_data_uri.replace('raw','transformed').replace('json','parquet')
    df_pyspark = spark.createDataFrame(pd.read_json(raw_data_uri, orient='split'))

    #Changing datatype
    df_pyspark = df_pyspark.withColumn('month', df_pyspark['month'].cast('date'))

    #Creating maturity column
    MATURE_ESTATES = ['ANG MO KIO', 'BEDOK', 'BISHAN', 'BUKIT MERAH', 'BUKIT TIMAH', 'CENTRAL AREA', 'CLEMENTI','GEYLANG', 'KALLANG/WHAMPOA', 'MARINE PARADE', 'PASIR RIS', 'QUEENSTOWN', 'SERANGOON', 'TAMPINES', 'TOA PAYOH']
    NON_MATURE_ESTATES = ['BUKIT BATOK', 'BUKIT PANJANG', 'CHOA CHU KANG', 'HOUGANG', 'JURONG EAST', 'JURONG WEST', 'PUNGGOL', 'SEMBAWANG', 'SENGKANG', 'TENGAH', 'WOODLANDS', 'YISHUN']
    maturity_conditions = when(col('town').isin(MATURE_ESTATES), 'Mature') \
        .when(col('town').isin(NON_MATURE_ESTATES), 'Non-Mature') \
        .otherwise('Uncategorized')
    df_pyspark = df_pyspark.withColumn('maturity', maturity_conditions)

    #Creating region column ref https://www.hdb.gov.sg/about-us/history/hdb-towns-your-home
    NORTH = ['SEMBAWANG', 'WOODLANDS', 'YISHUN']
    NORTH_EAST = ['ANG MO KIO', 'HOUGANG', 'PUNGGOL', 'SENGKANG', 'SERANGOON']
    EAST = ['BEDOK', 'PASIR RIS', 'TAMPINES']
    WEST = ['BUKIT BATOK', 'BUKIT PANJANG', 'CHOA CHU KANG', 'CLEMENTI', 'JURONG EAST', 'JURONG WEST', 'TENGAH']
    CENTRAL = ['BISHAN', 'BUKIT MERAH', 'BUKIT TIMAH', 'CENTRAL AREA', 'GEYLANG', 'KALLANG/WHAMPOA', 'MARINE PARADE', 'QUEENSTOWN', 'TOA PAYOH']
    region_conditions =  when(col('town').isin(NORTH), 'North') \
        .when(col('town').isin(NORTH_EAST), 'North-East').when(col('town').isin(EAST), 'East') \
        .when(col('town').isin(WEST), 'West').when(col('town').isin(CENTRAL), 'Central') \
        .otherwise('Uncategorized')
    df_pyspark = df_pyspark.withColumn('region', region_conditions)
    
    #Convert datatypes in the pyspark dataframe to the equivalent in GCP 
    df_pyspark = gcp.convert_datatype(df_pyspark)

    df_pyspark.toPandas().to_parquet(transformed_data_uri) 
    shortened_transformed_data_uri = transformed_data_uri.replace(f'gs://{bucket}/','')
    ti.xcom_push(key='shortened_transformed_data_uri', value=shortened_transformed_data_uri)
