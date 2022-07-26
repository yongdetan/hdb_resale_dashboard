from google.cloud import storage

#Convert dataframe datatypes to its equivalent in GCP to prevent errors
def convert_datatype(dataframe):
    for data in dataframe.dtypes:
        if ('int' in data[1]):
            dataframe=dataframe.withColumn(data[0], dataframe[data[0]].cast('integer'))
        if ('double' in data[1]):
            dataframe=dataframe.withColumn(data[0], dataframe[data[0]].cast('float'))
    return dataframe

def retrieve_latest_data_gcs(bucket, folder, file_format):
    try:
        storage_client = storage.Client()

        #retrieve all blobs form cloud storage and store them in a list
        blobs = storage_client.list_blobs(bucket, prefix=f'{folder}/raw/') 

        #sort the blob list based on their updated date and in descending order
        sorted_blob_list = sorted(blobs, key=lambda x: x.updated, reverse=True) 
        
        #retrieve the name of the latest file from the sorted list
        latest_file = str(sorted_blob_list[0].name)

        #remove the file format of the latest file 
        latest_file = latest_file.replace(f'.{file_format}','')

        #split the latest file name based on _ (all file names have the date of the data which is separated by _ e.g. 2022-01-01_2020-01-03)
        dates = latest_file.split('_')
        return dates[1]
    except:
        return ''
