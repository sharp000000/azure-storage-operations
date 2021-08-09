import os, uuid, io
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient 

connect_str = '' 

data_parquet = pd.read_parquet('./data/test.parquet')
data_csv = pd.read_csv('./data/test.csv')

"""
    Function to write parquet and csv files into blobs in storage account using stream 

    Args:
        connect_str: Storage account connection string 
        container_name: Name of container in storage account
        file_name: Name of new or existing file with extension 
        df: Pandas dataframe which will be written into blob 
        file_type: Type of file: parquet or csv
        
    Returns:
        Blob exists or not
        
"""

def uploadBlobFiles(connect_str, container_name, file_name, df, file_type):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

    if file_type == 'parquet':
        buffer = io.BytesIO()
        df.to_parquet(buffer,index=False)
    else:
        buffer = io.StringIO()
        df.to_csv(buffer,encoding='utf-8',index=False)
    blob_client.upload_blob(buffer.getvalue(),overwrite=True)

    blob = BlobClient.from_connection_string(conn_str=connect_str, container_name=container_name, blob_name=file_name)

    return blob.exists()

uploadBlobFiles(connect_str,'test','test.parquet',data_parquet,'parquet')
uploadBlobFiles(connect_str,'test','test.csv',data_csv,'csv')