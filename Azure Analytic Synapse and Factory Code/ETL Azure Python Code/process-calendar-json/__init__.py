import logging
import os
import json
import csv
import pandas as pd
import pyarrow
import fastparquet
from io import StringIO, BytesIO
from datetime import datetime, timedelta

import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, StandardBlobTier
from azure.storage.filedatalake import DataLakeServiceClient


def download_file_from_directory(adls_fsys_name, adls_dir_name):

    file_system_client = adls_service_client.get_file_system_client(file_system=adls_fsys_name)

    directory_client = file_system_client.get_directory_client(adls_dir_name)
    

    file_client = directory_client.get_file_client("test.json")

    download = file_client.download_file()
    
    downloaded_bytes = download.readall()

    logging.info("function one should have something to print")

    return downloaded_bytes

def upload_file_to_directory(adls_fsys_name, adls_dir_name, bytes):
    logging.info("Function 1 has passed function test")
    logging.info(bytes)
    try:
        file_system_client = adls_service_client.get_file_system_client(file_system=adls_fsys_name)

        directory_client = file_system_client.get_directory_client(adls_dir_name)
        
        file_client = directory_client.create_file("uploaded-file.txt")

        filecontent = str(bytes, 'utf-8')

        data = json.loads(filecontent)

        print(type(data))

        df = pd.json_normalize(data, 'value')

        df = df[['subject', 'start.dateTime', 'end.dateTime','categories']]

        df['categories'] = df['categories'].str.get(0)

        df.columns = df.columns.str.replace(".", "_")

        print(df)

        #df = df.to_csv(quotechar='"', quoting=csv.QUOTE_NONNUMERIC, index=None)
        df = df.to_parquet(index=False)

        file_client.upload_data(data=df, overwrite=True, length=len(df))

        file_client.flush_data(len(df))

    except Exception as e:
        print(e)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Parameters/Configurations
    processed_file_format = 'parquet'
    processed_file_prefix = 'u1_demo'

    try:
        global adls_service_client
        # Set variables from appsettings configurations/Environment Variables.
        key_vault_name = os.environ["KEY_VAULT_NAME"]
        key_vault_Uri = f"https://{key_vault_name}.vault.azure.net"
        blob_secret_name = os.environ["ABS_SECRET_NAME"]

        adls_acct_name='dlsfunctionetldemo'
        adls_acct_url = f'https://{adls_acct_name}.dfs.core.windows.net/' 
        adls_fsys_name='processed-u1-data'
        adls_dir_name='agenda_data'
        adls_secret_name='adls-access-key1'
 
        # Authenticate and securely retrieve Key Vault secret for access key value.
        az_credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=key_vault_Uri, credential= az_credential)
        access_key_secret = secret_client.get_secret(blob_secret_name)

        adls_service_client = DataLakeServiceClient(
            account_url = adls_acct_url,
            credential = az_credential
        )

        # Retrieve, structure, and upload agenda
        a=download_file_from_directory(adls_fsys_name, adls_dir_name)
        upload_file_to_directory(adls_fsys_name, adls_dir_name,bytes=a)

    except Exception as e:
        logging.info(e)

        return func.HttpResponse(
                f"!! This HTTP triggered function executed unsuccessfully. \n\t {e} ",
                status_code=200
        )

    return func.HttpResponse("This HTTP triggered function executed successfully.")