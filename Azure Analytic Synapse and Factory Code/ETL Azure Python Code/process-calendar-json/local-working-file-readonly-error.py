import logging
import os
import json
import csv
import pandas as pd
import pyarrow
import fastparquet
from io import StringIO
from datetime import datetime, timedelta

import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, StandardBlobTier
from azure.storage.filedatalake import DataLakeServiceClient


def download_file_from_directory(adls_fsys_name, adls_dir_name):
    try:
        file_system_client = adls_service_client.get_file_system_client(file_system=adls_fsys_name)

        directory_client = file_system_client.get_directory_client(adls_dir_name)
        
        local_file = open("test.json",'wb')

        file_client = directory_client.get_file_client("test.json")

        download = file_client.download_file()

        downloaded_bytes = download.readall()

        local_file.write(downloaded_bytes)

        local_file.close()

    except Exception as e:
     print(e)

def read_and_extract_graph_json():
    with open('test.json','rb') as f:

        data = json.load(f)

        ## Enter first array 
        x = data[0]

        ## Get dictionairy item of key named 'value'
        z = x['value']

        ## Set parameters
        lengthOfEvents = len(z)
        eventNumber = 0

        # open new csv in the write mode
        outputFile = open('output.csv', 'w')

        # create the csv writer
        writer = csv.writer(outputFile)

    ## Loop lists and extract values from each dictionary 
        for index in range(lengthOfEvents):
            event = z[eventNumber]
            eventNumber += 1

        ## Collect data from outlook event
            row = [event['subject'],event['start']['dateTime'],event['end']['dateTime'],event['categories']]

        # write a row to the csv file
            writer.writerow(row)
        
    # close the file
    outputFile.close()

    ## Read and output csv in dataframe
    colnames=['cal_subject', 'cal_start_time', 'cal_end_time', 'event_category'] 
    userCaldf = pd.read_csv('output.csv', names=colnames, header=None, index_col=False)

    ## Data type and sort frame
    userCaldf["cal_start_time"] = pd.to_datetime(userCaldf["cal_start_time"])
    userCaldf.sort_values(by='cal_start_time', inplace=True)

    print(userCaldf.to_string())
    userCaldf.to_csv('output.csv', index=False)

def upload_file_to_directory(adls_fsys_name, adls_dir_name):
    try:

        file_system_client = adls_service_client.get_file_system_client(file_system=adls_fsys_name)

        directory_client = file_system_client.get_directory_client(adls_dir_name)
        
        file_client = directory_client.create_file("uploaded-file.txt")

        local_file = open("output.csv",'r')

        file_contents = local_file.read()

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

        file_client.flush_data(len(file_contents))

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
        download_file_from_directory(adls_fsys_name, adls_dir_name)
        read_and_extract_graph_json()
        upload_file_to_directory(adls_fsys_name, adls_dir_name)

    except Exception as e:
        logging.info(e)

        return func.HttpResponse(
                f"!! This HTTP triggered function executed unsuccessfully. \n\t {e} ",
                status_code=200
        )

    return func.HttpResponse("This HTTP triggered function executed successfully.")