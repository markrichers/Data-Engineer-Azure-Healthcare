import logging
import os
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

def return_blob_files(container_client, arg_date, std_date_format):
    start_date = datetime.strptime(arg_date, std_date_format).date() - timedelta(days=1)

    blob_files = [blob for blob in container_client.list_blobs() if blob.creation_time.date() >= start_date]

    return blob_files

def read_csv_to_dataframe(container_client, filename, file_delimiter= ','):
    blob_client = container_client.get_blob_client(blob=filename)

    # Retrieve extract blob file
    blob_download = blob_client.download_blob()

    # Read blob file into DataFrame
    blob_data = StringIO(blob_download.content_as_text())
    df = pd.read_csv(blob_data,delimiter=file_delimiter, skiprows=1, header=None) #index_col=None,
    return df

def write_dataframe_to_datalake(df, datalake_service_client, filesystem_name, dir_name, filename):

    file_path = f'{dir_name}/{filename}'

    file_client = datalake_service_client.get_file_client(filesystem_name, file_path)

    processed_df = df.to_parquet(index=False)
    #processed_df = df.to_csv(index=False)

    file_client.upload_data(data=processed_df,overwrite=True, length=len(processed_df))

    file_client.flush_data(len(processed_df))

    return True

def archive_cooltier_blob_file(blob_service_client, storage_account_url, source_container, archive_container, blob_list):

    for blob in blob_list:
        blob_name = blob.name
        source_blob_url = f'{storage_account_url}{source_container}/{blob_name}'

        # Copy source blob file to archive container and change blob access tier to 'Cool'
        archive_blob_client = blob_service_client.get_blob_client(archive_container, blob_name)

        archive_blob_client.start_copy_from_url(source_url=source_blob_url, standard_blob_tier=StandardBlobTier.Cool)

        (blob_service_client.get_blob_client(source_container, blob_name)).delete_blob(delete_snapshots='include')

    return True

def ingest_relational_data(container_client, blob_file_list):
    df = pd.concat([read_csv_to_dataframe(container_client=container_client, filename=blob_name.name) for blob_name in blob_file_list], ignore_index=False)
    
    return df

def process_relational_data(df):
    #Rename and clean columns
    df.columns = ["user_id","ring_id","mm_level","scr_min","scl","step_count","calibration_lvl","time_iso","time_unix"]

    # # Create column to match/merge 'time_strip' column in weather data
    df['time_new'] = df['time_iso'].astype(str).str[11:16]
    df['time_new'] = df['time_new'].str.replace(':','.')
    df['time_new'] = df['time_new'].astype(float)

    # # Create column to match 'datetime' column in weather data
    df['datetime'] = df['time_iso'].astype(str).str[0:10]

    # print(df.dtypes)

    # Filter out uncalibrated data
    df = df[(df['datetime'] > '1971-01-01')]

    # Adjust to match and set data type
    df['time_strip'] = df['time_new'].astype(int)
    df.rename(columns={'Time_Strip':'time_strip'}, inplace=True)
    df[['time_strip']] = df[['time_strip']].astype(int)

    # Date format for powerbi
    df['datetime']=pd.to_datetime(df["datetime"].astype(str), format="%Y-%m-%d")
    df['datetime'] = df['datetime'].dt.strftime('%m/%d/%Y')

    processed_df = df 
    
    # # Check the blob file data
    # #logging.info(processed_df.head(5))

    return processed_df

def load_relational_data(processed_df, datalake_service_client, filesystem_name, dir_name, file_format, file_prefix):
    now = datetime.today().strftime("%Y%m%d_%H%M%S")
    processed_filename = f'{file_prefix}_{now}.{file_format}'
    write_dataframe_to_datalake(processed_df, datalake_service_client, filesystem_name, dir_name, processed_filename)
    return True

def run_cloud_etl(service_client, storage_account_url, source_container, archive_container, source_container_client, blob_file_list, datalake_service_client, filesystem_name, dir_name, file_format, file_prefix):
    df = ingest_relational_data(source_container_client, blob_file_list)
    df = process_relational_data(df)
    result = load_relational_data(df, datalake_service_client, filesystem_name, dir_name, file_format, file_prefix)
    result = archive_cooltier_blob_file(service_client, storage_account_url, source_container, archive_container, blob_file_list)

    return result

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Parameters/Configurations
    arg_date = '2014-07-01'
    std_date_format = '%Y-%m-%d'
    processed_file_format = 'parquet' #csv
    processed_file_prefix = 'u2_demo'

    # List of columns relevant for analysis
    #cols = ['User_ID', 'Device_ID','Ring_ID', 'MM level', 'SCR/min', 'SCL', 'Step count', 'aa', 'Time_ISO', 'Time_UNIX']

    try:
        # Set variables from appsettings configurations/Environment Variables.
        key_vault_name = os.environ["KEY_VAULT_NAME"]
        key_vault_Uri = f"https://{key_vault_name}.vault.azure.net"
        blob_secret_name = os.environ["ABS_SECRET_NAME"]

        abs_acct_name='functionsdemodata'
        abs_acct_url=f'https://{abs_acct_name}.blob.core.windows.net/'
        abs_container_name='moodmetric-u2'
        archive_container_name = 'moodmetric-u2-archive'

        adls_acct_name='dlsfunctionetldemo'
        adls_acct_url = f'https://{adls_acct_name}.dfs.core.windows.net/' 
        adls_fsys_name='processed-u2-data'
        adls_dir_name='ring_data'
        adls_secret_name='adls-access-key1'

        # Authenticate and securely retrieve Key Vault secret for access key value.
        az_credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=key_vault_Uri, credential= az_credential)
        access_key_secret = secret_client.get_secret(blob_secret_name)

        # Initialize Azure Service SDK Clients
        abs_service_client = BlobServiceClient(
            account_url = abs_acct_url,
            credential = az_credential
        )

        abs_container_client = abs_service_client.get_container_client(container=abs_container_name)

        adls_service_client = DataLakeServiceClient(
            account_url = adls_acct_url,
            credential = az_credential
        )

        # Run ETL Application
        process_file_list = return_blob_files(
            container_client = abs_container_client,
            arg_date = arg_date,
            std_date_format = std_date_format
        )

        run_cloud_etl(
            source_container_client = abs_container_client,
            blob_file_list = process_file_list,
            datalake_service_client = adls_service_client,
            filesystem_name = adls_fsys_name,
            dir_name = adls_dir_name,
            file_format = processed_file_format,
            file_prefix = processed_file_prefix,
            service_client = abs_service_client,
            storage_account_url = abs_acct_url,
            source_container = abs_container_name,
            archive_container = archive_container_name
        )

    except Exception as e:
        logging.info(e)

        return func.HttpResponse(
                f"!! This HTTP triggered function executed unsuccessfully. \n\t {e} ",
                status_code=200
        )

    return func.HttpResponse("This HTTP triggered function executed successfully.")