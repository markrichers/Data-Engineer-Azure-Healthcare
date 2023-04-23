import logging
import os
import pandas as pd
import pyarrow
import fastparquet
from io import StringIO
from datetime import datetime, timedelta
from knmy import knmy

import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, StandardBlobTier
from azure.storage.filedatalake import DataLakeServiceClient

def get_weather():
    disclaimer, stations, variables, data_1 = knmy.get_hourly_data(stations=[370], start=2021010101, end=2022101324,
                                                             inseason=True, variables=['ALL'], parse=True)
    
    data_1 = data_1.rename(columns={"STN":"Station_Code","H":"Time_Strip", "YYYYMMDD":"Datetime","T":"Tempurature","DD":"Wind_direction","FF":"Wind_Speed AVG_10_min", "FH":"Wind_Speed", "DR":"Precitipation"})
    data_1 = data_1[["Station_Code","Datetime","Wind_Speed","Tempurature","Time_Strip","Precitipation"]]
    data_1["Tempurature"] = data_1["Tempurature"] / 10
    
    return data_1

def read_csv_to_dataframe(data_1, file_delimiter= ','):
    
    df = pd.read_csv(data_1, delimiter=file_delimiter)

    return df

def write_dataframe_to_datalake(df, datalake_service_client, filesystem_name, dir_name, filename):

    file_path = f'{dir_name}/{filename}'

    file_client = datalake_service_client.get_file_client(filesystem_name, file_path)

    processed_df = df.to_parquet(index=True)
    #processed_df = df.to_csv(index=True) #false

    file_client.upload_data(data=processed_df,overwrite=True, length=len(processed_df))

    file_client.flush_data(len(processed_df))

    return True

def process_relational_data(df):
    df.rename(columns={'Station_Code':'station_code', 'Datetime': 'datetime', 'Wind_Speed':'wind_speed', 'Tempurature':'tempurature', 'Time_Strip':'time_strip', 'Precitipation': 'precitipation'},inplace=True)
    df['datetime']= pd.to_datetime(df["datetime"].astype(str), format="%Y%m%d")
    df['datetime'] = df['datetime'].dt.strftime('%m/%d/%Y')
    processed_df = df 
    
    # Check the blob file data
    #df.rename(columns={'Station_Code':'station_code', 'Datetime': 'datetime', 'Wind_Speed':'wind_speed', 'Tempurature':'tempurature', 'Time_Strip':'time_strip', 'Precitipation': 'precitipation'},inplace=True)  
    logging.info(processed_df.head(5))


    return processed_df

def load_relational_data(processed_df, datalake_service_client, filesystem_name, dir_name, file_format, file_prefix):
    now = datetime.today().strftime("%Y%m%d_%H%M%S")
    processed_filename = f'{file_prefix}_{now}.{file_format}'
    write_dataframe_to_datalake(processed_df, datalake_service_client, filesystem_name, dir_name, processed_filename)
    return True

def run_cloud_etl(datalake_service_client, filesystem_name, dir_name, file_format, file_prefix): #service_client, storage_account_url, source_container, archive_container, source_container_client, blob_file_list, 
    df = get_weather() 
    df = process_relational_data(df)
    result = load_relational_data(df, datalake_service_client, filesystem_name, dir_name, file_format, file_prefix)

    return result

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Parameters/Configurations
    arg_date = '2014-07-01'
    std_date_format = '%Y-%m-%d'
    processed_file_format = 'parquet' #csv
    processed_file_prefix = 'u1_demo_weather'

    # List of columns relevant for analysis
    #cols = ['User_ID', 'Device_ID','Ring_ID', 'MM level', 'SCR/min', 'SCL', 'Step count', 'aa', 'Time_ISO', 'Time_UNIX']

    try:
        # Set variables from appsettings configurations/Environment Variables.
        key_vault_name = os.environ["KEY_VAULT_NAME"]
        key_vault_Uri = f"https://{key_vault_name}.vault.azure.net"
        blob_secret_name = os.environ["ABS_SECRET_NAME"]

        adls_acct_name='dlsfunctionetldemo'
        adls_acct_url = f'https://{adls_acct_name}.dfs.core.windows.net/' 
        adls_fsys_name='processed-u2-data'
        adls_dir_name='weather_data'
        adls_secret_name='adls-access-key1'

        # Authenticate and securely retrieve Key Vault secret for access key value.
        az_credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=key_vault_Uri, credential= az_credential)
        access_key_secret = secret_client.get_secret(blob_secret_name)

        adls_service_client = DataLakeServiceClient(
            account_url = adls_acct_url,
            credential = az_credential
        )

        run_cloud_etl(
            datalake_service_client = adls_service_client,
            filesystem_name = adls_fsys_name,
            dir_name = adls_dir_name,
            file_format = processed_file_format,
            file_prefix = processed_file_prefix
        )

    except Exception as e:
        logging.info(e)

        return func.HttpResponse(
                f"!! This HTTP triggered function executed unsuccessfully. \n\t {e} ",
                status_code=200
        )

    return func.HttpResponse("This HTTP triggered function executed successfully.")