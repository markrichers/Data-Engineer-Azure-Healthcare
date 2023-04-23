import logging
import os

import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

'''Test if you can reach your storage account using the stored keyvault secret. 
Also make sure your 'local.setting.json' has the correct env variables set if testing locally.
If working in the cloud, use powershell script 12 to update them for your azure function in the cloud.'''
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Parameters/Configurations
    abs_acct_name='functionsdemodata'
    abs_acct_url=f'https://{abs_acct_name}.blob.core.windows.net/'
    abs_container_name='demo-functionetl-data'

    try:
        # Set variables from appsettings configurations/Environment Variables.
        key_vault_name = os.environ["KEY_VAULT_NAME"]
        key_vault_Uri = f"https://{key_vault_name}.vault.azure.net"
        blob_secret_name = os.environ["ABS_SECRET_NAME"]

        # Authenticate and securely retrieve Key Vault secret for access key value.
        az_credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=key_vault_Uri, credential= az_credential)
        access_key_secret = secret_client.get_secret(blob_secret_name)

    except Exception as e:
        logging.info(e)
        return func.HttpResponse(
                f"!! This HTTP triggered function executed unsuccessfully. \n\t {e} ",
                status_code=200
            )

    return func.HttpResponse("This HTTP triggered function executed successfully.")