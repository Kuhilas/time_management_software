from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from configparser import ConfigParser
import os

def config(filename='src\\data\\database.ini', section='keyvault'):
    parser = ConfigParser()
    
    if not os.path.exists(filename):
        raise Exception(f"Configuration file not found at {filename}")

    parser.read(filename)

    if parser.has_section(section):
        params = parser[section]
        keyvault_url = params['vault_url']

        # Authenticate using Managed Identity
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=keyvault_url, credential=credential)

        # Fetch database credentials
        db_config = {
            'host': client.get_secret("POSTGRESQL-HOST").value,
            'database': client.get_secret("POSTGRESQL-DATABASE").value,
            'user': client.get_secret("POSTGRESQL-USERNAME").value,
            'password': client.get_secret("POSTGRESQL-PASSWORD").value,
            'port': client.get_secret("POSTGRESQL-PORT").value or "5432",
            'sslmode': 'require',
        }

        # Fetch storage account details
        storage_config = {
            'storage_account_url': client.get_secret("STORAGE-ACCOUNT-URL").value,
            'storage_container_name': client.get_secret("STORAGE-CONTAINER-NAME").value,
            'storage_account_key': client.get_secret("STORAGE-ACCOUNT-KEY").value,
        }

        return db_config, storage_config
    else:
        raise Exception(f"Section {section} not found in the {filename} file")