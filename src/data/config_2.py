from configparser import ConfigParser
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

def config(filename='src\\data\\database.ini', section='keyvault'):
    # Create a parser object to read the configuration
    parser = ConfigParser()
    
    # Check if the .ini file exists
    if not os.path.exists(filename):
        raise Exception(f"Configuration file not found at {filename}")

    # Read the configuration file
    parser.read(filename)

    # Check if the section exists in the .ini file
    if parser.has_section(section):
        # Get the parameters of the specified section
        params = parser[section]
        
        # Retrieve Key Vault URL and Vault Name from the .ini file
        keyvault_name = params['vault_name']
        keyvault_url = params['vault_url']
        
        # Authenticate using Managed Identity (no login needed if on Azure)
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=keyvault_url, credential=credential)
        
        # Fetch database credentials from Key Vault
        db_config = {
            'host': client.get_secret("POSTGRESQL-HOST").value,
            'database': client.get_secret("POSTGRESQL-DATABASE").value,
            'user': client.get_secret("POSTGRESQL-USERNAME").value,
            'password': client.get_secret("POSTGRESQL-PASSWORD").value,
            'port': client.get_secret("POSTGRESQL-PORT").value or "5432",
            'sslmode': 'require'
        }
        
        return db_config
    else:
        raise Exception(f"Section {section} not found in the {filename} file")