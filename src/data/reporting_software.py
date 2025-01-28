import psycopg2
from psycopg2.extras import RealDictCursor
from config import config
import configparser
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient
from datetime import datetime, timedelta

# Fetch daily and weekly report data from the database using Azure CLI (remember to activate venv .\venv\Scripts\activate)
def fetch_and_write_report():
    con = None
    daily_data = []
    weekly_data = []

    # Fetch daily report data
    daily_sql_query = """
        SELECT consultantName, customerName,
               SUM(dailyHours) AS total_hours
        FROM worktime
        WHERE startTime >= CURRENT_DATE AND startTime < CURRENT_DATE + INTERVAL '1 day'
        GROUP BY consultantName, customerName;
    """
    
    # Fetch weekly report data
    weekly_sql_query = """
        SELECT consultantName, customerName,
               SUM(dailyHours) AS total_hours
        FROM worktime
        WHERE startTime >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY consultantName, customerName;
    """
    
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)
        
        # Fetch and store daily data
        cursor.execute(daily_sql_query)
        daily_data = cursor.fetchall()
        
        # Fetch and store weekly data
        cursor.execute(weekly_sql_query)
        weekly_data = cursor.fetchall()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

    # Write both reports to a single file
    today_date = datetime.now().strftime('%d-%m-%Y')
    week_start_date = (datetime.now() - timedelta(days=7)).strftime('%d-%m-%Y')
    week_end_date = datetime.now().strftime('%d-%m-%Y')

    report_filename = f"consultant_report_{today_date}.txt"

    with open(report_filename, "w") as file:
        # Write daily report
        file.write(f"Consultant Daily Report ({today_date})\n")
        file.write("=" * 30 + "\n")
        for row in daily_data:
            file.write(f"Consultant: {row['consultantname']}, Customer: {row['customername']}, Total Hours: {row['total_hours']:.2f}\n")
        
        file.write("\n")

        # Write weekly report
        file.write(f"Consultant Weekly Report ({week_start_date} - {week_end_date})\n")
        file.write("=" * 30 + "\n")
        for row in weekly_data:
            file.write(f"Consultant: {row['consultantname']}, Customer: {row['customername']}, Total Hours: {row['total_hours']:.2f}\n")
    
    print(f"Report generated successfully: {report_filename}")
    return report_filename

# Upload the report to Azure Blob Storage
def upload_to_blob(blob_service_client: BlobServiceClient, container_name: str, file_path: str, blob_name: str):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)  # Set overwrite=True to replace existing files
    
    print(f"File '{file_path}' uploaded to blob '{blob_name}' in container '{container_name}'.")

# Load configuration from .ini file
def load_config():
    config = configparser.ConfigParser()
    config.read('src\\data\\database.ini')
    return config

if __name__ == "__main__":
    report_filename = fetch_and_write_report()
    
    config = load_config()
    container_name = config['blob']['container_name']
    storage_url = config['blob']['storage_url']

    credential = DefaultAzureCredential()

    blob_service_client = BlobServiceClient(storage_url, credential=credential)

    today_date = datetime.now().strftime('%d-%m-%Y')
    blob_name = report_filename

    upload_to_blob(blob_service_client, container_name, report_filename, blob_name)