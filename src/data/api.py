from flask import Flask, jsonify
from reporting_software_f import fetch_and_write_report, upload_to_blob, load_config
from azure.storage.blob import BlobServiceClient
from datetime import datetime

app = Flask(__name__)

@app.route('/report', methods=['POST'])
def generate_report():
    try:
        report_filename = fetch_and_write_report()
        
        if report_filename:
            # Load storage configuration
            config = load_config()
            container_name = config['blob']['container_name']
            sas_url = config['blob']['sas_url']

            blob_service_client = BlobServiceClient(sas_url)

            # Generate blob name based on the filename
            blob_name = report_filename

            # Upload report to Azure Blob Storage
            upload_to_blob(blob_service_client, container_name, report_filename, blob_name)
            
            return jsonify({"message": "Report generated and uploaded successfully", "blob_name": blob_name}), 200
        else:
            return jsonify({"error": "Failed to generate report"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)