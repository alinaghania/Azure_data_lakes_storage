# Import the required modules:
from google_drive_downloader import GoogleDriveDownloader as gdd
from azure.storage.filedatalake import DataLakeServiceClient
import os
import re
import requests

# Set up Azure Data Lake Storage credentials:
account_name = '<your_account_name>'
account_key = '<your_account_key>'
file_system_name = '<your_file_system_name>'

# The Google Drive link to download the data from:
google_drive_link = 'https://drive.google.com/drive/folders/13QnrPt6dHwBIDAYhtCCgJMAwE5m3RHeE?usp=sharing'

# Upload the files to Azure Data Lake Storage:
def upload_files_to_azure_data_lake(google_drive_link, filesystem_name):
    # Create a DataLakeServiceClient using Azure Data Lake Storage credentials
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", account_name),
                                           credential=account_key)
    
    # Create a file system client using the specified file system name
    filesystem_client = service_client.create_file_system(file_system=filesystem_name)

    # Retrieve the file IDs from the Google Drive folder
    file_ids = get_google_drive_folder_files(google_drive_link)

    # Upload each file to Azure Data Lake Storage
    for file_id in file_ids:
        file_name = file_id + '.txt'  # Set a desired file name for each file
        file_path = 'temp/' + file_name  # Set a desired path in Azure Data Lake Storage

        # Download the file from Google Drive using its file ID
        with gdd.open(file_id) as file:
            # Create the file in the Azure Data Lake Storage file system
            filesystem_client.create_file(file_path)
            
            # Upload the file to Azure Data Lake Storage
            filesystem_client.upload_file(file_path, file)

def get_google_drive_folder_files(google_drive_link):
    # Extract the folder ID from the Google Drive link
    folder_id = extract_folder_id(google_drive_link)

    # Construct the API URL to retrieve file information from the folder
    api_url = f"https://www.googleapis.com/drive/v3/files?q='{folder_id}' in parents&fields=files(id)"

    # Authenticate and make a GET request to retrieve file information
    response = requests.get(api_url)
    data = response.json()

    file_ids = []
    if "files" in data:
        # Extract the file IDs from the response data
        file_ids = [file["id"] for file in data["files"]]

    return file_ids

def extract_folder_id(google_drive_link):
    # Extract the folder ID from the Google Drive link
    match = re.search(r"/folders/([\w-]+)", google_drive_link)
    if match:
        folder_id = match.group(1)
        return folder_id

    raise ValueError("Invalid Google Drive link")

# Example usage:
upload_files_to_azure_data_lake(google_drive_link, file_system_name)
