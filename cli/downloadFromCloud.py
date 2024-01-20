from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './IIITorrentClientCredentials.json'

def download_cs_file(bucket_name, file_name, destination_file_name): 
    try:
        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(file_name)
        blob.download_to_filename(destination_file_name)

        return True
    except:
        return False

def download_cs_folder(bucket_name, folder_name, destination_folder_name):
    try:
        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix=folder_name)

        for blob in blobs:
            # Construct the destination file path
            destination_file_path = f"{destination_folder_name}/{blob.name[len(folder_name):]}"

            # Ensure the destination folder exists
            os.makedirs(os.path.dirname(destination_file_path), exist_ok=True)

            # Download the blob to the destination file
            blob.download_to_filename(destination_file_path)
        return True
    except:
        return False


fileToDownload = "ok.txt"
if download_cs_file('iiitorrent', fileToDownload, fileToDownload):
    print("Downloaded File Successfully!")
else:
    print("Download Failed")

# folder = "randomFolder"
# if download_cs_file('iiitorrent', fileToDownload, fileToDownload):
#     print("Downloaded File Successfully!")
# else:
#     print("Download Failed")

