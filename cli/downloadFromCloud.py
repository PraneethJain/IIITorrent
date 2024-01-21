from google.cloud import storage
import os
from rich.progress import Progress

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./IIITorrentClientCredentials.json"


def download_cs_folder(bucket_name, folder_name, destination_folder_name):
    try:
        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)

        blobs = list(bucket.list_blobs(prefix=folder_name))
        totalChunks = len(blobs)

        with Progress() as progress:
            task1 = progress.add_task("[blue]Downloading...", total=totalChunks)

            for blob in blobs:
                progress.update(task1, advance=1)
                destination_file_path = (
                    f"{destination_folder_name}/{blob.name[len(folder_name):]}"
                )

                os.makedirs(os.path.dirname(destination_file_path), exist_ok=True)

                blob.download_to_filename(destination_file_path)
                blob.delete()

        return True
    except Exception as e:
        print(e)
        return False


if __name__ == "__main__":
    folder = "behera"
    if download_cs_folder("iiitorrent", folder, folder):
        print("Downloaded Folder Successfully!")
    else:
        print("Download Failed")
