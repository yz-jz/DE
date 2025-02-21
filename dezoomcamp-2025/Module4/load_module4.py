import os
import gzip
import shutil
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import time


#Change this to your bucket name
BUCKET_NAME = "module2dzoomcamp" 

#If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "../k.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

filename = "fhv_tripdata_2019-"
BASE_URL = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{filename}"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024  

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(month):
    url = f"{BASE_URL}{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, f"{filename}{month}.csv.gz")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  
    
    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            
            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")
        
        time.sleep(5)  
    
    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

# unzipping
    for i in file_paths:
        if i is not None:
            print(f"-- Extracting {i}")
            file = i.split(".")
            file = ".".join(file[:-1])
            with gzip.open(i,'rb') as f_in:
                with open(file,'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

    for i in range(len(file_paths)):
        if file_paths[i] is not None:
            ss = file_paths[i].split(".")
            ss = ".".join(ss[:-1])
            file_paths[i] = ss

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    print("All files processed and verified.")
