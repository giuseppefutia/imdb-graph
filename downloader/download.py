import os
import requests
import gzip
import shutil

IMDB_DATASETS_URL = "https://datasets.imdbws.com/"
FILES = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz"
]

def download_and_extract(url, output_folder="data"):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    file_name = os.path.basename(url)
    file_path = os.path.join(output_folder, file_name)
    extracted_file_path = file_path.replace(".gz", "")
    
    print(f"Downloading {file_name} from {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(file_path, "wb") as f:
        f.write(response.content)
    
    print(f"Extracting {file_name}...")
    with gzip.open(file_path, "rb") as f_in, open(extracted_file_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    
    os.remove(file_path)
    print(f"Extracted to {extracted_file_path}")

for file in FILES:
    download_and_extract(IMDB_DATASETS_URL + file)
