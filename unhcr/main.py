import requests
import pandas as pd
import time
from datetime import datetime
import os 
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
# --- RUN ID ---
RUN_ID = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

LOG_FILE = os.path.join(LOG_DIR, f"pipeline_{RUN_ID}.log")
def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{timestamp}] {message}"

    print(full_msg)

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(full_msg + "\n")


def download_csv(url, csv_suffix):
    try:
        filename = f"unhcr_{csv_suffix}.csv"
        page = 1
        all_data = []

        while True:
            params = {
                "page": page,
                "limit": 10000,
                "coo_all": "true",
                "coa_all": "true"
            }

            r = requests.get(url, params=params)
            data = r.json()

            items = data.get("items", [])

            if not items:
                break

            all_data.extend(items)

            log(f"Plik {filename}, strona {page}, rekordów: {len(items)}")

            page += 1

        df = pd.DataFrame(all_data)
        df.to_csv(f"unhcr_{csv_suffix}.csv", index=False)
        
        
        file_size = os.path.getsize(filename)
        log(f"Zapisano {filename} ({file_size} bajtów): {len(df)} rekordów")
        
        os.system("hdfs dfs -mkdir -p /unhcr/")
        os.system(f"hdfs dfs -rm -f /unhcr/{filename}")
        os.system(f"hdfs dfs -put {filename} /unhcr/")
        os.system(f"hdfs dfs -setrep -w 3 /unhcr/{filename}")
        log(f"Wgrano na HDFS z 3 replikami plik: {filename}")
    except Exception as e:
        log(f"Wystąpił błąd: {e}")

if __name__=="__main__":
    urlpopulation = "https://api.unhcr.org/population/v1/population/"
    urldemographics = "https://api.unhcr.org/population/v1/demographics/"
    download_csv(urlpopulation, "population")
    download_csv(urldemographics, "demographics")