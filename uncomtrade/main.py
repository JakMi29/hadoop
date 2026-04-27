import requests
import csv
import os
import time
import subprocess
from datetime import datetime

# --- CONFIG ---
API_KEYS = [
    "396453ffcb994eefa9c3b5f90850673b",
    "1984c211f7ba4f0e88ff757dd0019209",
]

current_key_idx = 0

def get_headers():
    return {
        "Ocp-Apim-Subscription-Key": API_KEYS[current_key_idx]
    }

def switch_api_key():
    global current_key_idx

    if current_key_idx < len(API_KEYS) - 1:
        current_key_idx += 1
        log(f"[API] Switched to key #{current_key_idx + 1}")
        return True
    else:
        log("[API] No more API keys available")
        return False
    
BASE_URL = "https://comtradeapi.un.org/data/v1/get/C/A/HS"

MAX_RECORDS = 100000

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
LOG_DIR = os.path.join(BASE_DIR, "logs")
STATE_DIR = os.path.join(BASE_DIR, "state")

HDFS_DIR = "/data/un_comtrade"

RUN_ID = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

LOG_FILE = os.path.join(LOG_DIR, f"pipeline_{RUN_ID}.log")
CHECKPOINT_LATEST = os.path.join(STATE_DIR, "checkpoint_latest.txt")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)

# --- LOGGING ---
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full = f"[{ts}] {msg}"
    print(full)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(full + "\n")

# --- CHECKPOINT ---
def load_checkpoint():
    if not os.path.exists(CHECKPOINT_LATEST):
        return None, None
    with open(CHECKPOINT_LATEST) as f:
        y, c = f.read().strip().split(",")
        return int(y), int(c)

def save_checkpoint(year, cmd):
    with open(CHECKPOINT_LATEST, "w") as f:
        f.write(f"{year},{cmd}")

# --- CSV CLEAN ---
def remove_cmd_from_csv(path, cmd):
    if not os.path.exists(path):
        return

    temp = path + ".tmp"

    with open(path, "r", encoding="utf-8") as inp, \
         open(temp, "w", newline="", encoding="utf-8") as out:

        reader = csv.DictReader(inp)
        writer = csv.DictWriter(out, fieldnames=reader.fieldnames)

        writer.writeheader()

        removed = 0
        for row in reader:
            if row["cmdCode"] == str(cmd).zfill(2):
                removed += 1
                continue
            writer.writerow(row)

    os.replace(temp, path)
    log(f"[CLEAN] Removed {removed} rows for cmd={cmd}")

# --- HDFS ---
def hdfs_exists(path):
    return subprocess.run(["hdfs", "dfs", "-test", "-e", path]).returncode == 0

def hdfs_delete(path):
    subprocess.run(["hdfs", "dfs", "-rm", "-f", path],
                   stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)

def upload_to_hdfs(local_path):
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_DIR])

    name = os.path.basename(local_path)
    hdfs_path = f"{HDFS_DIR}/{name}"

    subprocess.run(["hdfs", "dfs", "-put", local_path, HDFS_DIR], check=True)
    subprocess.run(["hdfs", "dfs", "-setrep", "-w", "3", hdfs_path], check=True)

    log(f"[HDFS] Uploaded {name} (replication=3)")

# --- SYNC CSV -> HDFS ---
def sync_csv_to_hdfs(skip_year):
    log("[SYNC] Checking local CSV...")

    for f in os.listdir(DATA_DIR):

        # ✔ tylko pliki csv
        if not f.endswith(".csv"):
            continue

        # ✔ tylko format YYYY.csv
        if not (len(f) == 8 and f[:4].isdigit() and f[4:] == ".csv"):
            log(f"[SYNC] Ignoring invalid filename: {f}")
            continue

        year = int(f[:4])

        # ✔ pomijamy niepełny rok
        if year == skip_year:
            log(f"[SYNC] Skipping incomplete year: {year}")
            continue

        local = os.path.join(DATA_DIR, f)
        hdfs = f"{HDFS_DIR}/{f}"

        if not hdfs_exists(hdfs):
            log(f"[SYNC] Uploading {f}")
            upload_to_hdfs(local)
        else:
            log(f"[SYNC] Exists in HDFS: {f}")

# --- FETCH ---
def fetch(year, cmd):
    params = {
        "period": str(year),
        "cmdCode": str(cmd).zfill(2),
        "flowCode": "M,X",
        "maxRecords": str(MAX_RECORDS),
        "includeDesc": "true"
    }

    return requests.get(BASE_URL, params=params, headers=get_headers(), timeout=60)

# --- FETCH WITH RETRY ---
def fetch_retry(year, cmd, retries=5):

    for attempt in range(1, retries + 1):
        try:
            res = fetch(year, cmd)

            if res.status_code == 200:
                return res

            if res.status_code == 403:
                log("[API] Quota exceeded for current key")

                if switch_api_key():
                    continue
                else:
                    return res

            raise Exception(res.text)

        except Exception as e:
            log(f"[RETRY] y={year} cmd={cmd:02d} try={attempt} err={e}")

            if attempt == retries:
                log(f"[FAILED] y={year} cmd={cmd:02d}")
                return None

            time.sleep(2 * attempt)

# --- SAVE ---
def save_csv(data, year):
    if not data:
        return

    path = os.path.join(DATA_DIR, f"{year}.csv")
    exists = os.path.exists(path)

    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=data[0].keys())

        if not exists:
            w.writeheader()

        w.writerows(data)

# --- MAIN ---
def run_pipeline():

    log(f"[START] RUN_ID={RUN_ID}")

    # 1. checkpoint
    start_year, start_cmd = load_checkpoint()

    if start_year is None:
        start_year, start_cmd = 1997, 1

    log(f"[CHECKPOINT] year={start_year} cmd={start_cmd}")

    # 2. sync (bez niepełnego roku)
    sync_csv_to_hdfs(start_year)

    # 3. clean niepełnego roku
    local_csv = os.path.join(DATA_DIR, f"{start_year}.csv")
    remove_cmd_from_csv(local_csv, start_cmd)

    hdfs_path = f"{HDFS_DIR}/{start_year}.csv"
    if hdfs_exists(hdfs_path):
        log(f"[HDFS] Removing incomplete {start_year}")
        hdfs_delete(hdfs_path)

    # 4. główna pętla
    for year in range(start_year, 2026):

        cmd_start = start_cmd if year == start_year else 1

        hdfs_path = f"{HDFS_DIR}/{year}.csv"

        if year != start_year and hdfs_exists(hdfs_path):
            log(f"[SKIP] {year} already done")
            continue

        log(f"[YEAR] {year} from cmd={cmd_start}")

        for cmd in range(cmd_start, 98):

            log(f"[INFO] START y={year} cmd={cmd:02d}")

            res = fetch_retry(year, cmd)

            if res is None:
                continue

            if res.status_code == 403:
                log("[STOP] Quota exceeded")
                save_checkpoint(year, cmd)
                return

            data = res.json().get("data", [])

            log(f"[INFO] DONE y={year} cmd={cmd:02d} rec={len(data)}")

            save_csv(data, year)

            save_checkpoint(year, cmd)

            time.sleep(1)

        # upload dopiero po roku
        path = os.path.join(DATA_DIR, f"{year}.csv")

        if os.path.exists(path):
            upload_to_hdfs(path)

        log(f"[SUMMARY] year={year} completed")

    log("[END] Pipeline finished")


if __name__ == "__main__":
    run_pipeline()
