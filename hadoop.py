import requests
import os
import datetime
import subprocess


class ACLEDAquisition:
    def __init__(self, email, password):
        self.email = email
        self.password = password
        self.session = requests.Session()
        self.login_url = "https://acleddata.com/user/login?_format=json"
        self.read_url = "https://acleddata.com/api/acled/read?_format=csv"

    def log_event(self, status, size=0, error=""):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp} | Status: {status} | Size: {size} bytes | Error: {error}\n"
        with open("acquisition.log", "a") as f:
            f.write(log_entry)
        print(log_entry.strip())

    def login(self):
        payload = {"name": self.email, "pass": self.password}
        try:
            response = self.session.post(self.login_url, json=payload, timeout=30)
            if response.status_code == 200:
                return True
            self.log_event("FAILED_LOGIN", error=f"Status: {response.status_code}")
            return False
        except Exception as e:
            self.log_event("ERROR_LOGIN", error=str(e))
            return False

    def fetch_and_upload(self, hdfs_path="/user/root/ukraine_data.csv"):
        params = {"country": "Ukraine", "limit": 500}

        try:
            response = self.session.get(self.read_url, params=params, timeout=60)
            if response.status_code != 200:
                self.log_event("FAILED_FETCH", error=f"Status: {response.status_code}")
                return

            response.encoding = 'utf-8-sig'
            raw_data = response.text
            local_file = "/tmp/temp_acled.csv"

            with open(local_file, "w", encoding="utf-8") as f:
                f.write(raw_data)

            data_size = os.path.getsize(local_file)

            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/user/root"], capture_output=True)

            put_process = subprocess.run([
                "hdfs", "dfs",
                "-D", "dfs.replication=3",
                "-put", "-f", local_file, hdfs_path
            ], capture_output=True, text=True)

            if put_process.returncode == 0:
                self.log_event("SUCCESS", size=data_size)
            else:
                self.log_event("FAILED_HDFS_UPLOAD", error=put_process.stderr)

            if os.path.exists(local_file):
                os.remove(local_file)

        except Exception as e:
            self.log_event("CRITICAL_ERROR", error=str(e))

if __name__ == "__main__":
    client = ACLEDAquisition("263507@student.pwr.edu.pl", "hadoop123hadoop123")
    if client.login():
        client.fetch_and_upload()