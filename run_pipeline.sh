#!/bin/bash

echo "=== START PIPELINE ==="

# --- sprawdzenie czy python jest zainstalowany ---
if ! command -v pip &> /dev/null
then
    curl https://bootstrap.pypa.io/pip/3.9/get-pip.py -o get-pip.py
    python3.9 get-pip.py
else
    echo "[INFO] Pip already installed"
fi

# --- instalacja pip jeśli brak ---
echo "[INFO] Installing Python dependencies..."
python3.9 -m pip install --upgrade pip > /dev/null 2>&1

# --- instalacja wymaganych bibliotek ---
python3.9 -m pip install -e .

# --- uruchomienie pipeline ---
echo "[INFO] Running pipeline..."
python3.9 ./acled/hadoop.py
python3.9 ./unhcr/main.py
python3.9 ./uncomtrade/main.py

echo "=== PIPELINE FINISHED ==="
