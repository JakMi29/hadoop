#!/bin/bash

echo "=== START PIPELINE ==="

# --- sprawdzenie czy python jest zainstalowany ---
if ! command -v python &> /dev/null
then
    echo "[INFO] Installing Python..."
    yum install -y python pip
else
    echo "[INFO] Python already installed"
fi

# --- instalacja pip jeśli brak ---
echo "[INFO] Installing Python dependencies..."
python -m pip install --upgrade pip > /dev/null 2>&1

# --- instalacja wymaganych bibliotek ---
python -m pip install -e .

# --- uruchomienie pipeline ---
echo "[INFO] Running pipeline..."
python ./acled/hadoop.py
python ./unhcr/main.py
python ./uncomtrade/main.py

echo "=== PIPELINE FINISHED ==="
