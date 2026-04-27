#!/bin/bash

echo "=== START PIPELINE ==="

# --- sprawdzenie czy python jest zainstalowany ---
if ! command -v python3 &> /dev/null
then
    echo "[INFO] Installing Python..."
    yum install -y python3
else
    echo "[INFO] Python already installed"
fi

# --- instalacja pip jeśli brak ---
echo "[INFO] Installing Python dependencies..."
python3 -m ensurepip > /dev/null 2>&1
python3 -m pip install --upgrade pip > /dev/null 2>&1

# --- instalacja wymaganych bibliotek ---
python3 -m pip install requests > /dev/null 2>&1

# --- uruchomienie pipeline ---
echo "[INFO] Running pipeline..."
python3 /root/main.py

echo "=== PIPELINE FINISHED ==="
