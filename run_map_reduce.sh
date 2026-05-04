#!/bin/bash

echo "=== START MAVEN BUILD PIPELINE ==="

# 1. Sprawdzenie/Instalacja Javy (Maven wymaga JDK)
if ! command -v java &> /dev/null; then
    echo "[INFO] Installing Java 17..."
    if command -v yum &> /dev/null; then
        sudo yum install -y java-17-openjdk-devel
    else
        sudo apt-get update && sudo apt-get install -y openjdk-17-jdk
    fi
else
    echo "[INFO] Java already installed: $(java -version 2>&1 | head -n 1)"
fi

# 2. Sprawdzenie/Instalacja Mavena
if ! command -v mvn &> /dev/null; then
    echo "[INFO] Maven not found. Installing Maven..."
    if command -v yum &> /dev/null; then
        sudo yum install -y maven
    else
        sudo apt-get install -y maven
    fi
else
    echo "[INFO] Maven already installed: $(mvn -version | head -n 1)"
fi

# 3. Sprawdzenie czy plik pom.xml istnieje
if [ ! -f "pom.xml" ]; then
    echo "[ERROR] No pom.xml found in $(pwd)! Cannot build project."
    exit 1
fi

# 4. Czyszczenie i kompilacja projektu
echo "[INFO] Building JAR file using Maven..."
mvn clean package -DskipTests

# 5. Sprawdzenie czy budowa się udała
if [ $? -eq 0 ]; then
    echo "[SUCCESS] Build finished successfully."

    # Wyświetlenie lokalizacji gotowego JAR-a
    JAR_PATH=$(find target -name "*.jar" | head -n 1)
    echo "[INFO] Your JAR is ready at: $JAR_PATH"
else
    echo "[ERROR] Maven build failed!"
    exit 1
fi

echo "=== PIPELINE FINISHED ==="