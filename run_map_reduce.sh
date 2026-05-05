#!/bin/bash

echo "=== START MAP REDUCE PIPELINE ==="

# instalacja mavena
yum install -y maven

# kompilacja
cd java
mvn clean package

# uruchomienie

# acled
hadoop jar target/hadoop-1.0-SNAPSHOT.jar acled.Acled /acled_data.csv /output/acled
#hadoop jar target/hadoop-1.0-SNAPSHOT.jar unhcr.mapreduce.Main
# final

# todo usunięcie tymczasowych plików z hdfs

echo "=== PIPELINE FINISHED ==="