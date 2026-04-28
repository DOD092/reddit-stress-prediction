#!/bin/bash

PROJECT_ROOT="/mnt/c/Users/ADMIN/Desktop/project/reddit_prediction"
KAFKA_HOME="$HOME/kafka"

cd "$PROJECT_ROOT"
source venv/bin/activate

echo "===== START ZOOKEEPER ====="
gnome-terminal -- bash -c "cd $KAFKA_HOME && bin/zookeeper-server-start.sh config/zookeeper.properties; exec bash"

sleep 8

echo "===== START KAFKA ====="
gnome-terminal -- bash -c "cd $KAFKA_HOME && bin/kafka-server-start.sh config/server.properties; exec bash"

sleep 10

echo "===== START AIRFLOW ====="
gnome-terminal -- bash -c "cd $PROJECT_ROOT && source venv/bin/activate && airflow standalone; exec bash"

sleep 10

echo "===== START SPARK STREAM ====="
gnome-terminal -- bash -c "cd $PROJECT_ROOT && source venv/bin/activate && python _spark/stream.py; exec bash"

sleep 5

echo "===== START STREAMLIT DETECTED RESULT ====="
gnome-terminal -- bash -c "cd $PROJECT_ROOT && source venv/bin/activate && streamlit run visualize/detected-result.py; exec bash"

echo "===== SYSTEM STARTED ====="
echo "Airflow:   http://localhost:8080"
echo "Streamlit: http://localhost:8501"
echo ""
echo "Sau đó vào Airflow chạy offline_dag trước, rồi online_dag."
echo "Nếu muốn đẩy data thủ công:"
echo "python _kafka/produce.py"
