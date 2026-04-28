# 🚀 Reddit Stress Detection Pipeline

Real-time system phát hiện **stress trong bài viết Reddit** sử dụng Kafka, Spark, Airflow và Streamlit.

---

## 📌 Pipeline


Reddit / CSV
↓
Kafka (crawl topic)
↓
Spark Structured Streaming (preprocess + predict)
↓
Kafka (detected topic)
↓
Streamlit Dashboard


---

## ⚙️ Tech Stack

- Apache Kafka (streaming)
- Apache Spark (PySpark, MLlib)
- Apache Airflow (workflow orchestration)
- Streamlit (dashboard visualization)
- Python (pandas, scikit-learn)

---


## ▶️ How to Run (Local - No Docker)

### 1. Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

2. Start Kafka
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
cd ~/kafka
bin/kafka-server-start.sh config/server.properties

3. Train model (Airflow)
airflow standalone

Open: http://localhost:8080

Run: offline_dag

4. Start streaming
python _spark/stream.py

5. Push data
python _kafka/produce.py

6. Dashboard
streamlit run visualize/crawled-data.py
streamlit run visualize/detected-result.py

Open: http://localhost:8501
