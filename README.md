# 🚀 Reddit Stress Detection Pipeline

Real-time system phát hiện **stress trong bài viết Reddit** sử dụng Kafka, Spark, Airflow và Streamlit.

---

## 📌 Pipeline

### 🔹 Offline Pipeline (Training)

- Load dataset (CSV)
- Data preprocessing (clean, tokenize)
- Train models (SVM, Logistic Regression, Random Forest, ...)
- Evaluate models (Accuracy, F1-score)
- Select best model
- Save model → `models/Best_model_*`

### 🔹 Online Pipeline (Real-time)

- Crawl data (Reddit / CSV)
- Push data → Kafka (crawl topic)
- Spark Structured Streaming:
  - Read Kafka stream
  - Preprocess text
  - Load Best_model
  - Predict stress
- Push result → Kafka (detected topic)
- Streamlit Dashboard:
  - Crawled Data visualization
  - Detected Result visualization
---

### 🔹 Online Pipeline (Real-time)

- Crawl data (Reddit / CSV)
- Push data → Kafka (crawl topic)
- Spark Structured Streaming:
  - Read Kafka stream
  - Preprocess text
  - Load Best_model
  - Predict stress
- Push result → Kafka (detected topic)
- Streamlit Dashboard:
  - Crawled Data visualization
  - Detected Result visualization

---

## ⚙️ Tech Stack

- Apache Kafka (streaming)
- Apache Spark (PySpark, MLlib)
- Apache Airflow (workflow orchestration)
- Streamlit (dashboard visualization)
- Python (pandas, scikit-learn)

---


## ▶️ How to Run

### 1. Setup
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Start Kafka
```bash
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

### 3. Train model (Airflow)
```bash
airflow standalone
```
Open: http://localhost:8080


Run: offline_dag
Run: online_dag

### 4. Start streaming
```bash
python _spark/stream.py
```

### 5. Push data
```bash
python _kafka/produce.py
```

### 6. Dashboard
```bash
streamlit run visualize/crawled-data.py
streamlit run visualize/detected-result.py
```

Open: http://localhost:8501

Open: http://localhost:8502
