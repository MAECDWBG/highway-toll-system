# 🛣️ Highway Toll Collection System

**Student:** Mayukh Ghosh | **Roll No:** 23052334 | **Batch:** Data Engineering 2025-2026 | **University:** KIIT University

---

## 📌 Problem Statement

Manual toll collection causes revenue leakage, long queues, and zero real-time analytics. This project builds a fully automated, cloud-native data pipeline that ingests FASTag RFID events, processes them in real time, stores them in a Redshift data warehouse, and exposes live KPI dashboards.

---

## 🏗️ Architecture

```
RFID Readers
     │
     ▼
Kafka (toll-transactions topic)
     │
     ▼
PySpark Structured Streaming ──► S3 (Raw Parquet)
     │                                │
     │                           Glue Catalog
     │                                │
     ▼                                ▼
Redis Cache (blacklist/balance)   Redshift Spectrum
                                       │
                          Airflow DAGs (nightly batch)
                                       │
                                  Redshift Mart
                                       │
                                   Grafana Dashboard
```

---

## 📁 Project Structure

```
highway-toll-system/
├── kafka/
│   ├── producer.py
│   └── consumer_test.py
├── spark/
│   ├── streaming_job.py
│   └── batch_transform.py
├── airflow/dags/
│   └── toll_etl_dag.py
├── sql/
│   ├── schema.sql
│   └── queries.sql
├── great_expectations/
│   └── checkpoint.py
├── api/
│   └── app.py
├── terraform/
│   └── main.tf
├── docker/
│   └── docker-compose.yml
├── scripts/
│   └── setup.sh
├── tests/
│   ├── test_producer.py
│   └── test_transforms.py
├── requirements.txt
└── README.md
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Apache Kafka 3.6 |
| Streaming | PySpark 3.5 Structured Streaming |
| Orchestration | Apache Airflow 2.8 |
| Raw Storage | Amazon S3 (Parquet) |
| Data Warehouse | Amazon Redshift |
| Catalog | AWS Glue Data Catalog |
| Cache | Amazon ElastiCache (Redis) |
| Data Quality | Great Expectations 0.18 |
| Visualization | Grafana 10 |
| API | FastAPI |
| IaC | Terraform 1.6 |
| Language | Python 3.11 |
| Container | Docker / Docker Compose |

---

## 🚀 Quick Start (Local)

### Prerequisites
- Docker & Docker Compose installed
- Python 3.11+
- Java 11+ (for Spark)

### 1. Clone the repo
```bash
git clone https://github.com/<your-username>/highway-toll-system.git
cd highway-toll-system
```

### 2. Start local services
```bash
docker-compose -f docker/docker-compose.yml up -d
```

### 3. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the Kafka producer (simulates toll events)
```bash
python kafka/producer.py
```

### 5. Run the Spark Streaming job
```bash
spark-submit spark/streaming_job.py
```

### 6. Access Airflow UI
```
http://localhost:8080  (admin / admin)
```

### 7. Access Grafana
```
http://localhost:3000  (admin / admin)
```

---

## 📊 Star Schema

```
FACT_TRANSACTION (transaction_id, plaza_id, vehicle_id, date_id, class_id, payment_id, amount, entry_time, exit_time)
DIM_VEHICLE      (vehicle_id, tag_id, owner, registered_state)
DIM_PLAZA        (plaza_id, name, highway, location, operator)
DIM_DATE         (date_id, date, hour, day_of_week, is_weekend)
DIM_CLASS        (class_id, name, axles, base_rate)
DIM_PAYMENT      (payment_id, method, status)
```

---

## 👤 Author

**Mayukh Ghosh** — Roll No: 23052334
Data Engineering Batch 2025-2026, KIIT University
