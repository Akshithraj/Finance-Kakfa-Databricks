Finance Kafka Databricks – Real-Time Streaming Analytics & Fraud Detection:
End-to-end streaming data engineering project built on **Databricks**, **Apache Kafka**, **Spark Structured Streaming**, and **Delta Lake** using the **Medallion Architecture (Bronze → Silver → Gold)**.

Overview:
This project simulates a production-grade financial transaction processing system that ingests streaming data from Kafka, processes it through the Medallion Architecture, detects fraudulent and high-value transactions in near real time, and sends automated alerts.

Architecture:
```text
                    +----------------------+
                    |   Transaction Data   |
                    |   Kafka Producer     |
                    +----------+-----------+
                               |
                               v
                      Apache Kafka Topic
                               |
                               v
                  Spark Structured Streaming
                               |
             +-----------------+-----------------+
             |                                   |
             v                                   v
      Bronze Layer                     Fraud Watchlist
   (Raw Delta Tables)                  (CSV / Kafka)
             |                                   |
             +---------------+-------------------+
                             |
                             v
                      Silver Layer
             (Cleaning, Validation, Enrichment)
                             |
                             v
                      Gold Layer
     +--------------------------------------------+
     | Fraud Detection                            |
     | High Value Transactions                    |
     | Transaction Aggregations                   |
     +----------------------+---------------------+
                            |
                            v
                  Email Notifications
                            |
                            v
                 Databricks Dashboard
```

---


 Key Features:

* Real-time transaction ingestion using **Apache Kafka**
* Spark Structured Streaming with checkpointing
* Medallion Architecture (Bronze → Silver → Gold)
* Delta Lake for reliable storage
* Fraud watchlist enrichment
* High-value transaction detection
* Email alert notifications
* Databricks Dashboard for monitoring
* Secret management using Databricks Secret Scopes


Tech Stack:

| Category      | Technologies               |
| ------------- | -------------------------- |
| Language      | Python, PySpark            |
| Streaming     | Apache Kafka               |
| Processing    | Spark Structured Streaming |
| Storage       | Delta Lake                 |
| Platform      | Databricks                 |
| Notifications | SMTP Email                 |
| Dashboard     | Databricks Dashboard       |

Repository Structure:

```text
Finance-Kafka-Databricks/

│
├── finance_streaming/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── alert/
│
├── finance_customers_silver_load/
│
├── fraud_watchlist_file_generator/
│
├── Setup_secret_scope.ipynb
├── Kakfa_streaming_test.ipynb
├── autoloader_test.ipynb
├── send_email.py.ipynb
│
└── Finance Streaming Analytics.lvdash.json
```

---


Pipeline Flow:
Bronze Layer:
* Reads streaming transaction data from Kafka
* Stores raw events in Delta tables
* Preserves original schema

 Silver Layer:
* Parses JSON messages
* Cleans and validates records
* Removes invalid transactions
* Enriches transactions with fraud watchlist

Gold Layer:
Business-ready tables and streaming analytics
* Fraud Card Detection
* High Value Transaction Alerts
* Transaction Count per Minute
* Real-time Aggregations

Alert Layer:
Automatically sends notifications when:
* Fraudulent card detected
* High-value transaction detected

Dashboard:
The Databricks Dashboard monitors
* Streaming throughput
* Fraud alerts
* High-value transactions
* Transactions processed per minute
* Pipeline health

