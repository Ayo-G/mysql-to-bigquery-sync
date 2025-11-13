## **MySQL to BigQuery Sync Pipeline**

### **Overview**

This repository contains a **production-ready data pipeline** that synchronises data incrementally from **MySQL** to **BigQuery** via an **SSH tunnel**.
It is designed for scalability, reliability, and maintainability, supporting **multiple tables**, **automatic schema updates**, **error handling**, and **email alerts** — all configurable from a single YAML file.

---

### **Key Features**

* Multi-table support via `config/tables.yml`
* Automatic schema detection & column creation in BigQuery
* Centralised metadata tracking for Looker Studio dashboards
* Comprehensive error handling & HTML email alerts
* Modular, extensible codebase ready for production environments
* Compatible with Google Cloud Functions or manual execution

---

### **Architecture**

The pipeline performs the following high-level steps:

1. Establishes an **SSH tunnel** to the MySQL database
2. Extracts incremental records based on a timestamp column (`updated_at` or custom)
3. Loads the extracted data into a **BigQuery staging table**
4. Merges new or updated records into the target BigQuery table
5. Automatically detects and adds new columns (schema evolution handling)
6. Updates a **metadata table** to log:

   * Last sync time
   * Run status
   * Row count
   * Column count
   * Remarks (errors or schema updates)
7. Sends an **HTML-formatted email** summarizing any pipeline errors

---

### **Repository Structure**

```
mysql-to-bigquery-sync/
│
├── main.py                     # Main pipeline controller script
├── requirements.txt            # All Python dependencies
│
├── config/
│   └── tables.yml              # Config file defining tables and connection settings
│
├── credentials/
│   ├── key.json                # BigQuery service account key
│   └── key.ppk                 # SSH private key for MySQL access
│
└── README.md                   # Project documentation
```

---

### **Configuration**

All settings are centralised in `config/tables.yml`.
Click [here](config/tables.yml) to view

---

### **Setup Instructions**

#### **1. Clone the repository**

```bash
git clone https://github.com/ayo-g/mysql-to-bigquery-sync.git
cd mysql-to-bigquery-sync
```

#### **2. Install dependencies**

```bash
pip install -r requirements.txt
```

#### **3. Configure environment**

* Place your **BigQuery service account key** under `credentials/key.json`
* Place your **SSH private key** under `credentials/key.ppk`
* Update the configuration file: `config/tables.yml`

#### **4. Run locally**

```bash
python main.py
```

#### **5. (Optional) Deploy to Cloud Functions**

Rename the handler to `main` if required and deploy with:

```bash
gcloud functions deploy mysql_to_bigquery_sync \
  --runtime python311 \
  --trigger-http \
  --entry-point handler \
  --timeout 540s \
  --memory 1024MB
```

---

### **Monitoring and Looker Studio Dashboard**

The pipeline automatically updates a **metadata table** in BigQuery.
You can connect this table directly to **Looker Studio** to visualise:

* Last Run Time
* Last Sync Time
* Row Count
* Column Count
* Sync Status
* Remarks

This allows for near real-time visibility into sync health across all tables.

---

### **Email Alerts**

At the end of each pipeline run:

* All encountered errors are compiled into an HTML email.
* A single structured alert is sent to all addresses in the `alert_recipients` list.

Email structure includes:

| Table | Step | Remark | Timestamp |
| ----- | ---- | ------ | --------- |
| testtable1 | 3 | test remark | 2025-11-11 12:30:00 |
| testtable2 | 4 | another test remark | 2025-11-15 12:55:14 |

---

### **Scalability & Extensibility**

* Add new tables easily via `config/tables.yml`
* Add new database sources by extending the MySQL extraction function
* Integrate scheduling with Cloud Scheduler or Apache Airflow
* Extend alerting to Slack, PagerDuty, or other monitoring tools

