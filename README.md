# MySQL to BigQuery Sync Pipeline

Automated ETL pipeline for syncing MySQL database tables to Google BigQuery with incremental loading, schema monitoring, and production alerting.

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 🎯 Overview

A data pipeline that syncs MySQL tables to BigQuery, evolving from basic full-table sync to production-ready incremental loading with schema monitoring and multi-channel alerting.

**Current Features (v3):**
- ⚡ **Incremental Loading** - Syncs only changed rows using timestamp-based filtering
- 🔍 **Schema Monitoring** - Detects new columns in MySQL and adds them to BigQuery
- ⚙️ **YAML Configuration** - External config file for easy management
- 🚀 **Production Ready** - Metadata tracking, error handling, logging
- 📢 **Multi-Channel Alerts** - Email and Telegram notifications for sync status

## 📈 Version History

This repository shows the evolution of the pipeline through three versions, each addressing real production needs:

### [v3 - Schema Monitoring](https://github.com/Ayo-G/mysql-to-bigquery-sync/tree/main/version%203) (Current)
**Problem Solved:** Schema changes in MySQL broke the pipeline  
**Solution:** Automatic detection and addition of new columns

- Compares MySQL and BigQuery table schemas before each sync
- Automatically adds new columns to BigQuery when detected
- YAML-based configuration for easier deployment
- Email and Telegram alerting for monitoring
- Enhanced error handling and logging

**Impact:** Zero pipeline failures from column additions over 6+ months

---

### [v2 - Incremental Loading](https://github.com/Ayo-G/mysql-to-bigquery-sync/tree/main/version%202)
**Problem Solved:** Full table syncs were slow and resource-intensive  
**Solution:** Timestamp-based incremental loading

- Only syncs rows modified since last run
- Metadata table tracks last sync timestamp per table
- Merge logic for upsert operations
- Reduced sync time by 90%+

**Impact:** Enabled hourly syncs instead of daily

---

### [v1 - Basic Sync](https://github.com/Ayo-G/mysql-to-bigquery-sync/tree/main/version%201)
**Problem Solved:** Manual data exports were time-consuming  
**Solution:** Automated daily full-table sync

- Established automated MySQL to BigQuery sync
- Replaced manual CSV exports
- Simple daily batch processing

**Impact:** Eliminated 8+ hours of weekly manual work

---

## 🚀 Quick Start

### Current Version (v3)

```bash
# Clone the repository
git clone https://github.com/Ayo-G/mysql-to-bigquery-sync.git
cd mysql-to-bigquery-sync/v3-schema-evolution

# Install dependencies
pip install -r requirements.txt

# Configure (edit with your credentials)
cp config.yaml config.example.yaml
nano config.yaml

# Run the sync
python main.py
```

For detailed setup, see the [v3 README](https://github.com/Ayo-G/mysql-to-bigquery-sync/tree/main/version%203/README.md).

## 🏗️ Architecture (v3)

```
┌─────────────┐
│   MySQL DB  │ 
└──────┬──────┘
       │
       │ 1. Check schema differences
       ▼
┌──────────────────┐
│  Sync Pipeline   │
│  - Compare cols  │
│  - Add new cols  │
│  - Extract Δ     │
└────────┬─────────┘
         │
         │ 2. Load changed rows
         ▼
┌────────────────────┐
│   BigQuery         │
│  ┌──────────────┐  │
│  │  Production  │  │ ◄── Data merged here
│  └──────────────┘  │
│  ┌──────────────┐  │
│  │   Metadata   │  │ ◄── Sync state tracked
│  └──────────────┘  │
└────────┬───────────┘
         │
         │ 3. Send alerts
         ▼
   ┌───────────┐
   │  Alerts   │
   │ Email/Telegram │
   └───────────┘
```

## 💡 Key Features Explained

### Incremental Loading (v2+)
Instead of syncing entire tables, the pipeline:
1. Queries metadata table for last sync timestamp
2. Extracts only rows where `updated_at > last_sync_time`
3. Merges new/changed rows into BigQuery
4. Updates metadata with current timestamp

### Schema Monitoring (v3)
Before each sync, the pipeline:
1. Fetches column list from MySQL table
2. Fetches column list from BigQuery table
3. Identifies columns in MySQL but not in BigQuery
4. Adds missing columns to BigQuery table
5. Proceeds with data sync

**Note:** Currently handles column additions only. Type changes or column removals require manual intervention.

## 📊 Performance

| Metric | v1 | v2 | v3 |
|--------|----|----|-----|
| Sync Method | Full table | Incremental | Incremental |
| Schema Changes | Manual | Manual | Auto (add columns) |
| Data Freshness | 24 hours | <15 minutes | <15 minutes |
| Config Method | Hardcoded | Hardcoded | YAML file |
| Monitoring | Logs only | Logs only | Email + Telegram |

## 🛠️ Tech Stack

- **Language:** Python 3.8+
- **Libraries:** `mysql-connector-python`, `google-cloud-bigquery`, `PyYAML`
- **Source:** MySQL
- **Destination:** Google BigQuery
- **Configuration:** YAML
- **Alerting:** SMTP (email), Telegram Bot API

## 📖 Documentation

- **[v1 - Basic Sync](https://github.com/Ayo-G/mysql-to-bigquery-sync/tree/main/version%201/README.md)** - Initial full-table sync implementation
- **[v2 - Incremental Loading](https://github.com/Ayo-G/mysql-to-bigquery-sync/tree/main/version%202/README.md)** - Timestamp-based change detection
- **[v3 - Schema Monitoring](https://github.com/Ayo-G/mysql-to-bigquery-sync/tree/main/version%203/README.md)** - Current production version

## 🎯 Use Cases

- Syncing operational MySQL databases to BigQuery for analytics
- Keeping data warehouse up-to-date with transactional data
- Enabling real-time dashboards on near-real-time data
- Reducing manual data export processes

## 📝 License

MIT License - see [LICENSE](LICENSE) file for details

## 👤 Author

**Ayomide Gbadegesin**
- GitHub: [@Ayo-G](https://github.com/Ayo-G)
- LinkedIn: [/in/ayo-g](https://linkedin.com/in/ayo-g)
- Portfolio: [rebrand.ly/ayo-g](https://rebrand.ly/ayo-g)

---

Built to solve real data synchronization challenges in production environments.
