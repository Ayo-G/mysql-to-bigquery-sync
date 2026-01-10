# MySQL to BigQuery Sync Pipeline

Automated ETL pipeline for syncing MySQL database tables to Google BigQuery with incremental loading, schema monitoring, and production alerting.

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 🎯 Overview

A data pipeline that syncs MySQL tables to BigQuery, evolving from basic full-table sync to production-ready incremental loading with automated schema monitoring and multi-channel alerting.

**Current Features (v3):**

- ⚡ **Incremental Loading** - Syncs only changed rows using timestamp-based filtering
- 🔍 **Schema Monitoring** - Detects new columns in MySQL and adds them to BigQuery
- 🏗️ **Modular Architecture** - Separated concerns for maintainability and testing
- ⚙️ **YAML Configuration** - External config file for easy management
- 🚀 **Production Ready** - Metadata tracking, error handling, structured logging
- 📢 **Multi-Channel Alerts** - Email and Telegram notifications for sync status

## 📈 Version History

This repository demonstrates the evolution of the pipeline through three versions, each addressing real production needs:

### [v3 - Schema Monitoring & Modular Architecture](./version%203) (Current)

**Problems Solved:** 
- Schema changes in MySQL broke the pipeline
- Monolithic code was difficult to maintain and test

**Solutions:** 
- Automatic detection and addition of new columns
- Modular architecture with 7 specialized components
- Production-grade configuration and alerting

**Key Files:**
```
version 3/
├── config/
│   └── tables.yml                  # Configuration
├── credentials/
│   ├── key.json                    # BigQuery credentials
│   └── key.ppk                     # MySQL credentials (optional)
├── src/
│   ├── bq_handler.py              # BigQuery operations
│   ├── config_loader.py           # YAML config parser
│   ├── metadata_manager.py        # Sync metadata tracking
│   ├── mysql_extractor.py         # MySQL data extraction
│   ├── notifier.py                # Email & Telegram alerts
│   ├── schema_manager.py          # Schema comparison & updates
│   └── sync_orchestrator.py       # Main sync orchestration
├── pipeline_entry.py              # Production entry point
└── requirements.txt               # Dependencies
```

**Impact:** Zero pipeline failures from schema changes + improved maintainability with modular design

-----

### [v2 - Incremental Loading](./v2-incremental-sync)

**Problem Solved:** Full table syncs were slow and resource-intensive  
**Solution:** Timestamp-based incremental loading

- Only syncs rows modified since last run
- Metadata table tracks last sync timestamp per table
- Merge logic for upsert operations
- Reduced sync time by 90%+

**Impact:** Enabled 15-minute sync intervals instead of daily

-----

### [v1 - Basic Sync](./v1-basic-sync)

**Problem Solved:** Manual data exports were time-consuming  
**Solution:** Automated daily full-table sync

- Established automated MySQL to BigQuery sync
- Replaced manual CSV exports
- Simple daily batch processing

**Impact:** Eliminated 8+ hours of weekly manual work

-----

## 🚀 Quick Start

### Current Version (v3)

```bash
# Clone the repository
git clone https://github.com/Ayo-G/mysql-to-bigquery-sync.git
cd mysql-to-bigquery-sync/version\ 3

# Install dependencies
pip install -r requirements.txt

# Set up credentials
# 1. Place BigQuery service account JSON in credentials/key.json
# 2. Place MySQL credentials in credentials/key.ppk (if using key auth)

# Configure tables
# Edit config/tables.yml with your table configurations
nano config/tables.yml

# Run the sync
python pipeline_entry.py
```

For detailed setup and configuration, see the [v3 README](./version%203/README.md).

## 🏗️ Architecture (v3)

```
┌─────────────┐
│   MySQL DB  │ 
└──────┬──────┘
       │
       │ 1. Extract schema & check for changes
       ▼
┌──────────────────────────┐
│  Sync Pipeline           │
│  ┌────────────────────┐  │
│  │ config_loader.py   │  │ ◄─ Loads config/tables.yml
│  └────────────────────┘  │
│  ┌────────────────────┐  │
│  │ schema_manager.py  │  │ ◄─ Compares & updates schemas
│  └────────────────────┘  │
│  ┌────────────────────┐  │
│  │ mysql_extractor.py │  │ ◄─ Extracts changed rows
│  └────────────────────┘  │
│  ┌────────────────────┐  │
│  │ bq_handler.py      │  │ ◄─ Loads to BigQuery
│  └────────────────────┘  │
│  ┌────────────────────┐  │
│  │ metadata_manager   │  │ ◄─ Tracks sync state
│  └────────────────────┘  │
│  ┌────────────────────┐  │
│  │ notifier.py        │  │ ◄─ Sends alerts
│  └────────────────────┘  │
│  ┌────────────────────┐  │
│  │sync_orchestrator   │  │ ◄─ Coordinates all modules
│  └────────────────────┘  │
└────────┬─────────────────┘
         │
         │ 2. Merge data (UPSERT)
         ▼
┌────────────────────┐
│   BigQuery         │
│  ┌──────────────┐  │
│  │  Production  │  │ ◄── Data merged here
│  │    Tables    │  │
│  └──────────────┘  │
│  ┌──────────────┐  │
│  │   Metadata   │  │ ◄── Sync state tracked
│  │    Table     │  │
│  └──────────────┘  │
└────────┬───────────┘
         │
         │ 3. Send notifications
         ▼
   ┌───────────┐
   │  Alerts   │
   │  Email +  │
   │ Telegram  │
   └───────────┘
```

## 💡 Key Features Explained

### Incremental Loading (v2+)

Instead of syncing entire tables, the pipeline:

1. Queries metadata table for last sync timestamp
2. Extracts only rows where `updated_at > last_sync_time`
3. Merges new/changed rows into BigQuery using MERGE statements
4. Updates metadata with current timestamp

### Schema Monitoring (v3)

Before each sync, the pipeline:

1. Fetches column list from MySQL table (via `DESCRIBE`)
2. Fetches column list from BigQuery table (via `INFORMATION_SCHEMA.COLUMNS`)
3. Identifies columns in MySQL but not in BigQuery
4. Adds missing columns to BigQuery table (via `ALTER TABLE ADD COLUMN`)
5. Proceeds with data sync

**Note:** Currently handles column additions only. Type changes or column removals require manual intervention.

### Modular Architecture (v3)

The codebase is organized into specialized modules:

- **config_loader.py** - Loads and validates YAML configuration
- **mysql_extractor.py** - Handles all MySQL interactions
- **bq_handler.py** - Manages BigQuery operations
- **schema_manager.py** - Detects and applies schema changes
- **metadata_manager.py** - Tracks sync state
- **notifier.py** - Sends email and Telegram alerts
- **sync_orchestrator.py** - Coordinates the entire process

Benefits: easier testing, better maintainability, clearer separation of concerns

## 📊 Performance

| Metric | v1 | v2 | v3 |
|--------|----|----|-----|
| Sync Method | Full table | Incremental | Incremental |
| Schema Changes | Manual | Manual | Auto (add columns) |
| Architecture | Monolithic | Monolithic | Modular |
| Data Freshness | 24 hours | <15 minutes | <15 minutes |
| Configuration | Hardcoded | Hardcoded | YAML file |
| Monitoring | Logs only | Logs only | Email + Telegram |
| Code Organization | Single file | Single file | 7 modules |
| Entry Point | `main.py` | `main.py` | `pipeline_entry.py` |

## 🛠️ Tech Stack

- **Language:** Python 3.8+
- **Core Libraries:** 
  - `mysql-connector-python` - MySQL connectivity
  - `google-cloud-bigquery` - BigQuery API client
  - `PyYAML` - Configuration parsing
- **Source:** MySQL
- **Destination:** Google BigQuery
- **Configuration:** YAML
- **Alerting:** SMTP (email), Telegram Bot API
- **Credentials:** JSON (BigQuery), PPK/password (MySQL)

## 📖 Documentation

- **[v1 - Basic Sync](./version%201/README.md)** - Initial full-table sync implementation
- **[v2 - Incremental Loading](./version%202/README.md)** - Timestamp-based change detection
- **[v3 - Schema Monitoring](./version%203/README.md)** - Current production version with modular architecture

## 🎯 Use Cases

- Syncing operational MySQL databases to BigQuery for analytics
- Keeping data warehouse up-to-date with transactional data
- Enabling real-time dashboards on near-real-time data
- Reducing manual data export/import processes
- Building event-driven data pipelines

## 🔒 Security Best Practices

1. **Never commit credentials** - Use `.gitignore` for `credentials/` and `config/tables.yml`
2. **Use service accounts** - Minimal permissions for BigQuery and MySQL users
3. **Enable SSL/TLS** - For MySQL connections in production
4. **Rotate credentials** - Regularly update passwords and keys
5. **Monitor access logs** - Track who/what accesses your data

## 📝 License

MIT License - see [LICENSE](LICENSE) file for details

## 👤 Author

**Ayomide Gbadegesin**

- GitHub: [@Ayo-G](https://github.com/Ayo-G)
- LinkedIn: [/in/ayo-g](https://linkedin.com/in/ayo-g)
- Portfolio: [rebrand.ly/ayo-g](https://rebrand.ly/ayo-g)

## 🏆 Production Stats (v3)

- **Uptime:** 99.8% over 6+ months
- **Total Syncs:** 50,000+
- **Average Sync Time:** 4-6 minutes for 7 tables
- **Schema Failures:** 0 (since v3 deployment)
- **Data Volume:** 10M+ rows synced monthly

-----

Built to solve real data synchronization challenges in production environments. This project showcases the evolution of a data pipeline from a simple automation script to a robust, production-grade system.
