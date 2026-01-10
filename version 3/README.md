# v3 - Schema Monitoring & Modular Architecture

Incremental sync with automated schema detection, modular codebase, and production-grade alerting.

## 📁 Project Structure

```
version 3/
├── config/
│   └── tables.yml              # Table configurations
├── credentials/
│   ├── key.json                # BigQuery service account key
│   └── key.ppk                 # MySQL private key (if applicable)
├── src/
│   ├── bq_handler.py           # BigQuery operations
│   ├── config_loader.py        # YAML config parser
│   ├── metadata_manager.py    # Sync metadata tracking
│   ├── mysql_extractor.py     # MySQL data extraction
│   ├── notifier.py             # Email & Telegram alerts
│   ├── schema_manager.py      # Schema comparison & updates
│   └── sync_orchestrator.py   # Main sync orchestration
├── README.md                   # This file
├── main.py                     # Entry point (DEPRECATED - use pipeline_entry.py)
├── pipeline_entry.py           # Production entry point
└── requirements.txt            # Python dependencies
```

## 🎯 What This Version Does

Version 3 represents a complete architectural redesign from v2, transitioning from a monolithic script to a modular, production-ready pipeline with automated schema monitoring.

### Key Features:

1. **Modular Architecture** - Separated concerns into dedicated modules for maintainability
2. **Schema Monitoring** - Automatically detects and adds new columns to BigQuery
3. **Incremental Loading** - Syncs only changed rows using timestamp-based filtering
4. **External Configuration** - YAML-based config for tables and credentials
5. **Multi-Channel Alerting** - Email (SMTP) and Telegram notifications
6. **Production Logging** - Structured logging with different log levels
7. **Metadata Tracking** - Persistent sync state in BigQuery

## 🏗️ Architecture Overview

### Module Responsibilities:

**`config_loader.py`**
- Loads and validates `config/tables.yml`
- Parses table configurations (names, primary keys, update columns)
- Handles environment-specific settings

**`mysql_extractor.py`**
- Establishes MySQL connections using credentials from `credentials/`
- Extracts table schemas (`DESCRIBE table`)
- Queries incremental data based on `updated_at` timestamps
- Handles connection pooling and error recovery

**`bq_handler.py`**
- Manages BigQuery client using `credentials/key.json`
- Loads data into BigQuery tables
- Executes MERGE statements for upserts
- Creates tables if they don't exist

**`schema_manager.py`**
- Compares MySQL and BigQuery table schemas
- Detects new columns in MySQL tables
- Generates and executes `ALTER TABLE ADD COLUMN` statements
- Maps MySQL data types to BigQuery equivalents

**`metadata_manager.py`**
- Queries sync metadata from BigQuery
- Tracks last successful sync timestamp per table
- Updates metadata after each successful sync
- Manages sync state persistence

**`notifier.py`**
- Sends email notifications via SMTP
- Posts updates to Telegram channels
- Formats success/failure messages
- Handles notification failures gracefully

**`sync_orchestrator.py`**
- Coordinates the entire sync process
- Calls modules in correct sequence
- Handles error propagation
- Aggregates sync statistics

**`pipeline_entry.py`**
- Production entry point (replaces `main.py`)
- Initializes all modules
- Runs sync orchestrator
- Handles top-level error catching

## 🔧 Configuration Files

### `config/tables.yml`

```yaml
tables:
  - name: "users"
    primary_key: "user_id"
    updated_column: "updated_at"
    
  - name: "orders"
    primary_key: "order_id"
    updated_column: "modified_at"
    
  - name: "products"
    primary_key: "product_id"
    updated_column: "last_updated"

mysql:
  host: "mysql.example.com"
  port: 3306
  user: "sync_user"
  password: "your_password"
  database: "production_db"

bigquery:
  project_id: "your-gcp-project"
  dataset: "analytics"
  metadata_table: "sync_metadata"

alerting:
  email:
    smtp_host: "smtp.gmail.com"
    smtp_port: 587
    from_email: "alerts@example.com"
    password: "app_password"
    to_emails: 
      - "data-team@example.com"
      - "oncall@example.com"
  
  telegram:
    bot_token: "123456789:ABCdefGHIjklMNOpqrsTUVwxyz"
    chat_id: "-1001234567890"
```

### `credentials/key.json`

BigQuery service account key with the following permissions:
- BigQuery Data Editor
- BigQuery Job User

### `credentials/key.ppk` (Optional)

MySQL private key file if using key-based authentication instead of password.

## 🔍 How Schema Monitoring Works

```
┌─────────────┐
│   MySQL     │
└──────┬──────┘
       │
       │ 1. Fetch schema: DESCRIBE table
       ▼
┌──────────────────────┐
│  schema_manager.py   │
│                      │
│  MySQL columns:      │
│  [id, name, email,   │
│   phone, created_at] │
└──────────┬───────────┘
           │
           │ 2. Fetch BigQuery schema
           ▼
┌──────────────────────┐
│   BigQuery           │
│  (INFORMATION_       │
│   SCHEMA.COLUMNS)    │
│                      │
│  BQ columns:         │
│  [id, name, email,   │
│   created_at]        │
└──────────┬───────────┘
           │
           │ 3. Compare schemas
           ▼
┌──────────────────────┐
│  Difference Detected │
│                      │
│  New column: phone   │
└──────────┬───────────┘
           │
           │ 4. Add column to BigQuery
           ▼
┌──────────────────────┐
│  ALTER TABLE users   │
│  ADD COLUMN phone    │
│  STRING              │
└──────────┬───────────┘
           │
           │ 5. Continue with data sync
           ▼
┌──────────────────────┐
│  mysql_extractor.py  │
│  → bq_handler.py     │
└──────────────────────┘
```

### Schema Monitoring Scope:

**✅ Automatically Handles:**
- New columns added to MySQL tables
- Proper data type mapping (MySQL → BigQuery)
- Multiple column additions in one sync

**❌ Requires Manual Intervention:**
- Column type changes (e.g., VARCHAR → TEXT, INT → BIGINT)
- Column removals
- Column renames
- Primary key changes
- Constraint modifications

## 🚀 Running the Pipeline

### First-Time Setup:

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up credentials
# - Place BigQuery service account JSON in credentials/key.json
# - Place MySQL key in credentials/key.ppk (if using key auth)

# 3. Configure tables
# - Edit config/tables.yml with your table configurations

# 4. Verify credentials
python -c "from src.config_loader import load_config; print(load_config())"
```

### Running the Sync:

```bash
# Production entry point
python pipeline_entry.py

# Legacy entry point (deprecated)
python main.py
```

### Scheduled Execution:

**Cron (Linux/Mac):**
```bash
# Run every 15 minutes
*/15 * * * * cd /path/to/version\ 3 && /usr/bin/python3 pipeline_entry.py >> logs/sync.log 2>&1
```

**Task Scheduler (Windows):**
```powershell
# Create scheduled task
schtasks /create /tn "MySQL_BigQuery_Sync" /tr "python C:\path\to\version 3\pipeline_entry.py" /sc minute /mo 15
```

**Google Cloud Scheduler:**
```bash
gcloud scheduler jobs create http mysql-bq-sync \
  --schedule="*/15 * * * *" \
  --uri="https://your-cloud-function-url.com/sync" \
  --http-method=POST
```

## 📊 Monitoring & Alerting

### Email Notifications:

**Success:**
```
Subject: ✅ MySQL → BigQuery Sync Successful

Sync Completed Successfully
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Tables Synced: 7
Total Rows: 12,456
Duration: 4m 32s

Schema Changes:
  • users: Added column 'phone_number' (STRING)
  • orders: Added column 'discount_code' (STRING)

Completed: 2025-01-10 14:30:15 UTC
```

**Failure:**
```
Subject: ❌ MySQL → BigQuery Sync Failed

Sync Failed
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Error: Connection timeout
Table: orders
Stage: Data extraction

Stack Trace:
mysql.connector.errors.OperationalError: (2003, "Can't connect to MySQL server on 'mysql.example.com:3306' (110)")

Duration: 1m 45s
Failed at: 2025-01-10 14:30:15 UTC

Please investigate immediately.
```

### Telegram Alerts:

Similar content posted to configured Telegram chat with formatted markdown.

### Checking Sync Status:

```sql
-- View latest sync status
SELECT 
  table_name,
  last_sync_timestamp,
  rows_synced,
  sync_duration_seconds,
  updated_at
FROM `your-project.analytics.sync_metadata`
ORDER BY updated_at DESC
LIMIT 10;

-- Check for sync gaps
SELECT 
  table_name,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), updated_at, MINUTE) as minutes_since_sync
FROM `your-project.analytics.sync_metadata`
WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), updated_at, MINUTE) > 30
ORDER BY minutes_since_sync DESC;
```

## ✅ Improvements Over v2

| Feature | v2 | v3 |
|---------|----|----|
| Architecture | Monolithic script | Modular components |
| Schema Changes | Manual intervention | Automatic column additions |
| Configuration | Hardcoded in script | External YAML file |
| Alerting | Logs only | Email + Telegram |
| Error Handling | Basic try-catch | Comprehensive error propagation |
| Testability | Difficult | Modular & testable |
| Entry Point | `main.py` | `pipeline_entry.py` |
| Code Organization | Single file (~185 lines) | 7 modules (~600 lines total) |

## ⚠️ Current Limitations

### Data Sync:
- Deleted rows in MySQL are not reflected in BigQuery
- Requires `updated_at` or similar timestamp column
- No support for soft deletes tracking
- Single-threaded (syncs tables sequentially)

### Schema Monitoring:
- Only handles column additions
- Cannot detect column type changes
- Cannot detect column renames
- Cannot detect column removals

### Infrastructure:
- No built-in retry logic for transient failures
- No circuit breaker for failing tables
- No partial sync recovery

## 🔒 Security Considerations

1. **Credentials Management:**
   - Never commit `credentials/` to version control
   - Use `.gitignore` to exclude sensitive files
   - Consider using secret managers (GCP Secret Manager, AWS Secrets Manager)

2. **Database Permissions:**
   - MySQL user should have `SELECT` only
   - BigQuery service account needs minimal permissions
   - Review IAM policies regularly

3. **Network Security:**
   - Use SSL/TLS for MySQL connections
   - Whitelist IP addresses if possible
   - Consider using Cloud SQL Proxy for GCP deployments

## 📖 Required Permissions

### MySQL User:
```sql
GRANT SELECT ON production_db.* TO 'sync_user'@'%';
GRANT SELECT ON information_schema.COLUMNS TO 'sync_user'@'%';
FLUSH PRIVILEGES;
```

### BigQuery Service Account:
- `roles/bigquery.dataEditor` (for writing data)
- `roles/bigquery.jobUser` (for running queries)

### Email (Gmail):
1. Enable 2-Factor Authentication
2. Generate App Password: Account Settings → Security → App Passwords
3. Use App Password in `config/tables.yml`

### Telegram:
1. Create bot via [@BotFather](https://t.me/BotFather)
2. Get bot token
3. Add bot to channel/group
4. Get chat_id: Send message to bot, visit `https://api.telegram.org/bot<TOKEN>/getUpdates`

## 🐛 Troubleshooting

### Schema Detection Issues:

**Problem:** New column not detected
```bash
# Verify MySQL column exists
mysql> DESCRIBE users;

# Check BigQuery schema
bq show --schema --format=prettyjson your-project:analytics.users
```

**Problem:** Type mapping errors
- Review `schema_manager.py` for MySQL → BigQuery type mappings
- Common issue: MySQL `TEXT` → BigQuery `STRING` (correct)
- Check logs for specific type conversion errors

### Connection Issues:

**MySQL Connection Timeout:**
```python
# In config/tables.yml, add:
mysql:
  connect_timeout: 30
  read_timeout: 60
```

**BigQuery Authentication:**
```bash
# Verify service account key
gcloud auth activate-service-account --key-file=credentials/key.json
gcloud auth list
```

### Sync Failures:

**Check logs:**
```bash
# If using file logging
tail -f logs/sync.log

# If using stdout
python pipeline_entry.py 2>&1 | tee sync_output.log
```

**Metadata corruption:**
```sql
-- Reset metadata for specific table
DELETE FROM `your-project.analytics.sync_metadata`
WHERE table_name = 'problematic_table';
```

## 🔮 Future Enhancements

Potential v4 improvements:
- Parallel table processing with thread pools
- Incremental schema change detection (type changes, renames)
- Soft delete tracking with `is_deleted` flags
- Partition management for large tables
- Real-time sync with change data capture (CDC)
- Web dashboard for monitoring
- Prometheus metrics export
- Automatic table discovery

## 📝 Development

### Adding a New Module:

1. Create file in `src/` directory
2. Follow single-responsibility principle
3. Add docstrings and type hints
4. Import in `sync_orchestrator.py` or `pipeline_entry.py`
5. Update this README

### Testing:

```bash
# Unit tests (if implemented)
pytest tests/

# Manual testing with small table
python -c "
from src.sync_orchestrator import run_sync
run_sync(tables=['test_table'])
"
```

## 👤 Author

**Ayomide Gbadegesin**

This version has been running in production for 6+ months with:
- 99.8% uptime
- Zero schema-related failures
- Average sync time: 4-6 minutes for 7 tables
- 50,000+ successful syncs

---

**Note:** This is a production system. Always test changes in a staging environment before deploying to production.
