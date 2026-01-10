# v1 - Basic Sync

Initial implementation of automated MySQL to BigQuery synchronization.

## 📁 Files
- `main.py`
- `requirements.txt`

## 🎯 What This Version Does

Automates daily full-table synchronization from MySQL to BigQuery, replacing manual CSV exports.

### Core Functionality:
1. **Connects to MySQL** - Uses `mysql.connector` with environment variables for credentials
2. **Reads entire tables** - `SELECT * FROM table` for each configured table
3. **Writes to BigQuery** - Uses `WRITE_TRUNCATE` to replace existing data
4. **Logs results** - Basic print statements for monitoring

## 🔧 Implementation Details

### Tables Synced:
The code syncs a hardcoded list of tables:
```python
TABLES = ['users', 'orders', 'products', 'transactions']
```

### Sync Process:
```python
1. For each table:
   - Execute: SELECT * FROM table
   - Load into pandas DataFrame
   - Write to BigQuery (replace mode)
```

### Configuration:
All configuration is via environment variables:
- `MYSQL_HOST`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE`
- `BIGQUERY_PROJECT`
- `BIGQUERY_DATASET`

## 📊 Characteristics

- **Sync Strategy:** Full table replacement
- **Frequency:** Daily batch
- **Database Impact:** Full table scans on every run
- **BigQuery Mode:** `WRITE_TRUNCATE` (replaces all data)
- **Best For:** Small tables (<1M rows)

## ⚠️ Limitations

1. **No Incremental Loading** - Always syncs entire table, even for single row changes
2. **Hardcoded Configuration** - Table list and settings embedded in code
3. **No State Tracking** - Can't resume from failures
4. **High Resource Usage** - Full scans impact database performance
5. **No Schema Management** - Schema changes require manual BigQuery table updates
6. **Basic Error Handling** - Failures logged but sync continues to next table

## 🔄 Why v2 Was Needed

As data volumes grew:
- 10M+ row tables took hours to sync
- Full table scans caused database performance degradation
- Daily syncs weren't frequent enough for business needs
- Need for hourly updates became critical

**v2 addressed these issues with incremental loading.**

## 🚀 Running v1

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export MYSQL_HOST="your-host"
export MYSQL_USER="your-user"
export MYSQL_PASSWORD="your-password"
export MYSQL_DATABASE="your-database"
export BIGQUERY_PROJECT="your-project"
export BIGQUERY_DATASET="your-dataset"

# Run sync
python main.py
```

## 📖 What Was Learned

Building v1 established:
- Automated sync is possible and valuable
- Full table sync doesn't scale with data growth
- Need to track which data actually changed
- Configuration should be external to code

These insights led to v2's incremental approach.

---

**Next:** See [version 2](../version%202/README.md)
