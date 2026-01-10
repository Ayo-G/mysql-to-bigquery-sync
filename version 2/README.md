# v2 - Incremental Sync

Timestamp-based incremental loading for efficient data synchronization.

## 📁 Files
- `main.py`
- `requirements.txt`
- `table.yml`

## 🎯 What This Version Does

Syncs only rows that changed since the last successful run, dramatically improving performance and reducing database load.

### Core Improvements Over v1:
1. **Incremental Loading** - Queries only modified rows using timestamp filtering
2. **Metadata Tracking** - BigQuery table stores last sync timestamp per table
3. **Merge Logic** - Upserts data instead of full replacement
4. **Automatic Resume** - Can continue from last successful sync point
5. **Seperate config file** - Credentials are now stored in a config file instead of hard-coding

## 🔧 Implementation Details

### Metadata Table:
Creates and maintains a BigQuery metadata table:
```sql
CREATE TABLE IF NOT EXISTS metadata_table (
  table_name STRING,
  last_sync_timestamp TIMESTAMP,
  rows_synced INT64,
  updated_at TIMESTAMP
)
```

### Incremental Query:
Instead of `SELECT * FROM table`, uses:
```sql
SELECT * FROM table 
WHERE updated_at > {last_sync_timestamp}
```

### Merge Strategy:
```python
1. Extract rows WHERE updated_at > last_sync_time
2. Load to BigQuery staging table (temp)
3. MERGE staging INTO production:
   - WHEN MATCHED: UPDATE
   - WHEN NOT MATCHED: INSERT
4. Update metadata table with current timestamp
```

### Requirements:
- Source tables must have an `updated_at` or similar timestamp column
- Tables must have a primary key for merge operations

## 📊 Performance Improvements

### Sync Time Reduction:
- **v1:** Full table scan (hours for large tables)
- **v2:** Only changed rows (minutes)
- **Improvement:** 90%+ faster for active tables

### Database Load:
- **v1:** Full table scan every sync
- **v2:** Small filtered query
- **Improvement:** 95%+ reduction in database load

### Data Freshness:
- **v1:** 24-hour batch (daily)
- **v2:** <15 minutes (can run hourly)

## 🏗️ How It Works

```
┌─────────────┐
│   MySQL     │
└──────┬──────┘
       │
       │ 1. Get last_sync_time from metadata
       │
       │ 2. SELECT * WHERE updated_at > last_sync_time
       ▼
┌──────────────────┐
│  Staging Table   │ (temporary)
└────────┬─────────┘
         │
         │ 3. MERGE INTO production
         ▼
┌──────────────────┐
│ Production Table │
└────────┬─────────┘
         │
         │ 4. Update metadata
         ▼
┌──────────────────┐
│  Metadata Table  │
└──────────────────┘
```

## ✅ Benefits

1. **Scalability** - Handles tables with 100M+ rows efficiently
2. **Performance** - 10x-100x faster than full sync
3. **Reduced Load** - Minimal impact on source database
4. **Flexibility** - Can run hourly or more frequently
5. **Reliability** - Automatic resume from failures

## ⚠️ Limitations

1. **Requires Timestamp Column** - Source tables need `updated_at` or equivalent
2. **No Schema Change Handling** - Column additions/changes break the pipeline
3. **Deletes Not Tracked** - Deleted rows in MySQL remain in BigQuery
4. **Manual Configuration** - Table list still hardcoded in script

## 🔄 Why v3 Was Needed

While v2 solved performance issues, operational challenges remained:

**Problem:** Adding a column in MySQL broke the pipeline
- Pipeline would fail on schema mismatch
- Required manual ALTER TABLE in BigQuery
- Caused downtime until fixed

**v3 addressed this with automated schema monitoring.**

## 🚀 Running v2

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables (same as v1)
export MYSQL_HOST="your-host"
export MYSQL_USER="your-user"
export MYSQL_PASSWORD="your-password"
export MYSQL_DATABASE="your-database"
export BIGQUERY_PROJECT="your-project"
export BIGQUERY_DATASET="your-dataset"

# First run creates metadata table automatically
python main.py
```

### First Run Behavior:
- Creates metadata table if it doesn't exist
- Since no last_sync_time exists, syncs all data (like v1)
- Subsequent runs sync only changes

## 📖 What Was Learned

Building v2 taught:
- Incremental loading is critical for scale
- Metadata tracking enables reliable operations
- Merge patterns work well for upserts
- Schema management is the next bottleneck

These lessons informed v3's schema monitoring approach.

---

**Next:** See [version 3](../version%203/README.md)
