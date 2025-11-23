import os
import pandas as pd
import pymysql
import yaml
import smtplib
from sshtunnel import SSHTunnelForwarder
from google.cloud import bigquery
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# =========================
# Load Configuration
# =========================
def load_config(config_path= '/config/tables.yml'):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)



load_config()
cfg = load_config('/config/tables.yml')
mysql_cfg = cfg['mysql']
bq_cfg = cfg['bigquery']
tables = cfg['tables']
recipients = bq_cfg.get('alert_recipients', [])
staging_dataset_id = f"{bq_cfg['dataset_id']}"
# =========================
# Utility: Send HTML email alert
# =========================
def send_error_email(error_list, recipients):
    if not error_list:
        return
    msg = MIMEMultipart("alternative")
    msg['Subject'] = "MySQL→BigQuery Sync Pipeline Alerts"
    msg['From'] = "SENDEREMAIL"
    msg['To'] = ", ".join(recipients)

    html_content = """
    <html><body>
    <h2>MySQL → BigQuery Sync Pipeline Report</h2>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse;">
        <thead>
            <tr style="background-color: #f2f2f2;">
                <th>Table</th>
                <th>Step</th>
                <th>Remark</th>
                <th>Timestamp</th>
            </tr>
        </thead><tbody>
    """
    for err in error_list:
        html_content += f"""
        <tr>
            <td>{err.get('table')}</td>
            <td>{err.get('step')}</td>
            <td>{err.get('remark')}</td>
            <td>{err.get('timestamp')}</td>
        </tr>
        """
    html_content += "</tbody></table></body></html>"
    msg.attach(MIMEText(html_content, "html"))

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login('USERNAME', 'PASSWORD')
            server.sendmail(msg['From'], recipients, msg.as_string())
    except Exception as e:
        print(f"Email alert failed: {e}")


# =========================
# Setup BigQuery client
# =========================
def get_bq_client(credentials_path):
    return bigquery.Client.from_service_account_json(credentials_path)


get_bq_client(bq_cfg['credentials_file'])
client = get_bq_client(bq_cfg['credentials_file'])
# =========================
# Ensure metadata table exists
# =========================
def setup_metadata_table(client, project_id, staging_dataset_id, metadata_table_id):
    table_ref = f"{project_id}.{staging_dataset_id}.{metadata_table_id}"
    try:
        client.get_table(table_ref)
    except Exception:
        schema = [
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_run", "TIMESTAMP"),
            bigquery.SchemaField("last_synced", "TIMESTAMP"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("row_count", "INT64"),
            bigquery.SchemaField("column_count", "INT64"),
            bigquery.SchemaField("remark", "STRING")
        ]
        client.create_table(bigquery.Table(table_ref, schema=schema))
    return table_ref
# =========================
# Get last synced timestamp
# =========================
def get_last_synced(client, project_id, staging_dataset_id, metadata_table_id, table_name):
    query = f"""
        SELECT last_synced
        FROM `{project_id}.{staging_dataset_id}.{metadata_table_id}`
        WHERE table_name = '{table_name}'
    """
    result = list(client.query(query).result())
    return result[0].last_synced if result else datetime(1970, 1, 1)

last_synced = get_last_synced(client, bq_cfg['project_id'], staging_dataset_id, bq_cfg['metadata_table_id'], 'users')
last_synced
# =========================
# Extract incremental data from MySQL
# =========================
def extract_mysql_data(mysql_cfg, last_synced, table_name, incremental_column):
    try:
        with SSHTunnelForwarder(
            (mysql_cfg['ssh_host'], mysql_cfg['ssh_port']),
            ssh_username=mysql_cfg['ssh_user'],
            ssh_pkey=mysql_cfg['ssh_private_key'],
            remote_bind_address=('127.0.0.1', mysql_cfg['db_port'])
        ) as tunnel:
            connection = pymysql.connect(
                host='127.0.0.1',
                port=tunnel.local_bind_port,
                user=mysql_cfg['db_user'],
                password=mysql_cfg['db_password'],
                database=mysql_cfg['db_name'],
                cursorclass=pymysql.cursors.DictCursor
            )
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table_name} WHERE {incremental_column} > '{last_synced}'")
                result = cursor.fetchall()
                df = pd.DataFrame(result)
        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)

def sanitize_dataframe_for_bigquery(df):
    """
    Ensures all columns are in BigQuery-compatible dtypes.
    - Integer → nullable Int64
    - Datetime → timezone-naive datetime64[ns]
    - Boolean → nullable boolean
    """

    for col in df.columns:

        if col == 'phone_number':
            pass
        
        elif col == 'date_paid':
            df[col] = pd.to_datetime(df[col], errors='coerce')
        # Nullable integer type
        elif pd.api.types.is_integer_dtype(df[col].dtype):
            df[col] = df[col].astype('Int64')

        
        # Object dtype but numeric → convert too
        elif df[col].dtype == object:
            # Check if column is actually integer-like
            try:
                df[col] = pd.to_numeric(df[col])
                if pd.api.types.is_integer_dtype(df[col].dtype):
                    df[col] = df[col].astype('Int64')
            except Exception:
                pass
        

        # Datetime columns
        if pd.api.types.is_datetime64_any_dtype(df[col].dtype):
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # Boolean → nullable boolean
        if pd.api.types.is_bool_dtype(df[col].dtype):
            df[col] = df[col].astype('boolean')

    return df


sanitize_dataframe_for_bigquery(df)
df['date_paid'].dtype
# =========================
# Ensure staging table exists
# =========================
def ensure_staging_table(client, target_table_ref, staging_table_ref):
    try:
        target_table = client.get_table(target_table_ref)
        try:
            client.get_table(staging_table_ref)
        except Exception:
            staging_table = bigquery.Table(staging_table_ref, schema=target_table.schema)
            client.create_table(staging_table)
        return target_table.schema, None
    except Exception as e:
        return None, str(e)

def handle_schema_changes(client, target_table_ref, df):
    new_columns = []
    try:
        table = client.get_table(target_table_ref)   # Fetch full existing table object
        existing_schema = list(table.schema)         # Existing schema list
        existing_cols = [f.name for f in existing_schema]

        updated = False

        for col in df.columns:
            if col not in existing_cols:

                # Infer BQ data type
                dtype = df[col].dtype
                if pd.api.types.is_integer_dtype(dtype):
                    bq_type = 'INT64'
                elif pd.api.types.is_float_dtype(dtype):
                    bq_type = 'FLOAT64'
                elif pd.api.types.is_bool_dtype(dtype):
                    bq_type = 'BOOL'
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    bq_type = 'TIMESTAMP'
                else:
                    bq_type = 'STRING'

                # Create new BigQuery field
                new_field = bigquery.SchemaField(col, bq_type)
                existing_schema.append(new_field)
                new_columns.append(col)
                updated = True

        if updated:
            # Apply only the modified schema
            table.schema = existing_schema
            client.update_table(table, ["schema"])

        return new_columns, None

    except Exception as e:
        return [], str(e)


# =========================
# Load DataFrame to staging table
# =========================
def load_to_staging(client, df, staging_table_ref, schema):
    try:
        if df.empty:
            return 0, None
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, staging_table_ref, job_config=job_config).result()
        return len(df), None
    except Exception as e:
        return 0, str(e)


# =========================
# Merge staging to target table
# =========================
def merge_to_target(client, project_id, dataset_id, target_table_id, staging_table_id, df):
    try:
        if df.empty:
            return "No new or updated rows", None
        target_ref = f"{project_id}.{dataset_id}.{target_table_id}"
        staging_ref = f"{project_id}.{dataset_id}.{staging_table_id}"
        target_schema = client.get_table(target_ref).schema
        cols = [f.name for f in target_schema]
        cols_without_id = [c for c in cols if c != "id"]
        update_clause = ", ".join([f"T.{c} = S.{c}" for c in cols_without_id])
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join([f"S.{c}" for c in cols])
        merge_sql = f"""
        MERGE `{target_ref}` T
        USING `{staging_ref}` S
        ON T.id = S.id
        WHEN MATCHED AND T.updated_at < S.updated_at THEN
          UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        client.query(merge_sql).result()
        client.query(f"TRUNCATE TABLE `{staging_ref}`").result()
        return "Merge completed successfully", None
    except Exception as e:
        return "", str(e)


# =========================
# Update metadata table
# =========================
def update_metadata(client, project_id, dataset_id, metadata_table_id, table_name,
                    last_synced, row_count, column_count, status, remark):
    try:
        merge_sql = f"""
        MERGE `{project_id}.{dataset_id}.{metadata_table_id}` M
        USING (SELECT '{table_name}' AS table_name,
               TIMESTAMP('{datetime.now()}') AS last_run,
               TIMESTAMP('{last_synced}') AS last_synced,
               '{status}' AS status,
               {row_count} AS row_count,
               {column_count} AS column_count,
               '{remark}' AS remark) S
        ON M.table_name = S.table_name
        WHEN MATCHED THEN
          UPDATE SET last_run=S.last_run, last_synced=S.last_synced, status=S.status,
                     row_count=S.row_count, column_count=S.column_count, remark=S.remark
        WHEN NOT MATCHED THEN
          INSERT (table_name, last_run, last_synced, status, row_count, column_count, remark)
          VALUES (S.table_name, S.last_run, S.last_synced, S.status,
                  S.row_count, S.column_count, S.remark)
        """
        client.query(merge_sql).result()
        return None
    except Exception as e:
        return str(e)

# =========================
# Main Handler
# =========================
def handler(request=None):
    cfg = load_config('/config/tables.yml')

    mysql_cfg = cfg['mysql']
    bq_cfg = cfg['bigquery']
    tables = cfg['tables']
    recipients = bq_cfg.get('alert_recipients', [])

    client = get_bq_client(bq_cfg['credentials_file'])
    staging_dataset_id = f"{bq_cfg['dataset_id']}"
    metadata_table_ref = setup_metadata_table(
        client,
        bq_cfg['project_id'],
        staging_dataset_id,
        bq_cfg['metadata_table_id']
    )

    error_list = []

    for tbl in tables:
        table_name = tbl['mysql_table']
        bq_table_id = tbl['bq_table']
        incremental_col = tbl.get('incremental_column', mysql_cfg['incremental_column'])

        last_synced = get_last_synced(
            client,
            bq_cfg['project_id'],
            staging_dataset_id,
            bq_cfg['metadata_table_id'],
            bq_table_id
        )

        # =========================================
        # Extract MySQL Data
        # =========================================
        df, err = extract_mysql_data(mysql_cfg, last_synced, table_name, incremental_col)
        if err:
            error_list.append({
                "table": table_name,
                "step": "MySQL Extract",
                "remark": err,
                "timestamp": datetime.now(),
                "rows_processed": 0,
                "column_count": 0,
                "merge_status": "FAILED",
                "new_columns": []
            })
            df = pd.DataFrame()

        # Sanitise dataframe
        df = sanitize_dataframe_for_bigquery(df)

        # Table references
        target_ref = f"{bq_cfg['project_id']}.{bq_cfg['dataset_id']}.{bq_table_id}"
        staging_ref = f"{bq_cfg['project_id']}.{staging_dataset_id}.{bq_table_id}_staging"

        # =========================================
        # Ensure Staging Table Exists
        # =========================================
        schema, err = ensure_staging_table(client, target_ref, staging_ref)
        if err:
            error_list.append({
                "table": table_name,
                "step": "Ensure Staging Table",
                "remark": err,
                "timestamp": datetime.now(),
                "rows_processed": 0,
                "column_count": 0,
                "merge_status": "FAILED",
                "new_columns": []
            })
            continue

        # =========================================
        # Schema Changes (Target)
        # =========================================
        new_cols, err = handle_schema_changes(client, target_ref, df)
        if err:
            error_list.append({
                "table": table_name,
                "step": "Schema Update (target)",
                "remark": err,
                "timestamp": datetime.now(),
                "rows_processed": 0,
                "column_count": len(schema),
                "merge_status": "FAILED",
                "new_columns": []
            })

        # =========================================
        # Schema Changes (Staging)
        # =========================================
        staging_new_cols, err = handle_schema_changes(client, staging_ref, df)
        if err:
            error_list.append({
                "table": table_name,
                "step": "Schema Update (staging)",
                "remark": err,
                "timestamp": datetime.now(),
                "rows_processed": 0,
                "column_count": len(schema),
                "merge_status": "FAILED",
                "new_columns": staging_new_cols
            })

        # =========================================
        # Load to Staging
        # =========================================
        row_count, err = load_to_staging(client, df, staging_ref, schema)
        if err:
            error_list.append({
                "table": table_name,
                "step": "Load to Staging",
                "remark": err,
                "timestamp": datetime.now(),
                "rows_processed": 0,
                "column_count": len(schema),
                "merge_status": "FAILED",
                "new_columns": new_cols
            })

        # =========================================
        # Merge into Target Table
        # =========================================
        merge_msg, err = merge_to_target(
            client,
            bq_cfg['project_id'],
            bq_cfg['dataset_id'],
            bq_table_id,
            f"{bq_table_id}_staging",
            df
        )

        if err:
            merge_status = "FAILED"
            error_list.append({
                "table": table_name,
                "step": "Merge",
                "remark": err,
                "timestamp": datetime.now(),
                "rows_processed": row_count,
                "column_count": len(schema),
                "merge_status": "FAILED",
                "new_columns": new_cols
            })
        else:
            merge_status = "SUCCESS"

        # =========================================
        # Build Remark for Metadata
        # =========================================
        remark = ""
        if new_cols:
            remark += f"New columns added: {', '.join(new_cols)}. "

        remark += f"Rows processed: {row_count}. "
        remark += f"Column count: {len(schema)}. "
        remark += merge_msg or ""

        # =========================================
        # Update Metadata Table
        # =========================================
        last_sync_val = df[incremental_col].max() if not df.empty else last_synced

        metadata_err = update_metadata(
            client,
            bq_cfg['project_id'],
            bq_cfg['dataset_id'],
            bq_cfg['metadata_table_id'],
            bq_table_id,
            last_sync_val,
            row_count,
            len(schema),
            merge_status,
            remark
        )

        if metadata_err:
            error_list.append({
                "table": table_name,
                "step": "Update Metadata",
                "remark": metadata_err,
                "timestamp": datetime.now(),
                "rows_processed": row_count,
                "column_count": len(schema),
                "merge_status": merge_status,
                "new_columns": new_cols
            })

        # =========================================
        # SUCCESS ENTRY
        # =========================================
        if merge_status == "SUCCESS":
            error_list.append({
                "table": table_name,
                "step": "Completed",
                "remark": remark,
                "timestamp": datetime.now(),
                "rows_processed": row_count,
                "column_count": len(schema),
                "merge_status": merge_status,
                "new_columns": new_cols
            })

    # =========================================
    # Send Email Alert
    # =========================================
    send_error_email(error_list, recipients)

    return "Pipeline executed successfully."
