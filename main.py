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
def load_config(config_path='config/tables.yml'):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# =========================
# Utility: Send HTML email alert
# =========================
def send_error_email(error_list, recipients):
    if not error_list:
        return
    msg = MIMEMultipart("alternative")
    msg['Subject'] = "MySQL→BigQuery Sync Pipeline Alerts"
    msg['From'] = "noreply@pipeline-monitor.com"
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
            server.login('your_email@gmail.com', 'your_app_password')
            server.sendmail(msg['From'], recipients, msg.as_string())
    except Exception as e:
        print(f"Email alert failed: {e}")


# =========================
# Setup BigQuery client
# =========================
def get_bq_client(credentials_path):
    return bigquery.Client.from_service_account_json(credentials_path)


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


# =========================
# Handle schema changes
# =========================
def handle_schema_changes(client, target_table_ref, df):
    new_columns = []
    try:
        target_table = client.get_table(target_table_ref)
        existing_cols = [f.name for f in target_table.schema]

        for col in df.columns:
            if col not in existing_cols:
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
                new_field = bigquery.SchemaField(col, bq_type)
                client.update_table(
                    bigquery.Table(target_table_ref, schema=target_table.schema + [new_field]),
                    ["schema"]
                )
                new_columns.append(col)
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
        staging_ref = f"{project_id}.{dataset_id}_staging.{staging_table_id}"
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
    cfg = load_config('config/tables.yml')

    mysql_cfg = cfg['mysql']
    bq_cfg = cfg['bigquery']
    tables = cfg['tables']
    recipients = bq_cfg.get('alert_recipients', [])

    client = get_bq_client(bq_cfg['credentials_file'])
    staging_dataset_id = f"{bq_cfg['dataset_id']}_staging"
    metadata_table_ref = setup_metadata_table(client, bq_cfg['project_id'], staging_dataset_id, bq_cfg['metadata_table_id'])

    error_list = []

    for tbl in tables:
        table_name = tbl['mysql_table']
        bq_table_id = tbl['bq_table']
        incremental_col = tbl.get('incremental_column', mysql_cfg['incremental_column'])
        last_synced = get_last_synced(client, bq_cfg['project_id'], staging_dataset_id, bq_cfg['metadata_table_id'], bq_table_id)

        # Extract MySQL data
        df, err = extract_mysql_data(mysql_cfg, last_synced, table_name, incremental_col)
        if err:
            error_list.append({'table': table_name, 'step': 'MySQL Extract', 'remark': err, 'timestamp': datetime.now()})
            df = pd.DataFrame()

        target_ref = f"{bq_cfg['project_id']}.{bq_cfg['dataset_id']}.{bq_table_id}"
        staging_ref = f"{bq_cfg['project_id']}.{staging_dataset_id}.{bq_table_id}_staging"
        schema, err = ensure_staging_table(client, target_ref, staging_ref)
        if err:
            error_list.append({'table': table_name, 'step': 'Ensure Staging', 'remark': err, 'timestamp': datetime.now()})
            continue

        # Schema changes
        new_cols, err = handle_schema_changes(client, target_ref, df)
        if err:
            error_list.append({'table': table_name, 'step': 'Schema Update', 'remark': err, 'timestamp': datetime.now()})

        # Load to staging
        row_count, err = load_to_staging(client, df, staging_ref, schema)
        if err:
            error_list.append({'table': table_name, 'step': 'Load Staging', 'remark': err, 'timestamp': datetime.now()})

        # Merge
        merge_remark, err = merge_to_target(client, bq_cfg['project_id'], bq_cfg['dataset_id'], bq_table_id, f"{bq_table_id}_staging", df)
        if err:
            error_list.append({'table': table_name, 'step': 'Merge', 'remark': err, 'timestamp': datetime.now()})
            status = "FAILED"
        else:
            status = "SUCCESS"

        # Metadata update
        remark = ""
        if new_cols:
            remark += f"New columns added: {', '.join(new_cols)}. "
        if merge_remark:
            remark += merge_remark
        if status == "FAILED" and not remark:
            remark = "Pipeline encountered errors. Check alert email."

        last_sync_val = df[incremental_col].max() if not df.empty else last_synced
        err = update_metadata(client, bq_cfg['project_id'], bq_cfg['dataset_id'],
                              bq_cfg['metadata_table_id'], bq_table_id,
                              last_sync_val, row_count, len(schema), status, remark)
        if err:
            error_list.append({'table': table_name, 'step': 'Update Metadata', 'remark': err, 'timestamp': datetime.now()})

    # Send email alert if any errors
    send_error_email(error_list, recipients)
    return "Pipeline executed successfully."
