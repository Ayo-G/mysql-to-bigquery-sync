import pandas as pd
from pandas_gbq import read_gbq
import smtplib
from email.mime.text import MIMEText
from sshtunnel import SSHTunnelForwarder
from google.cloud import storage
from google.cloud import bigquery
from tabulate import tabulate
import pymysql
import os
import pyarrow
from datetime import datetime

def handler(request):
  # SSH Config
  ssh_host = 'SSHHOST'
  ssh_port = 22
  ssh_user = 'SSHUSER'
  localhost = '127.0.0.1'

  cd = os.getcwd()
  file = 'key.ppk'
  ssh_private_key_path = os.path.join(cd, file)

  # MySQL Config
  db_user = 'DBUSER'
  db_password = 'DBPASSWORD'
  db_name = 'DBNAME'
  db_port = 3306
  db_host = localhost

  # BigQuery Config
  bq_project_id = 'PROJECTID'
  bq_dataset_id = 'DATASETID'
  bq_table_id = 'TABLEID'
  staging_dataset_id = bq_dataset_id + "_staging"
  staging_table_id = bq_table_id + "_staging"
  metadata_table_id = "sync_metadata"

  # BigQuery Client
  cd = os.getcwd()
  json_file = 'key.json'
  credentials_path = os.path.join(cd, json_file)
  client = bigquery.Client.from_service_account_json(credentials_path)


  # STEP 1 - Ensure the Metadata table exists
  metadata_table_ref = f"{bq_project_id}.{staging_dataset_id}.{metadata_table_id}"

  try:
      client.get_table(metadata_table_ref)
  except:
      schema = [
          bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("last_synced", "TIMESTAMP", mode="NULLABLE")
      ]
      table = bigquery.Table(metadata_table_ref, schema=schema)
      client.create_table(table)
      print(f"Created metadata table: {metadata_table_id}")
  else:
      print("Metadata table exists.")

  # STEP 2 - Get last synced timestamp
  query_last_sync = f"""
  SELECT last_synced
  FROM `{bq_project_id}.{staging_dataset_id}.{metadata_table_id}`
  WHERE table_name = '{bq_table_id}'
  """
  rows = client.query(query_last_sync).result()
  last_synced_row = list(rows)
  last_synced = last_synced_row[0].last_synced if last_synced_row else datetime(1970,1,1)

  print("Last synced timestamp:", last_synced)

  # STEP 3 - Extract incremental data from MYSQL
  with SSHTunnelForwarder(
      (ssh_host, ssh_port),
      ssh_username=ssh_user,
      ssh_pkey=ssh_private_key_path,
      remote_bind_address=(db_host, db_port),
  ) as tunnel:
      connection = pymysql.connect(
          host='127.0.0.1',
          port=tunnel.local_bind_port,
          user=db_user,
          password=db_password,
          database=db_name,
          charset='utf8mb4',
          cursorclass=pymysql.cursors.DictCursor,
      )
      try:
          with connection.cursor() as cursor:
              cursor.execute(f"SELECT * FROM {bq_table_id} WHERE updated_at > '{last_synced}'")
              result = cursor.fetchall()
              df = pd.DataFrame(result)
      finally:
          connection.close()

  print("Rows fetched from MySQL database:", len(df))

  # STEP 4 - Ensure Staging table exists
  target_table_ref = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
  target_table = client.get_table(target_table_ref)
  staging_table_ref = f"{bq_project_id}.{staging_dataset_id}.{staging_table_id}"

  try:
      client.get_table(staging_table_ref)
  except:
      staging_table = bigquery.Table(staging_table_ref, schema=target_table.schema)
      client.create_table(staging_table)
      print(f"Created staging table: {staging_table_id}")
  else:
      print("Staging table exists.")

  # STEP 5 - Load data into Staging table
  if not df.empty:
      load_job_config = bigquery.LoadJobConfig(
          schema=target_table.schema,
          write_disposition="WRITE_TRUNCATE"
      )
      load_job = client.load_table_from_dataframe(
          df, staging_table_ref, job_config=load_job_config
      )
      load_job.result()
      print(f"Loaded {len(df)} row(s) into staging table.")
  else:
      print("No new or updated rows to load.")

  # STEP 6 - Dynamic Merge SQL
  if not df.empty:
      cols = [field.name for field in target_table.schema]
      cols_without_id = [c for c in cols if c != "id"]
      update_clause = ",\n    ".join([f"T.{c} = S.{c}" for c in cols_without_id])
      insert_cols = ", ".join(cols)
      insert_vals = ", ".join([f"S.{c}" for c in cols])

      merge_sql = f"""
      MERGE `{bq_project_id}.{bq_dataset_id}.{bq_table_id}` T
      USING `{bq_project_id}.{staging_dataset_id}.{staging_table_id}` S
      ON T.id = S.id
      WHEN MATCHED AND T.updated_at < S.updated_at THEN
        UPDATE SET
          {update_clause}
      WHEN NOT MATCHED THEN
        INSERT ({insert_cols})
        VALUES ({insert_vals})
      """
      client.query(merge_sql).result()
      print("MERGE completed successfully.")

      # CLEAR STAGING TABLE AFTER MERGE
      truncate_sql = f"TRUNCATE TABLE `{bq_project_id}.{staging_dataset_id}.{staging_table_id}`"
      client.query(truncate_sql).result()
      print(f"Staging table {staging_table_id} truncated successfully.")

  else:
      print("Can't Merge because no new or updated rows to load.")


  # STEP 7 - Update the Metadata table
  if not df.empty:
      new_last_synced = udf['updated_at'].max()
      merge_metadata_sql = f"""
      MERGE `{bq_project_id}.{staging_dataset_id}.{metadata_table_id}` M
      USING (SELECT '{bq_table_id}' AS table_name, TIMESTAMP('{new_last_synced}') AS last_synced) S
      ON M.table_name = S.table_name
      WHEN MATCHED THEN UPDATE SET M.last_synced = S.last_synced
      WHEN NOT MATCHED THEN INSERT (table_name, last_synced) VALUES (S.table_name, S.last_synced)
      """
      client.query(merge_metadata_sql).result()
      print(f"Metadata updated: last_synced = {new_last_synced}")
