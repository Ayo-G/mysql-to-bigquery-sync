"""
Metadata management module for tracking sync operations.
"""
from google.cloud import bigquery
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Tuple


class MetadataManager:
    """Handles metadata table operations for tracking sync history."""
    
    # Timezone for Lagos, Nigeria
    TIMEZONE = ZoneInfo("Africa/Lagos")
    
    def __init__(self, client: bigquery.Client):
        self.client = client
    
    def setup_metadata_table(
        self,
        project_id: str,
        dataset_id: str,
        metadata_table_id: str
    ) -> Tuple[str, Optional[str]]:
        """
        Ensure metadata table exists with proper schema.
        
        Args:
            project_id: BigQuery project ID
            dataset_id: BigQuery dataset ID
            metadata_table_id: Metadata table ID
            
        Returns:
            Tuple of (table_ref, error_message)
        """
        table_ref = f"{project_id}.{dataset_id}.{metadata_table_id}"
        
        try:
            self.client.get_table(table_ref)
            return table_ref, None
            
        except Exception:
            # Create metadata table with enhanced schema
            schema = [
                bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("last_run_time", "TIMESTAMP", 
                                   description="Time when sync was last run (regardless of status)"),
                bigquery.SchemaField("last_sync_time", "TIMESTAMP", 
                                   description="Time when sync last completed successfully"),
                bigquery.SchemaField("status", "STRING", 
                                   description="Sync status: SUCCESS or FAILED"),
                bigquery.SchemaField("row_count", "INT64", 
                                   description="Number of rows synced in last successful run"),
                bigquery.SchemaField("column_count", "INT64", 
                                   description="Total columns in BigQuery table after sync"),
                bigquery.SchemaField("remark", "STRING", 
                                   description="Details about sync: errors, new columns, etc.")
            ]
            
            try:
                table = bigquery.Table(table_ref, schema=schema)
                self.client.create_table(table)
                return table_ref, None
            except Exception as e:
                return table_ref, f"Failed to create metadata table: {str(e)}"
    
    def get_last_sync_time(
        self,
        project_id: str,
        dataset_id: str,
        metadata_table_id: str,
        table_name: str
    ) -> datetime:
        """
        Get the last successful sync timestamp for a table.
        
        Args:
            project_id: BigQuery project ID
            dataset_id: BigQuery dataset ID
            metadata_table_id: Metadata table ID
            table_name: Name of the table to check
            
        Returns:
            Last sync timestamp or epoch if never synced
        """
        query = f"""
            SELECT last_sync_time
            FROM `{project_id}.{dataset_id}.{metadata_table_id}`
            WHERE table_name = '{table_name}'
            AND status = 'SUCCESS'
            ORDER BY last_sync_time DESC
            LIMIT 1
        """
        
        try:
            result = list(self.client.query(query).result())
            return result[0].last_sync_time if result else datetime(1970, 1, 1)
        except Exception:
            return datetime(1970, 1, 1)
    
    def update_metadata(
        self,
        project_id: str,
        dataset_id: str,
        metadata_table_id: str,
        table_name: str,
        run_time: datetime,
        sync_time: Optional[datetime],
        status: str,
        row_count: int,
        column_count: int,
        remark: str
    ) -> Optional[str]:
        """
        Update metadata table with sync information.
        
        Args:
            project_id: BigQuery project ID
            dataset_id: BigQuery dataset ID
            metadata_table_id: Metadata table ID
            table_name: Name of synced table
            run_time: Time when sync was run
            sync_time: Time when sync completed successfully (None if failed)
            status: Sync status (SUCCESS or FAILED)
            row_count: Number of rows synced
            column_count: Total columns in table
            remark: Details about the sync
            
        Returns:
            Error message if failed, None if successful
        """
        try:
            # Escape single quotes in remark
            remark_escaped = remark.replace("'", "\\'")
            
            # Format run_time for BigQuery (remove timezone info for storage)
            run_time_str = run_time.astimezone(self.TIMEZONE).replace(tzinfo=None).isoformat()
            
            # Build sync_time clause
            if sync_time and status == 'SUCCESS':
                # Handle both datetime and Timestamp objects from pandas
                if hasattr(sync_time, 'to_pydatetime'):
                    # It's a pandas Timestamp
                    sync_time = sync_time.to_pydatetime()
                
                # If timezone-aware, convert to Lagos time then remove tz
                if hasattr(sync_time, 'tzinfo') and sync_time.tzinfo is not None:
                    sync_time_str = sync_time.astimezone(self.TIMEZONE).replace(tzinfo=None).isoformat()
                else:
                    # Already timezone-naive, just format
                    sync_time_str = sync_time.isoformat()
                
                sync_time_clause = f"TIMESTAMP('{sync_time_str}')"
            else:
                # Keep existing sync time if this run failed
                sync_time_clause = """
                    COALESCE(
                        (SELECT last_sync_time 
                         FROM `{0}.{1}.{2}` 
                         WHERE table_name = '{3}'),
                        TIMESTAMP('1970-01-01 00:00:00')
                    )
                """.format(project_id, dataset_id, metadata_table_id, table_name)
            
            merge_sql = f"""
            MERGE `{project_id}.{dataset_id}.{metadata_table_id}` M
            USING (
                SELECT 
                    '{table_name}' AS table_name,
                    TIMESTAMP('{run_time_str}') AS last_run_time,
                    {sync_time_clause} AS last_sync_time,
                    '{status}' AS status,
                    {row_count} AS row_count,
                    {column_count} AS column_count,
                    '{remark_escaped}' AS remark
            ) S
            ON M.table_name = S.table_name
            WHEN MATCHED THEN
              UPDATE SET 
                last_run_time = S.last_run_time,
                last_sync_time = S.last_sync_time,
                status = S.status,
                row_count = S.row_count,
                column_count = S.column_count,
                remark = S.remark
            WHEN NOT MATCHED THEN
              INSERT (table_name, last_run_time, last_sync_time, status, 
                      row_count, column_count, remark)
              VALUES (S.table_name, S.last_run_time, S.last_sync_time, 
                      S.status, S.row_count, S.column_count, S.remark)
            """
            
            # Debug: Show the sync_time being stored
            if sync_time and status == 'SUCCESS':
                print(f"[Metadata] Storing sync_time for {table_name}: {sync_time_str}")
            else:
                print(f"[Metadata] NOT updating sync_time for {table_name} (status={status}, sync_time={sync_time})")
            
            self.client.query(merge_sql).result()
            
            # Debug: Log what was stored
            print(f"[Metadata] Updated {table_name}: status={status}")
            
            return None
            
        except Exception as e:
            return f"Failed to update metadata: {str(e)}"