"""
Main orchestrator for MySQL to BigQuery sync pipeline.
"""
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from typing import Dict, List, Any

from config_loader import ConfigLoader
from mysql_extractor import MySQLExtractor
from bq_handler import BigQueryHandler
from schema_manager import SchemaManager
from metadata_manager import MetadataManager
from notifier import Notifier


class SyncOrchestrator:
    """Orchestrates the complete sync pipeline."""
    
    # Timezone for Lagos, Nigeria
    TIMEZONE = ZoneInfo("Africa/Lagos")
    
    def __init__(self, config_path: str = 'tables.yml'):
        # Load configuration
        self.config = ConfigLoader(config_path)
        
        # Initialize BigQuery client
        self.bq_client = bigquery.Client.from_service_account_json(
            self.config.bigquery_config['credentials_file']
        )
        
        # Initialize components
        self.mysql_extractor = MySQLExtractor(self.config.mysql_config)
        self.bq_handler = BigQueryHandler(self.bq_client)
        self.schema_manager = SchemaManager(self.bq_client)
        self.metadata_manager = MetadataManager(self.bq_client)
        
        # Initialize notifier with email config from YAML
        email_config = self.config.config.get('email', {})
        telegram_config = self.config.config.get('telegram', {})
        
        self.notifier = Notifier(
            sender_email=email_config.get('sender_email'),
            sender_password=email_config.get('sender_password'),
            smtp_server=email_config.get('smtp_server', 'smtp.gmail.com'),
            smtp_port=email_config.get('smtp_port', 587),
            telegram_bot_token=telegram_config.get('bot_token'),
            telegram_chat_id=telegram_config.get('chat_id')
        )
    
    def get_current_time(self) -> datetime:
        """Get current time in Lagos timezone."""
        return datetime.now(self.TIMEZONE)
    
    def sync_table(
        self, 
        table_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Sync a single table from MySQL to BigQuery.
        
        Args:
            table_config: Configuration dictionary for the table
            
        Returns:
            Dictionary with sync results
        """
        mysql_table = table_config['mysql_table']
        bq_table_id = table_config['bq_table']
        primary_key = table_config.get('primary_key', 'id')
        incremental_col = table_config.get(
            'incremental_column', 
            self.config.mysql_config.get('incremental_column', 'updated_at')
        )
        
        bq_config = self.config.bigquery_config
        project_id = bq_config['project_id']
        dataset_id = bq_config['dataset_id']
        staging_dataset_id = bq_config['staging_dataset_id']
        
        run_time = self.get_current_time()
        result = {
            'table': mysql_table,
            'status': 'FAILED',
            'run_time': run_time,
            'sync_time': None,
            'row_count': 0,
            'column_count': 0,
            'new_columns': [],
            'remark': '',
            'last_synced_value': None  # Initialize this early
        }
        
        try:
            # Get MySQL schema
            mysql_schema, err = self.mysql_extractor.get_table_schema(mysql_table)
            if err:
                result['remark'] = f"Failed to get MySQL schema: {err}"
                return result
            
            # Check if BigQuery table exists, create if not
            target_ref = f"{project_id}.{dataset_id}.{bq_table_id}"
            try:
                self.bq_client.get_table(target_ref)
            except Exception:
                # Table doesn't exist, create it
                success, err = self.schema_manager.create_table_from_mysql_schema(
                    project_id, dataset_id, bq_table_id, mysql_schema
                )
                if not success:
                    result['remark'] = f"Failed to create table: {err}"
                    return result
                result['remark'] += "Table created from MySQL schema. "
            
            # Add missing columns to existing table
            new_cols, err = self.schema_manager.add_missing_columns(
                target_ref, mysql_schema
            )
            if err:
                result['remark'] += f"Column addition warning: {err}. "
            elif new_cols:
                result['new_columns'] = new_cols
                result['remark'] += f"Added columns: {', '.join(new_cols)}. "
            
            # Get last sync time (from staging dataset metadata)
            last_synced = self.metadata_manager.get_last_sync_time(
                project_id, staging_dataset_id, 
                bq_config['metadata_table_id'], 
                bq_table_id
            )
            
            print(f"[{mysql_table}] Last synced timestamp: {last_synced}")
            
            # Extract data from MySQL
            df, err = self.mysql_extractor.extract_incremental_data(
                mysql_table, incremental_col, last_synced
            )
            if err:
                result['remark'] += f"MySQL extraction failed: {err}"
                return result
            
            print(f"[{mysql_table}] Extracted {len(df)} rows where {incremental_col} > {last_synced}")
            
            # Prepare DataFrame using MySQL schema as source of truth
            df = self.schema_manager.prepare_dataframe_with_schema(df, mysql_schema)
            
            # Ensure staging table exists (in staging dataset)
            staging_ref = f"{project_id}.{staging_dataset_id}.{bq_table_id}_staging"
            schema, err = self.bq_handler.ensure_staging_table(target_ref, staging_ref)
            if err:
                result['remark'] += f"Staging table error: {err}"
                return result
            
            # Update staging table schema if needed
            staging_new_cols, err = self.schema_manager.add_missing_columns(
                staging_ref, mysql_schema
            )
            if err:
                result['remark'] += f"Staging schema update warning: {err}. "
            
            # Load to staging
            row_count, err = self.bq_handler.load_to_staging(df, staging_ref, schema)
            if err:
                result['remark'] += f"Load to staging failed: {err}"
                return result
            
            result['row_count'] = row_count
            
            # Merge to target (staging dataset to production dataset)
            merge_msg, err = self.bq_handler.merge_staging_to_target(
                project_id, dataset_id, staging_dataset_id,
                bq_table_id, f"{bq_table_id}_staging", 
                primary_key, incremental_col, df
            )
            
            if err:
                result['remark'] += f"Merge failed: {err}"
                return result
            
            result['remark'] += merge_msg
            result['status'] = 'SUCCESS'
            result['sync_time'] = self.get_current_time()
            
            # Get the actual last synced timestamp from the data
            if not df.empty and incremental_col in df.columns:
                last_synced_value = df[incremental_col].max()
                print(f"[{mysql_table}] New last_synced_value: {last_synced_value}")
                result['last_synced_value'] = last_synced_value
            else:
                print(f"[{mysql_table}] No new data, keeping old last_synced: {last_synced}")
                result['last_synced_value'] = last_synced  # Keep the old value
            
        except Exception as e:
            result['remark'] = f"Unexpected error: {str(e)}"
        
        return result
    
    def run_sync_pipeline(self) -> List[Dict[str, Any]]:
        """
        Run the complete sync pipeline for all configured tables.
        
        Returns:
            List of sync results for each table
        """
        # Setup metadata table (in staging dataset)
        bq_config = self.config.bigquery_config
        self.metadata_manager.setup_metadata_table(
            bq_config['project_id'],
            bq_config['staging_dataset_id'],  # Use staging dataset for metadata
            bq_config['metadata_table_id']
        )
        
        sync_results = []
        
        # Sync each table
        for table_config in self.config.tables:
            result = self.sync_table(table_config)
            sync_results.append(result)
            
            # Determine the sync time value to store
            if result['status'] == 'SUCCESS' and result.get('last_synced_value') is not None:
                sync_time_to_store = result['last_synced_value']
                print(f"[{table_config['bq_table']}] Storing last_synced_value: {sync_time_to_store}")
            else:
                sync_time_to_store = None
                print(f"[{table_config['bq_table']}] No last_synced_value to store (status={result['status']}, value={result.get('last_synced_value')})")
            
            # Update metadata (in staging dataset)
            self.metadata_manager.update_metadata(
                bq_config['project_id'],
                bq_config['staging_dataset_id'],  # Use staging dataset for metadata
                bq_config['metadata_table_id'],
                table_config['bq_table'],
                result['run_time'],
                sync_time_to_store,  # Use the actual last synced timestamp
                result['status'],
                result['row_count'],
                result['column_count'],
                result['remark']
            )
        
        # Send notifications
        if self.config.alert_recipients:
            email_err = self.notifier.send_email_alert(
                sync_results, 
                self.config.alert_recipients
            )
            if email_err:
                print(f"Email notification failed: {email_err}")
            else:
                print(f"Email notification sent to {len(self.config.alert_recipients)} recipient(s)")
        else:
            print("No email recipients configured - skipping email notification")
        
        # Send Telegram notification
        telegram_err = self.notifier.send_telegram_sync_notification(sync_results)
        if telegram_err:
            print(f"Telegram notification failed: {telegram_err}")
        else:
            print("Telegram notification sent successfully")
        
        return sync_results