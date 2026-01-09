"""
BigQuery operations handler module.
"""
import pandas as pd
from google.cloud import bigquery
from typing import Tuple, Optional


class BigQueryHandler:
    """Handles BigQuery operations including loading and merging data."""
    
    def __init__(self, client: bigquery.Client):
        self.client = client
    
    def ensure_staging_table(
        self,
        target_table_ref: str,
        staging_table_ref: str
    ) -> Tuple[Optional[list], Optional[str]]:
        """
        Ensure staging table exists with same schema as target table.
        
        Args:
            target_table_ref: Full reference to target table
            staging_table_ref: Full reference to staging table
            
        Returns:
            Tuple of (schema, error_message)
        """
        try:
            target_table = self.client.get_table(target_table_ref)
            
            try:
                self.client.get_table(staging_table_ref)
            except Exception:
                staging_table = bigquery.Table(
                    staging_table_ref, 
                    schema=target_table.schema
                )
                self.client.create_table(staging_table)
            
            return target_table.schema, None
            
        except Exception as e:
            return None, f"Failed to ensure staging table: {str(e)}"
    
    def load_to_staging(
        self,
        df: pd.DataFrame,
        staging_table_ref: str,
        schema: list
    ) -> Tuple[int, Optional[str]]:
        """
        Load DataFrame to staging table.
        
        Args:
            df: DataFrame to load
            staging_table_ref: Full reference to staging table
            schema: BigQuery schema
            
        Returns:
            Tuple of (row_count, error_message)
        """
        try:
            if df.empty:
                return 0, None
            
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_TRUNCATE"
            )
            
            self.client.load_table_from_dataframe(
                df, 
                staging_table_ref, 
                job_config=job_config
            ).result()
            
            return len(df), None
            
        except Exception as e:
            return 0, f"Failed to load to staging: {str(e)}"
    
    def merge_staging_to_target(
        self,
        project_id: str,
        target_dataset_id: str,
        staging_dataset_id: str,
        target_table_id: str,
        staging_table_id: str,
        primary_key: str,
        incremental_column: str,
        df: pd.DataFrame
    ) -> Tuple[str, Optional[str]]:
        """
        Merge data from staging table to target table.
        
        Args:
            project_id: BigQuery project ID
            target_dataset_id: Production dataset ID
            staging_dataset_id: Staging dataset ID
            target_table_id: Target table ID
            staging_table_id: Staging table ID
            primary_key: Primary key column for merge
            incremental_column: Column to check for updates
            df: DataFrame being merged (for empty check)
            
        Returns:
            Tuple of (success_message, error_message)
        """
        try:
            if df.empty:
                return "No new or updated rows", None
            
            target_ref = f"{project_id}.{target_dataset_id}.{target_table_id}"
            staging_ref = f"{project_id}.{staging_dataset_id}.{staging_table_id}"
            
            # Get schema
            target_schema = self.client.get_table(target_ref).schema
            cols = [f.name for f in target_schema]
            
            # Build update and insert clauses
            cols_without_pk = [c for c in cols if c != primary_key]
            update_clause = ", ".join([f"T.{c} = S.{c}" for c in cols_without_pk])
            insert_cols = ", ".join(cols)
            insert_vals = ", ".join([f"S.{c}" for c in cols])
            
            # Build merge SQL
            merge_sql = f"""
            MERGE `{target_ref}` T
            USING `{staging_ref}` S
            ON T.{primary_key} = S.{primary_key}
            WHEN MATCHED AND T.{incremental_column} < S.{incremental_column} THEN
              UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            
            self.client.query(merge_sql).result()
            
            # Truncate staging table
            self.client.query(f"TRUNCATE TABLE `{staging_ref}`").result()
            
            return "Merge completed successfully", None
            
        except Exception as e:
            return "", f"Merge failed: {str(e)}"
    
    def get_table_info(self, table_ref: str) -> Tuple[Optional[dict], Optional[str]]:
        """
        Get table information including row count and column count.
        
        Args:
            table_ref: Full BigQuery table reference
            
        Returns:
            Tuple of (info_dict, error_message)
        """
        try:
            table = self.client.get_table(table_ref)
            
            info = {
                'row_count': table.num_rows,
                'column_count': len(table.schema),
                'schema': table.schema
            }
            
            return info, None
            
        except Exception as e:
            return None, f"Failed to get table info: {str(e)}"
