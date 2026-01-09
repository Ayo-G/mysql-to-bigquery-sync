"""
Schema management module for MySQL to BigQuery type conversion and table creation.
"""
import pandas as pd
from google.cloud import bigquery
from typing import List, Tuple, Optional, Dict


class SchemaManager:
    """Handles schema conversion and management between MySQL and BigQuery."""
    
    # MySQL to BigQuery type mapping
    TYPE_MAPPING = {
        'tinyint': 'INT64',
        'smallint': 'INT64',
        'mediumint': 'INT64',
        'int': 'INT64',
        'bigint': 'INT64',
        'float': 'FLOAT64',
        'double': 'FLOAT64',
        'decimal': 'NUMERIC',
        'char': 'STRING',
        'varchar': 'STRING',
        'text': 'STRING',
        'tinytext': 'STRING',
        'mediumtext': 'STRING',
        'longtext': 'STRING',
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'timestamp': 'TIMESTAMP',
        'time': 'TIME',
        'year': 'INT64',
        'binary': 'BYTES',
        'varbinary': 'BYTES',
        'blob': 'BYTES',
        'tinyblob': 'BYTES',
        'mediumblob': 'BYTES',
        'longblob': 'BYTES',
        'enum': 'STRING',
        'set': 'STRING',
        'json': 'JSON',
        'bool': 'BOOL',
        'boolean': 'BOOL'
    }
    
    def __init__(self, client: bigquery.Client):
        self.client = client
    
    def mysql_type_to_bigquery(self, mysql_type: str) -> str:
        """
        Convert MySQL data type to BigQuery data type.
        
        Args:
            mysql_type: MySQL type string (e.g., 'int(11)', 'varchar(255)')
            
        Returns:
            BigQuery type string
        """
        # Extract base type (remove size specifications)
        base_type = mysql_type.split('(')[0].lower().strip()
        
        # Handle unsigned integers
        if 'unsigned' in mysql_type.lower():
            base_type = base_type.replace('unsigned', '').strip()
        
        return self.TYPE_MAPPING.get(base_type, 'STRING')
    
    def create_bigquery_schema_from_mysql(
        self, 
        mysql_schema: Dict[str, Dict]
    ) -> List[bigquery.SchemaField]:
        """
        Create BigQuery schema from MySQL schema.
        Always creates NULLABLE fields to avoid insertion errors.
        
        Args:
            mysql_schema: Dictionary with column info from MySQL
            
        Returns:
            List of BigQuery SchemaField objects
        """
        bq_schema = []
        
        for col_name, col_info in mysql_schema.items():
            bq_type = self.mysql_type_to_bigquery(col_info['type'])
            
            # Always use NULLABLE mode to prevent insertion errors
            # Even if MySQL shows NOT NULL, there might be existing NULL values
            # BigQuery constraints should be managed separately if needed
            mode = 'NULLABLE'
            
            bq_schema.append(
                bigquery.SchemaField(col_name, bq_type, mode=mode)
            )
        
        return bq_schema
    
    def create_table_from_mysql_schema(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        mysql_schema: Dict[str, Dict]
    ) -> Tuple[bool, Optional[str]]:
        """
        Create BigQuery table using MySQL schema.
        
        Args:
            project_id: BigQuery project ID
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            mysql_schema: MySQL schema dictionary
            
        Returns:
            Tuple of (success, error_message)
        """
        try:
            table_ref = f"{project_id}.{dataset_id}.{table_id}"
            
            # Check if table already exists
            try:
                self.client.get_table(table_ref)
                return True, f"Table {table_id} already exists"
            except Exception:
                pass
            
            # Create schema
            bq_schema = self.create_bigquery_schema_from_mysql(mysql_schema)
            
            # Create table
            table = bigquery.Table(table_ref, schema=bq_schema)
            self.client.create_table(table)
            
            return True, None
            
        except Exception as e:
            return False, f"Failed to create table: {str(e)}"
    
    def add_missing_columns(
        self,
        table_ref: str,
        mysql_schema: Dict[str, Dict]
    ) -> Tuple[List[str], Optional[str]]:
        """
        Add missing columns to BigQuery table based on MySQL schema.
        All new columns are created as NULLABLE.
        
        Args:
            table_ref: Full BigQuery table reference
            mysql_schema: MySQL schema dictionary
            
        Returns:
            Tuple of (list of added columns, error_message)
        """
        try:
            table = self.client.get_table(table_ref)
            existing_schema = list(table.schema)
            existing_cols = [f.name for f in existing_schema]
            
            new_columns = []
            
            for col_name, col_info in mysql_schema.items():
                if col_name not in existing_cols:
                    bq_type = self.mysql_type_to_bigquery(col_info['type'])
                    
                    # Always use NULLABLE for new columns
                    mode = 'NULLABLE'
                    
                    new_field = bigquery.SchemaField(col_name, bq_type, mode=mode)
                    existing_schema.append(new_field)
                    new_columns.append(col_name)
            
            if new_columns:
                table.schema = existing_schema
                self.client.update_table(table, ["schema"])
            
            return new_columns, None
            
        except Exception as e:
            return [], f"Failed to add columns: {str(e)}"
    
    def prepare_dataframe_with_schema(
        self, 
        df: pd.DataFrame, 
        mysql_schema: Dict[str, Dict]
    ) -> pd.DataFrame:
        """
        Prepare DataFrame using MySQL schema as the source of truth.
        Converts types to be compatible with BigQuery/PyArrow.
        
        Args:
            df: Input DataFrame from MySQL
            mysql_schema: MySQL schema dictionary
            
        Returns:
            Prepared DataFrame
        """
        if df.empty:
            return df
        
        for col in df.columns:
            if col not in mysql_schema:
                continue
            
            mysql_type = mysql_schema[col]['type'].lower()
            
            # Handle integer types - convert to nullable Int64 to avoid PyArrow issues
            if any(int_type in mysql_type for int_type in ['int', 'bigint', 'smallint', 'tinyint', 'mediumint']):
                # Clean string values that might appear in integer columns
                if df[col].dtype == 'object':
                    df[col] = df[col].replace(['', ' ', 'NULL', 'null', 'None'], None)
                # Convert to pandas nullable integer type
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].astype('Int64')
            
            # Handle float/double types
            elif any(float_type in mysql_type for float_type in ['float', 'double', 'decimal']):
                # Clean string values that might appear in float columns
                if df[col].dtype == 'object':
                    df[col] = df[col].replace(['', ' ', 'NULL', 'null', 'None'], None)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Ensure float64 dtype
                df[col] = df[col].astype('float64')
            
            # Handle datetime/timestamp columns - remove timezone for BigQuery
            elif any(dt in mysql_type for dt in ['datetime', 'timestamp']):
                df[col] = pd.to_datetime(df[col], errors='coerce')
                # Remove timezone if present
                if hasattr(df[col].dtype, 'tz') and df[col].dtype.tz is not None:
                    df[col] = df[col].dt.tz_localize(None)
            
            # Handle date columns
            elif 'date' in mysql_type and 'datetime' not in mysql_type:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            
            # Handle boolean types
            elif any(bool_type in mysql_type for bool_type in ['bool', 'boolean', 'tinyint(1)']):
                df[col] = df[col].astype('boolean')
        
        return df