"""
MySQL data extraction module with SSH tunnel support.
"""
import pandas as pd
import pymysql
from sshtunnel import SSHTunnelForwarder
from datetime import datetime
from typing import Tuple, Optional, Dict, Any


class MySQLExtractor:
    """Handles MySQL data extraction via SSH tunnel."""
    
    def __init__(self, mysql_config: Dict[str, Any]):
        self.config = mysql_config
    
    def extract_incremental_data(
        self, 
        table_name: str, 
        incremental_column: str,
        last_synced: datetime
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        """
        Extract incremental data from MySQL table.
        
        Args:
            table_name: Name of the MySQL table
            incremental_column: Column to use for incremental sync
            last_synced: Timestamp of last successful sync
            
        Returns:
            Tuple of (DataFrame, error_message)
        """
        try:
            with SSHTunnelForwarder(
                (self.config['ssh_host'], self.config['ssh_port']),
                ssh_username=self.config['ssh_user'],
                ssh_pkey=self.config['ssh_private_key'],
                remote_bind_address=('127.0.0.1', self.config['db_port'])
            ) as tunnel:
                connection = pymysql.connect(
                    host='127.0.0.1',
                    port=tunnel.local_bind_port,
                    user=self.config['db_user'],
                    password=self.config['db_password'],
                    database=self.config['db_name'],
                    cursorclass=pymysql.cursors.DictCursor
                )
                
                with connection.cursor() as cursor:
                    query = f"SELECT * FROM {table_name} WHERE {incremental_column} > %s"
                    cursor.execute(query, (last_synced,))
                    result = cursor.fetchall()
                    df = pd.DataFrame(result)
                
                connection.close()
            
            return df, None
            
        except Exception as e:
            return pd.DataFrame(), f"MySQL extraction failed: {str(e)}"
    
    def get_table_schema(self, table_name: str) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Fetch MySQL table schema information.
        
        Args:
            table_name: Name of the MySQL table
            
        Returns:
            Tuple of (schema_dict, error_message)
            schema_dict format: {column_name: {'type': mysql_type, 'nullable': bool}}
        """
        try:
            with SSHTunnelForwarder(
                (self.config['ssh_host'], self.config['ssh_port']),
                ssh_username=self.config['ssh_user'],
                ssh_pkey=self.config['ssh_private_key'],
                remote_bind_address=('127.0.0.1', self.config['db_port'])
            ) as tunnel:
                connection = pymysql.connect(
                    host='127.0.0.1',
                    port=tunnel.local_bind_port,
                    user=self.config['db_user'],
                    password=self.config['db_password'],
                    database=self.config['db_name'],
                    cursorclass=pymysql.cursors.DictCursor
                )
                
                with connection.cursor() as cursor:
                    query = f"DESCRIBE {table_name}"
                    cursor.execute(query)
                    columns = cursor.fetchall()
                    
                    schema = {}
                    for col in columns:
                        schema[col['Field']] = {
                            'type': col['Type'],
                            'nullable': col['Null'] == 'YES',
                            'key': col['Key'],
                            'default': col['Default']
                        }
                
                connection.close()
            
            return schema, None
            
        except Exception as e:
            return None, f"Failed to fetch MySQL schema: {str(e)}"
