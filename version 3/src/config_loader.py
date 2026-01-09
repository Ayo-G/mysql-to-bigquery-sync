"""
Configuration loader module for MySQL to BigQuery sync pipeline.
"""
import yaml
from typing import Dict, Any


class ConfigLoader:
    """Handles loading and parsing of configuration files."""
    
    def __init__(self, config_path: str = 'tables.yml'):
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing configuration file: {e}")
    
    @property
    def mysql_config(self) -> Dict[str, Any]:
        """Get MySQL configuration."""
        return self.config.get('mysql', {})
    
    @property
    def bigquery_config(self) -> Dict[str, Any]:
        """Get BigQuery configuration."""
        bq_config = self.config.get('bigquery', {})
        
        # If staging_dataset_id not specified, use dataset_id as fallback
        if 'staging_dataset_id' not in bq_config:
            bq_config['staging_dataset_id'] = bq_config.get('dataset_id')
        
        return bq_config
    
    @property
    def tables(self) -> list:
        """Get list of tables to sync."""
        return self.config.get('tables', [])
    
    @property
    def alert_recipients(self) -> list:
        """Get list of email recipients for alerts."""
        return self.bigquery_config.get('alert_recipients', [])
    
    def get_table_config(self, mysql_table: str) -> Dict[str, Any]:
        """Get configuration for a specific table."""
        for table in self.tables:
            if table['mysql_table'] == mysql_table:
                return table
        return None
