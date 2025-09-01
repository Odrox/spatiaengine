"""
Unified Configuration Management System for SpatiaEngine
"""
import os
import yaml
from typing import Dict, Any, Optional, List
from pathlib import Path
import logging

class ConfigManager:
    """Manages application configuration from YAML files and environment variables."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to the configuration file. Defaults to "config/sources.yaml"
        """
        self.config_path = config_path or "config/sources.yaml"
        self.config_data = {}
        self.logger = logging.getLogger('spatiaengine.config')
        self.load_config()
    
    def load_config(self) -> bool:
        """
        Load configuration from YAML file.
        
        Returns:
            bool: True if configuration loaded successfully, False otherwise
        """
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                # Align behavior with tests: raise FileNotFoundError when config is missing
                self.logger.error(f"Configuration file not found: {self.config_path}")
                raise FileNotFoundError(self.config_path)
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config_data = yaml.safe_load(f) or {}
            self.logger.info(f"Configuration loaded successfully from {self.config_path}")
            return True
        except FileNotFoundError:
            # Re-raise for callers/tests expecting this exception
            raise
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            return False
    
    def get_project_info(self) -> Dict[str, Any]:
        """
        Get project information configuration.
        
        Returns:
            Dict containing project information
        """
        return self.config_data.get('project_info', {})
    
    def get_projection_config(self) -> Dict[str, Any]:
        """
        Get projection configuration.
        
        Returns:
            Dict containing projection configuration
        """
        return self.config_data.get('projection', {})

    # Backward/compatibility alias expected by some tests
    def get_projection(self) -> Dict[str, Any]:
        """Alias for get_projection_config() for backward compatibility with tests."""
        return self.get_projection_config()
    
    def get_aoi_config(self) -> Dict[str, Any]:
        """
        Get Area of Interest configuration.
        
        Returns:
            Dict containing AOI configuration
        """
        return self.config_data.get('aoi_config', {})
    
    def get_datasources(self, type_filter: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get list of data source configurations, with optional filtering by type.
        Paths with environment variables are resolved.
        """
        datasources = self.config_data.get('datasources', [])
        
        resolved_datasources = []
        PATH_KEYS = ['path', 'index_gpkg_path', 'data_gpkg_path', 'local_cache_dir']
        for ds in datasources:
            resolved_ds = ds.copy()
            for key in PATH_KEYS:
                if key in resolved_ds and isinstance(resolved_ds[key], str):
                    resolved_ds[key] = self.resolve_path(resolved_ds[key])
            resolved_datasources.append(resolved_ds)

        if not type_filter:
            return resolved_datasources
        
        return [ds for ds in resolved_datasources if ds.get('type') in type_filter]
    
    def get_config_value(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation path.
        
        Args:
            key_path: Dot-separated path to configuration value (e.g., "project_info.id")
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key_path.split('.')
        value = self.config_data
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def resolve_path(self, path: str) -> str:
        """
        Resolve path with environment variable substitution.
        
        Args:
            path: Path string that may contain environment variables
            
        Returns:
            Resolved path string
        """
        # Replace environment variables in path
        resolved_path = os.path.expandvars(path)
        return resolved_path
    
    def get_data_path(self, relative_path: str = "") -> Path:
        """
        Get a data path resolved from environment variables.
        
        Args:
            relative_path: Relative path to append to base data path
            
        Returns:
            Resolved Path object
        """
        base_path = os.getenv('GIS_REFERENCE_PATH', '.')
        if relative_path:
            return Path(base_path) / relative_path
        return Path(base_path)