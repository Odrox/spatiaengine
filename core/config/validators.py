"""
Configuration Validation Utilities for SpatiaEngine
"""
import re
from typing import Dict, Any, List
import logging

class ConfigValidator:
    """Validates configuration data integrity."""
    
    def __init__(self):
        self.logger = logging.getLogger('spatiaengine.config.validator')
    
    @staticmethod
    def validate_project_info(project_info: Dict[str, Any]) -> List[str]:
        """
        Validate project information configuration.
        
        Args:
            project_info: Project information dictionary
            
        Returns:
            List of validation errors
        """
        errors = []
        if not project_info.get('id'):
            errors.append("Project ID is required")
        return errors
    
    @staticmethod
    def validate_aoi_config(aoi_config: Dict[str, Any]) -> List[str]:
        """
        Validate Area of Interest configuration.
        
        Args:
            aoi_config: AOI configuration dictionary
            
        Returns:
            List of validation errors
        """
        errors = []
        if not aoi_config.get('type'):
            errors.append("AOI type is required")
        if not aoi_config.get('definition'):
            errors.append("AOI definition is required")
        return errors
    
    @staticmethod
    def validate_datasource_config(datasource: Dict[str, Any]) -> List[str]:
        """
        Validate individual data source configuration.
        
        Args:
            datasource: Data source configuration dictionary
            
        Returns:
            List of validation errors
        """
        errors = []
        required_fields = ['id', 'name', 'type']
        for field in required_fields:
            if not datasource.get(field):
                errors.append(f"Data source missing required field: {field}")
        
        # Validate specific data source types
        ds_type = datasource.get('type', '')
        if ds_type == 'local_gpkg':
            if not datasource.get('path'):
                errors.append("Local GPKG data source requires 'path'")
            if not datasource.get('layer_name'):
                errors.append("Local GPKG data source requires 'layer_name'")
        elif ds_type == 'wfs':
            if not datasource.get('base_url'):
                errors.append("WFS data source requires 'base_url'")
            if not datasource.get('layer_name'):
                errors.append("WFS data source requires 'layer_name'")
        elif ds_type == 'indexed_local_gpkg':
            required_indexed_fields = ['data_gpkg_path', 'index_gpkg_path', 'index_layer_name', 'index_block_column']
            for field in required_indexed_fields:
                if not datasource.get(field):
                    errors.append(f"Indexed Local GPKG data source requires '{field}'")
        elif ds_type == 'mnt_lidar_quebec':
            required_mnt_fields = ['index_gpkg_path', 'index_layer_name', 'index_feuillet_column', 'index_url_column']
            for field in required_mnt_fields:
                if not datasource.get(field):
                    errors.append(f"MNT LiDAR data source requires '{field}'")
        elif ds_type == 'local_raster':
            if not datasource.get('path'):
                errors.append("Local raster data source requires 'path'")
        
        return errors
    
    @staticmethod
    def validate_crs_code(crs_code: str) -> bool:
        """
        Validate CRS code format.
        
        Args:
            crs_code: CRS code string to validate
            
        Returns:
            bool: True if valid CRS code, False otherwise
        """
        if not crs_code:
            return False
        # Check if it's a valid EPSG code
        return bool(re.match(r'^EPSG:\d+$', crs_code))
    
    @staticmethod
    def validate_path(path: str) -> bool:
        """
        Validate path format (basic validation).
        
        Args:
            path: Path string to validate
            
        Returns:
            bool: True if valid path format, False otherwise
        """
        if not path:
            return False
        # Basic path validation - check for invalid characters
        invalid_chars = ['<', '>', '|', '"', '?', '*']
        return not any(char in path for char in invalid_chars)
    
    def validate_full_config(self, config_data: Dict[str, Any]) -> List[str]:
        """
        Validate the complete configuration.
        
        Args:
            config_data: Complete configuration dictionary
            
        Returns:
            List of validation errors
        """
        errors = []
        
        # Validate project info
        project_info = config_data.get('project_info', {})
        errors.extend(self.validate_project_info(project_info))
        
        # Validate AOI config
        aoi_config = config_data.get('aoi_config', {})
        errors.extend(self.validate_aoi_config(aoi_config))
        
        # Validate projection config
        projection_config = config_data.get('projection', {})
        if projection_config.get('target_crs'):
            if not self.validate_crs_code(projection_config['target_crs']):
                errors.append(f"Invalid target CRS code: {projection_config['target_crs']}")
        
        # Validate data sources
        datasources = config_data.get('datasources', [])
        for i, datasource in enumerate(datasources):
            ds_errors = self.validate_datasource_config(datasource)
            for error in ds_errors:
                errors.append(f"Data source #{i} ({datasource.get('id', 'unknown')}): {error}")
        
        return errors