"""
Local GeoPackage Data Source Implementation for SpatiaEngine
"""
from typing import Dict, Any, Optional, Union, List
import os
from pathlib import Path
import logging

from .base import VectorDataSource
from ..utils.error_handler import DataSourceError, handle_errors
from ..utils.file_utils import resolve_path

class LocalGpkgDataSource(VectorDataSource):
    """Local GeoPackage data source implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the local GPKG data source.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(config)
        self.gpkg_path = config.get('path')
        self.layer_name = config.get('layer_name')
        self.snrc_column_name = config.get('snrc_column_name')
        
        # Validate configuration
        if not self.gpkg_path or not self.layer_name:
            self.logger.warning(f"Config LocalGpkg '{self.name}' incomplete. Disabling.")
            self.enabled = False
    
    def validate_config(self) -> List[str]:
        """
        Validate the data source configuration.
        
        Returns:
            List of validation errors
        """
        errors = []
        
        if not self.config.get('path'):
            errors.append("Local GPKG data source requires 'path'")
        if not self.config.get('layer_name'):
            errors.append("Local GPKG data source requires 'layer_name'")
        
        # Check if file exists
        if self.config.get('path'):
            gpkg_path = resolve_path(self.config['path'])
            if not gpkg_path.exists():
                errors.append(f"Local GPKG file not found: {gpkg_path}")
        
        return errors
    
    @handle_errors(DataSourceError, default_return=None)
    def fetch_data(self, 
                   aoi_bounds_epsg4326: tuple, 
                   temp_dir: Union[str, Path]) -> Optional[str]:
        """
        Fetch data from the local GPKG source.
        
        Args:
            aoi_bounds_epsg4326: AOI bounds in EPSG:4326
            temp_dir: Temporary directory for output
            
        Returns:
            Path to filtered data file, or None if no data
        """
        if not self.is_enabled():
            return None
            
        if not self.gpkg_path or not self.layer_name:
            self.logger.error(f"Fetch LocalGpkg '{self.name}' cancelled (path/layer missing).")
            return None
        
        # Special case for AOI index layer
        if self.id == "snrc_index_local_50k":
            return "IS_AOI_INDEX_LAYER"
        
        self.logger.info(f"Fetching data (GPKG Simple): {self.name}")
        
        # Import here to avoid circular imports
        try:
            from ..processing.vector_processor import filter_local_gpkg
            temp_filepath = filter_local_gpkg(
                self.config, 
                aoi_bounds_epsg4326, 
                str(temp_dir)
            )
            
            if temp_filepath:
                self.add_temp_file(temp_filepath)
                self.logger.info(f"Data fetched successfully: {temp_filepath}")
            else:
                self.logger.info(f"No data found in AOI for {self.name}")
            
            return temp_filepath
        except Exception as e:
            self.logger.error(f"Error filtering local GPKG data: {e}")
            return None