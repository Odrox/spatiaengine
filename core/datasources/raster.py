"""
Local Raster Data Source Implementation for SpatiaEngine
"""
from typing import Dict, Any, Optional, Union, List
import os
from pathlib import Path
import logging

from .base import RasterDataSource
from ..utils.error_handler import DataSourceError, handle_errors
from ..utils.file_utils import resolve_path

class LocalRasterDataSource(RasterDataSource):
    """Local raster data source implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the local raster data source.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(config)
        self.raster_path = config.get('path')
        self.output_name_raster = config.get('output_name_raster', self.id)
        
        # Validate configuration
        if not self.raster_path:
            self.logger.warning(f"Config 'path' missing for LocalRaster '{self.name}'. Disabling.")
            self.enabled = False
            return
        
        # Check if file exists
        raster_path = resolve_path(self.raster_path)
        if not raster_path.exists():
            self.logger.warning(f"Raster file not found: {raster_path}. Disabling.")
            self.enabled = False
    
    def validate_config(self) -> List[str]:
        """
        Validate the data source configuration.
        
        Returns:
            List of validation errors
        """
        errors = []
        
        if not self.config.get('path'):
            errors.append("Local raster data source requires 'path'")
        
        # Check if file exists
        if self.config.get('path'):
            raster_path = resolve_path(self.config['path'])
            if not raster_path.exists():
                errors.append(f"Raster file not found: {raster_path}")
        
        return errors
    
    @handle_errors(DataSourceError, default_return=None)
    def fetch_data(self, 
                   aoi_object_or_bounds: Any, 
                   temp_dir: Union[str, Path]) -> Optional[str]:
        """
        Fetch data from the local raster source.
        
        Args:
            aoi_object_or_bounds: AOI object or bounds
            temp_dir: Temporary directory for output (not used for local raster)
            
        Returns:
            Path to raster file, or None if no data
        """
        if not self.is_enabled() or not self.raster_path:
            return None
        
        raster_path = resolve_path(self.raster_path)
        if not raster_path.exists():
            self.logger.error(f"Raster file not found: {raster_path}")
            return None
        
        self.logger.info(f"Using local raster: {self.raster_path} for '{self.name}'")
        return str(raster_path)