"""
WFS (Web Feature Service) Data Source Implementation for SpatiaEngine
"""
from typing import Dict, Any, Optional, Union, List
from pathlib import Path
import logging

from .base import VectorDataSource
from ..utils.error_handler import DataSourceError, handle_errors

class WfsDataSource(VectorDataSource):
    """WFS data source implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the WFS data source.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(config)
        self.base_url = config.get('base_url')
        self.wfs_layer_name = config.get('layer_name')
        self.wfs_version = config.get('version', '2.0.0')
        self.output_format = config.get('output_format', 'application/json; subtype=geojson')
        self.srs_name = config.get('srs_name', 'EPSG:4326')
        self.extra_params = config.get('params', {})
        
        # Validate configuration
        if not self.base_url or not self.wfs_layer_name:
            self.logger.warning(f"Config WFS '{self.name}' incomplete. Disabling.")
            self.enabled = False
    
    def validate_config(self) -> List[str]:
        """
        Validate the data source configuration.
        
        Returns:
            List of validation errors
        """
        errors = []
        
        if not self.config.get('base_url'):
            errors.append("WFS data source requires 'base_url'")
        if not self.config.get('layer_name'):
            errors.append("WFS data source requires 'layer_name'")
        
        return errors
    
    @handle_errors(DataSourceError, default_return=None)
    def fetch_data(self, 
                   aoi_bounds_epsg4326: tuple, 
                   temp_dir: Union[str, Path]) -> Optional[str]:
        """
        Fetch data from the WFS source.
        
        Args:
            aoi_bounds_epsg4326: AOI bounds in EPSG:4326
            temp_dir: Temporary directory for output
            
        Returns:
            Path to downloaded data file, or None if no data
        """
        if not self.is_enabled():
            return None
            
        if not self.base_url or not self.wfs_layer_name:
            self.logger.error(f"Fetch WFS '{self.name}' cancelled (url/layer missing).")
            return None
        
        self.logger.info(f"Fetching data (WFS): {self.name}")
        
        # Import here to avoid circular imports
        try:
            from ..processing.vector_processor import download_wfs_data
            temp_filepath = download_wfs_data(
                self.config, 
                aoi_bounds_epsg4326, 
                str(temp_dir)
            )
            
            if temp_filepath:
                self.add_temp_file(temp_filepath)
                self.logger.info(f"WFS data downloaded successfully: {temp_filepath}")
            else:
                self.logger.info(f"No WFS data found in AOI for {self.name}")
            
            return temp_filepath
        except Exception as e:
            self.logger.error(f"Error downloading WFS data: {e}")
            return None