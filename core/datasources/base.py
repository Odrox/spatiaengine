"""
Abstract Base Classes for Data Sources in SpatiaEngine
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union, List
import logging
from pathlib import Path

from ..utils.error_handler import DataSourceError

class DataSource(ABC):
    """Abstract base class for all data sources."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the data source.
        
        Args:
            config: Configuration dictionary for the data source
        """
        self.id = config.get('id', 'unknown')
        self.name = config.get('name', 'Unknown Data Source')
        self.type = config.get('type', 'unknown')
        self.enabled = config.get('enabled', True)
        self.priority = config.get('priority_level', 99)
        self.output_layer_name = config.get('output_layer_name', self.id)
        self.config = config
        self.logger = logging.getLogger(f'spatiaengine.datasource.{self.id}')
        self.temp_files: List[str] = []
        
        # Validate configuration
        validation_errors = self.validate_config()
        if validation_errors:
            self.logger.warning(f"Configuration validation errors for {self.name}: {validation_errors}")
            self.enabled = False
    
    @abstractmethod
    def fetch_data(self, 
                   aoi_context: Any, 
                   temp_dir: Union[str, Path]) -> Union[str, List[str], None]:
        """
        Fetch data from the source.
        
        Args:
            aoi_context: Area of Interest context (geometry or bounds)
            temp_dir: Temporary directory for storing intermediate files
            
        Returns:
            Path to fetched data file(s), or None if no data
        """
        pass
    
    @abstractmethod
    def validate_config(self) -> List[str]:
        """
        Validate the data source configuration.
        
        Returns:
            List of validation errors
        """
        pass
    
    def is_enabled(self) -> bool:
        """
        Check if the data source is enabled and properly configured.
        
        Returns:
            bool: True if enabled and valid, False otherwise
        """
        if not self.enabled:
            return False
        return len(self.validate_config()) == 0
    
    def cleanup_temp_files(self) -> None:
        """Clean up temporary files created by this data source."""
        from ..utils.file_utils import safe_delete_file
        for temp_file in self.temp_files:
            safe_delete_file(temp_file)
        self.temp_files.clear()
    
    def add_temp_file(self, filepath: str) -> None:
        """
        Add a temporary file to the cleanup list.
        
        Args:
            filepath: Path to temporary file
        """
        self.temp_files.append(filepath)
    
    def __lt__(self, other):
        """Enable sorting by priority."""
        if not isinstance(other, DataSource):
            return NotImplemented
        return self.priority < other.priority
    
    def __repr__(self):
        return f"<{self.__class__.__name__}(id='{self.id}', name='{self.name}', type='{self.type}')>"
    
    def __str__(self):
        return f"{self.name} ({self.type})"

class VectorDataSource(DataSource):
    """Base class for vector data sources."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.layer_name = config.get('layer_name', '')
    
    @abstractmethod
    def fetch_data(self, 
                   aoi_context: Any, 
                   temp_dir: Union[str, Path]) -> Optional[str]:
        """
        Fetch vector data from the source.
        
        Args:
            aoi_context: Area of Interest context
            temp_dir: Temporary directory for storing intermediate files
            
        Returns:
            Path to fetched vector data file, or None if no data
        """
        pass

class RasterDataSource(DataSource):
    """Base class for raster data sources."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.output_name = config.get('output_name', self.id)
    
    @abstractmethod
    def fetch_data(self, 
                   aoi_context: Any, 
                   temp_dir: Union[str, Path]) -> Union[str, List[str], None]:
        """
        Fetch raster data from the source.
        
        Args:
            aoi_context: Area of Interest context
            temp_dir: Temporary directory for storing intermediate files
            
        Returns:
            Path to fetched raster data file(s), or None if no data
        """
        pass