"""
Data Source Factory for Creating Data Source Instances in SpatiaEngine
"""
from typing import Dict, Any, Optional, List
import logging

from .base import DataSource
from .local_gpkg import LocalGpkgDataSource
from .wfs import WfsDataSource
from .indexed_gpkg import IndexedLocalGpkgDataSource
from .mnt_lidar import MNTLiDARSource
from .raster import LocalRasterDataSource
from .courbes_niveau import CourbesNiveauSource

from ..utils.error_handler import DataSourceError

class DataSourceFactory:
    """Factory for creating data source instances."""
    
    # Registry of available data source types
    _registry = {
        'local_gpkg': LocalGpkgDataSource,
        'wfs': WfsDataSource,
        'indexed_local_gpkg': IndexedLocalGpkgDataSource,
        'mnt_lidar_quebec': MNTLiDARSource,
        'local_raster': LocalRasterDataSource,
        'courbes_niveau_quebec': CourbesNiveauSource
    }
    
    def __init__(self):
        self.logger = logging.getLogger('spatiaengine.datasource.factory')
    
    @classmethod
    def register_datasource(cls, type_name: str, datasource_class) -> None:
        """
        Register a new data source type.
        
        Args:
            type_name: String identifier for the data source type
            datasource_class: Class implementing the data source
        """
        cls._registry[type_name] = datasource_class
        logging.getLogger('spatiaengine.datasource.factory').info(
            f"Registered data source type: {type_name}"
        )
    
    @classmethod
    def create_datasource(cls, config: Dict[str, Any]) -> Optional[DataSource]:
        """
        Create a data source instance from configuration.
        
        Args:
            config: Configuration dictionary for the data source
            
        Returns:
            DataSource instance or None if creation failed
        """
        ds_type = config.get('type')
        if not ds_type:
            logging.getLogger('spatiaengine.datasource.factory').error(
                "Data source configuration missing 'type' field"
            )
            return None
        
        if ds_type not in cls._registry:
            logging.getLogger('spatiaengine.datasource.factory').warning(
                f"Unknown data source type: {ds_type}"
            )
            return None
        
        datasource_class = cls._registry[ds_type]
        try:
            datasource = datasource_class(config)
            if datasource.is_enabled():
                logging.getLogger('spatiaengine.datasource.factory').debug(
                    f"Created data source: {datasource}"
                )
                return datasource
            else:
                logging.getLogger('spatiaengine.datasource.factory').warning(
                    f"Data source {config.get('id')} is disabled or invalid"
                )
                return None
        except Exception as e:
            logging.getLogger('spatiaengine.datasource.factory').error(
                f"Failed to create data source {config.get('id')}: {e}"
            )
            return None
    
    @classmethod
    def create_datasources_from_list(cls, configs: List[Dict[str, Any]]) -> List[DataSource]:
        """
        Create multiple data sources from a list of configurations.
        
        Args:
            configs: List of configuration dictionaries
            
        Returns:
            List of DataSource instances
        """
        datasources = []
        for config in configs:
            datasource = cls.create_datasource(config)
            if datasource:
                datasources.append(datasource)
        
        # Sort by priority
        datasources.sort()
        return datasources
    
    @classmethod
    def get_available_types(cls) -> List[str]:
        """
        Get list of available data source types.
        
        Returns:
            List of available data source type identifiers
        """
        return list(cls._registry.keys())
    
    @classmethod
    def is_type_available(cls, type_name: str) -> bool:
        """
        Check if a data source type is available.
        
        Args:
            type_name: Data source type identifier
            
        Returns:
            True if type is available, False otherwise
        """
        return type_name in cls._registry