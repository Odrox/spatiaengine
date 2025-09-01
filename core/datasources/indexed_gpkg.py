"""
Indexed Local GeoPackage Data Source Implementation for SpatiaEngine
"""
from typing import Dict, Any, Optional, Union, List
import os
from pathlib import Path
import logging
import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from .base import VectorDataSource
from ..utils.error_handler import DataSourceError, handle_errors
from ..utils.file_utils import resolve_path

class IndexedLocalGpkgDataSource(VectorDataSource):
    """Indexed Local GeoPackage data source implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the indexed local GPKG data source.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(config)
        self.data_gpkg_path = config.get('data_gpkg_path')
        self.index_gpkg_path = config.get('index_gpkg_path')
        self.index_layer_name = config.get('index_layer_name')
        self.index_block_column = config.get('index_block_column')
        self.data_table_prefix = config.get('data_table_prefix', '')
        
        # Validate configuration
        required_fields = ['data_gpkg_path', 'index_gpkg_path', 'index_layer_name', 'index_block_column']
        missing_fields = [field for field in required_fields if not self.config.get(field)]
        
        if missing_fields:
            self.logger.warning(f"Config IndexedGPKG '{self.name}' incomplete. Disabling.")
            self.enabled = False
            return
        
        # Check if files exist
        data_path = resolve_path(self.data_gpkg_path)
        index_path = resolve_path(self.index_gpkg_path)
        
        if not data_path.exists():
            self.logger.warning(f"Data GPKG file not found: {data_path}. Disabling.")
            self.enabled = False
        elif not index_path.exists():
            self.logger.warning(f"Index GPKG file not found: {index_path}. Disabling.")
            self.enabled = False
    
    def validate_config(self) -> List[str]:
        """
        Validate the data source configuration.
        
        Returns:
            List of validation errors
        """
        errors = []
        
        required_fields = ['data_gpkg_path', 'index_gpkg_path', 'index_layer_name', 'index_block_column']
        for field in required_fields:
            if not self.config.get(field):
                errors.append(f"Indexed Local GPKG data source requires '{field}'")
        
        # Check if files exist
        if self.config.get('data_gpkg_path'):
            data_path = resolve_path(self.config['data_gpkg_path'])
            if not data_path.exists():
                errors.append(f"Data GPKG file not found: {data_path}")
        
        if self.config.get('index_gpkg_path'):
            index_path = resolve_path(self.config['index_gpkg_path'])
            if not index_path.exists():
                errors.append(f"Index GPKG file not found: {index_path}")
        
        return errors
    
    @handle_errors(DataSourceError, default_return=None)
    def fetch_data(self, 
                   aoi_bounds_epsg4326: tuple, 
                   temp_dir: Union[str, Path]) -> Optional[str]:
        """
        Fetch data from the indexed local GPKG source.
        
        Args:
            aoi_bounds_epsg4326: AOI bounds in EPSG:4326
            temp_dir: Temporary directory for output
            
        Returns:
            Path to combined data file, or None if no data
        """
        if not self.is_enabled():
            self.logger.debug(f"Fetch ignored for '{self.name}' (disabled).")
            return None
            
        required_fields = ['data_gpkg_path', 'index_gpkg_path', 'index_layer_name', 'index_block_column']
        missing_fields = [field for field in required_fields if not getattr(self, field)]
        
        if missing_fields:
            self.logger.error(f"Fetch cancelled for '{self.name}' (missing parameters: {missing_fields}).")
            return None
        
        self.logger.info(f"Fetching data (GPKG Indexed): {self.name}")
        self.logger.debug(f"Index: {self.index_gpkg_path} -> '{self.index_layer_name}' (col: {self.index_block_column})")
        self.logger.debug(f"Data GPKG: {self.data_gpkg_path}")
        
        try:
            # Step 1: Read index
            self.logger.debug("Step 1: Reading index...")
            gdf_index = gpd.read_file(self.index_gpkg_path, layer=self.index_layer_name)
            
            if gdf_index.crs is None:
                self.logger.warning(f"Index CRS '{self.index_layer_name}' not defined. Assuming EPSG:32198.")
                gdf_index = gdf_index.set_crs("EPSG:32198")
            
            index_crs = gdf_index.crs
            if not index_crs:
                self.logger.error(f"Unable to determine CRS for index '{self.index_layer_name}'.")
                return None
            
            self.logger.info(f"Index loaded ({len(gdf_index)} blocks). CRS: {index_crs}")
            
            # Step 2: Prepare AOI for intersection
            self.logger.debug("Step 2: Preparing AOI for intersection...")
            aoi_geom_4326 = box(*aoi_bounds_epsg4326)
            aoi_gdf_4326 = gpd.GeoDataFrame(geometry=[aoi_geom_4326], crs="EPSG:4326")
            aoi_geom_idx_crs = aoi_gdf_4326.to_crs(index_crs).geometry.iloc[0]
            self.logger.info(f"AOI ready for intersection (CRS: {index_crs}).")
            
            # Step 3: Find intersecting blocks
            self.logger.debug("Step 3: Finding intersecting blocks...")
            intersect_blocks = gdf_index[gdf_index.intersects(aoi_geom_idx_crs)]
            
            if intersect_blocks.empty:
                self.logger.info(f"No intersecting blocks found for '{self.name}'.")
                return None
            
            # Step 4: Extract table names
            self.logger.debug("Step 4: Extracting table names...")
            if self.index_block_column not in intersect_blocks.columns:
                self.logger.error(f"Column '{self.index_block_column}' not found in index '{self.index_layer_name}'.")
                return None
            
            block_ids = intersect_blocks[self.index_block_column].dropna().unique()
            table_names = [f"{self.data_table_prefix}{name}" for name in block_ids if name]
            
            if not table_names:
                self.logger.warning(f"No valid table names for '{self.name}'.")
                return None
            
            self.logger.info(f"{len(table_names)} table(s) to read: {', '.join(table_names)}")
            
            # Step 5: Read/merge table data
            self.logger.debug("Step 5: Reading/merging table data...")
            all_gdfs = []
            data_crs = "EPSG:32198"
            self.logger.info(f"Assumed data CRS: {data_crs}")
            
            bbox_data_crs = tuple(aoi_gdf_4326.to_crs(data_crs).total_bounds)
            self.logger.info(f"Using BBOX {bbox_data_crs} (CRS: {data_crs}) to read tables.")
            
            for name in table_names:
                self.logger.info(f"Reading table: '{name}'...")
                try:
                    gdf_tbl = gpd.read_file(self.data_gpkg_path, layer=name, bbox=bbox_data_crs)
                    if not gdf_tbl.empty:
                        if gdf_tbl.crs is None:
                            gdf_tbl = gdf_tbl.set_crs(data_crs)
                        elif gdf_tbl.crs.to_string().upper() != data_crs.upper():
                            gdf_tbl = gdf_tbl.to_crs(data_crs)
                        all_gdfs.append(gdf_tbl)
                        self.logger.info(f"  {len(gdf_tbl)} features read from '{name}'.")
                    else:
                        self.logger.info(f"  Table '{name}' empty after BBOX filter.")
                except Exception as e:
                    self.logger.error(f"Error reading table '{name}': {e}")
            
            if not all_gdfs:
                self.logger.info(f"No data in tables for '{self.name}'.")
                return None
            
            # Step 6: Concatenate data
            self.logger.info(f"Concatenating {len(all_gdfs)} table(s)...")
            try:
                gdf_combined = gpd.GeoDataFrame(pd.concat(all_gdfs, ignore_index=True, join='outer'), crs=data_crs)
            except Exception as e:
                self.logger.error(f"Error concatenating GDFs: {e}")
                return None
            
            self.logger.info(f"Total {len(gdf_combined)} features combined for '{self.name}'.")
            
            # Step 7: Save temporary file
            self.logger.debug("Step 7: Saving temporary file...")
            tmp_file = f"temp_indexed_{self.id}.geojson"
            temp_filepath = os.path.join(str(temp_dir), tmp_file)
            
            try:
                gdf_combined.to_file(temp_filepath, driver="GeoJSON")
                self.add_temp_file(temp_filepath)
                self.logger.info(f"Indexed data saved to: {temp_filepath}")
                return temp_filepath
            except Exception as e:
                self.logger.error(f"Error saving GeoJSON: {e}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error fetching IndexedGPKG '{self.name}': {e}")
            return None