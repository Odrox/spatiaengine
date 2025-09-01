"""
MNT LiDAR Data Source Implementation for SpatiaEngine
"""
from typing import Dict, Any, Optional, Union, List
import os
import requests
from pathlib import Path
import logging
from tqdm import tqdm

from .base import RasterDataSource
from ..utils.error_handler import DataSourceError, handle_errors
from ..utils.file_utils import resolve_path

class MNTLiDARSource(RasterDataSource):
    """MNT LiDAR data source implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the MNT LiDAR data source.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(config)
        self.index_gpkg_path = config.get('index_gpkg_path')
        self.index_layer_name = config.get('index_layer_name')
        self.index_feuillet_column = config.get('index_feuillet_column')
        self.index_url_column = config.get('index_url_column')
        self.output_name_mnt = config.get('output_name_mnt', 'MNT_fusionne')
        self.output_name_hillshade = config.get('output_name_hillshade', 'Hillshade')
        # Optional persistent local cache directory (config or env)
        self.local_cache_dir = (config.get('local_cache_dir') or os.getenv('MNT_LOCAL_CACHE_DIR', '')).strip()
        
        # Validate configuration
        required_fields = ['index_gpkg_path', 'index_layer_name', 'index_feuillet_column', 'index_url_column']
        missing_fields = [field for field in required_fields if not self.config.get(field)]
        
        if missing_fields:
            self.logger.warning(f"Config MNTLiDAR '{self.name}' incomplete. Disabling.")
            self.enabled = False
            return
        
        # Check if index file exists
        index_path = resolve_path(self.index_gpkg_path)
        if not index_path.exists():
            self.logger.warning(f"Index GPKG file not found: {index_path}. Disabling.")
            self.enabled = False
    
    def validate_config(self) -> List[str]:
        """
        Validate the data source configuration.
        
        Returns:
            List of validation errors
        """
        errors = []
        
        required_fields = ['index_gpkg_path', 'index_layer_name', 'index_feuillet_column', 'index_url_column']
        for field in required_fields:
            if not self.config.get(field):
                errors.append(f"MNT LiDAR data source requires '{field}'")
        
        # Check if index file exists
        if self.config.get('index_gpkg_path'):
            index_path = resolve_path(self.config['index_gpkg_path'])
            if not index_path.exists():
                errors.append(f"Index GPKG file not found: {index_path}")
        
        return errors
    
    @handle_errors(DataSourceError, default_return=None)
    def fetch_data(self, 
                   aoi_object: Any, 
                   temp_dir: Union[str, Path]) -> Optional[List[str]]:
        """
        Fetch MNT LiDAR data from the source.
        
        Args:
            aoi_object: AOI object with subfeuillet data
            temp_dir: Temporary directory for output
            
        Returns:
            List of paths to downloaded TIF files, or None if no data
        """
        if not self.is_enabled():
            return None
        
        required_fields = ['index_gpkg_path', 'index_layer_name', 'index_feuillet_column', 'index_url_column']
        missing_fields = [field for field in required_fields if not getattr(self, field)]
        
        if missing_fields:
            self.logger.error(f"Fetch MNT '{self.name}' cancelled (missing index parameters: {missing_fields}).")
            return None
        
        # Check if AOI has subfeuillet data
        if not hasattr(aoi_object, 'subfeuillet_20k_data_gdfs') or not aoi_object.subfeuillet_20k_data_gdfs:
            self.logger.warning(f"Subfeuillet 1:20k data missing for AOI for MNT '{self.name}'.")
            return None
        
        self.logger.info(f"Fetching data (MNT LiDAR): {self.name} based on AOI subfeuillets.")
        # Optional local cache override
        local_cache_dir = self.local_cache_dir
        
        local_tif_paths = []
        
        num_tiles = len(aoi_object.subfeuillet_20k_data_gdfs)
        if num_tiles == 0:
            self.logger.info(f"No subfeuillets to download for {self.name} based on AOI.")
            return None
        
        self.logger.info(f"{num_tiles} subfeuillet(s) MNT to download.")
        
        for i, tile_gdf in enumerate(aoi_object.subfeuillet_20k_data_gdfs):
            try:
                feuillet_name = tile_gdf[self.index_feuillet_column].iloc[0]
                folder_url = tile_gdf[self.index_url_column].iloc[0]
            except Exception as e:
                self.logger.error(f"Missing info for subfeuillet MNT (index {i}): {e}. Skipping.")
                continue
            
            if not feuillet_name or not folder_url:
                self.logger.warning(f"Missing info (name/url) for subfeuillet MNT (index {i}). Skipping.")
                continue
            
            tif_filename = f"MNT_{feuillet_name}.tif"
            download_url = folder_url.rstrip('/') + f"/{tif_filename}"
            
            self.logger.info(f"Processing MNT subfeuillet ({i+1}/{num_tiles}): {feuillet_name}")
            temp_tif_path = os.path.join(str(temp_dir), tif_filename)
            
            try:
                # Use local cache if provided
                if local_cache_dir:
                    cached_path = os.path.join(local_cache_dir, tif_filename)
                    if os.path.exists(cached_path):
                        self.logger.info(f"Using cached MNT: {cached_path}")
                        local_tif_paths.append(cached_path)
                        continue
                self.logger.debug(f"Downloading: {download_url} -> {temp_tif_path}")
                response = requests.get(download_url, stream=True, timeout=300)
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                block_size = 8192
                
                with open(temp_tif_path, 'wb') as f, tqdm(
                    desc=f"  Downloading {tif_filename}",
                    total=total_size,
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024,
                    leave=False,
                    ncols=80
                ) as bar:
                    for chunk in response.iter_content(chunk_size=block_size):
                        f.write(chunk)
                        bar.update(len(chunk))
                
                if total_size != 0 and os.path.getsize(temp_tif_path) != total_size:
                    self.logger.error(f"Download incomplete {tif_filename}. Expected: {total_size}, Received: {os.path.getsize(temp_tif_path)}")
                    if os.path.exists(temp_tif_path):
                        try:
                            os.remove(temp_tif_path)
                            self.logger.debug(f"Partial file {temp_tif_path} deleted.")
                        except Exception as e_del:
                            self.logger.warning(f"Unable to delete partial file {temp_tif_path}: {e_del}")
                    continue
                
                self.logger.info(f"MNT .tif downloaded: {temp_tif_path}")
                local_tif_paths.append(temp_tif_path)
                self.add_temp_file(temp_tif_path)
                # Persist to cache if requested
                if local_cache_dir:
                    try:
                        Path(local_cache_dir).mkdir(parents=True, exist_ok=True)
                        cache_copy_path = os.path.join(local_cache_dir, tif_filename)
                        if not os.path.exists(cache_copy_path):
                            from shutil import copy2
                            copy2(temp_tif_path, cache_copy_path)
                            self.logger.debug(f"Cached MNT tile to {cache_copy_path}")
                    except Exception as e_cache:
                        self.logger.warning(f"Could not cache MNT tile {tif_filename}: {e_cache}")
                
            except Exception as e:
                self.logger.error(f"Error downloading MNT {feuillet_name}: {e}")
        
        if not local_tif_paths:
            self.logger.warning(f"No MNT .tif files downloaded for '{self.name}'.")
            return None
        
        self.logger.info(f"Successfully downloaded {len(local_tif_paths)} MNT files.")
        return local_tif_paths