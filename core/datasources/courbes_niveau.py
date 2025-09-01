"""
Courbes Niveau Data Source Implementation for SpatiaEngine
"""
from typing import Dict, Any, Optional, Union, List
import os
import requests
from pathlib import Path
import logging
import geopandas as gpd
import pandas as pd
from tqdm import tqdm
import fiona

from .base import VectorDataSource
from ..utils.error_handler import DataSourceError, handle_errors
from ..utils.file_utils import resolve_path

class CourbesNiveauSource(VectorDataSource):
    """Courbes Niveau data source implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Courbes Niveau data source.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(config)
        self.index_gpkg_path = config.get('index_gpkg_path')
        self.index_layer_name = config.get('index_layer_name')
        self.index_feuillet_column = config.get('index_feuillet_column')
        self.index_url_column = config.get('index_url_column')
        self.data_gpkg_internal_layer = config.get('data_gpkg_internal_layer')
        
        # Validate configuration
        required_fields = ['index_gpkg_path', 'index_layer_name', 'index_feuillet_column', 'index_url_column']
        missing_fields = [field for field in required_fields if not self.config.get(field)]
        
        if missing_fields:
            self.logger.warning(f"Config CourbesNiveau '{self.name}' incomplete. Disabling.")
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
                errors.append(f"Courbes Niveau data source requires '{field}'")
        
        # Check if index file exists
        if self.config.get('index_gpkg_path'):
            index_path = resolve_path(self.config['index_gpkg_path'])
            if not index_path.exists():
                errors.append(f"Index GPKG file not found: {index_path}")
        
        return errors
    
    @handle_errors(DataSourceError, default_return=None)
    def fetch_data(self, 
                   aoi_object: Any, 
                   temp_dir: Union[str, Path]) -> Optional[str]:
        """
        Fetch Courbes Niveau data from the source.
        
        Args:
            aoi_object: AOI object with subfeuillet data
            temp_dir: Temporary directory for output
            
        Returns:
            Path to combined GeoJSON file, or None if no data
        """
        if not self.is_enabled():
            return None
        
        required_fields = ['index_gpkg_path', 'index_layer_name', 'index_feuillet_column', 'index_url_column']
        missing_fields = [field for field in required_fields if not getattr(self, field)]
        
        if missing_fields:
            self.logger.error(f"Fetch Courbes '{self.name}' cancelled (missing index parameters: {missing_fields}).")
            return None
        
        # Check if AOI has subfeuillet data
        if not hasattr(aoi_object, 'subfeuillet_20k_data_gdfs') or not aoi_object.subfeuillet_20k_data_gdfs:
            self.logger.warning(f"Subfeuillet 1:20k data missing for AOI for Courbes '{self.name}'.")
            return None
        
        self.logger.info(f"Fetching data (Courbes Niveau): {self.name} based on AOI subfeuillets.")
        
        downloaded_gpkg_paths = []
        all_gdfs_courbes = []
        
        num_tiles = len(aoi_object.subfeuillet_20k_data_gdfs)
        if num_tiles == 0:
            self.logger.info(f"No subfeuillets for courbes '{self.name}'.")
            return None
        
        self.logger.info(f"{num_tiles} subfeuillet(s) Courbes to process.")
        
        for i, tile_gdf in enumerate(aoi_object.subfeuillet_20k_data_gdfs):
            try:
                feuillet_name = tile_gdf[self.index_feuillet_column].iloc[0]
                folder_url = tile_gdf[self.index_url_column].iloc[0]
            except Exception as e:
                self.logger.error(f"Missing info for subfeuillet Courbes (index {i}): {e}. Skipping.")
                continue
            
            if not feuillet_name or not folder_url:
                self.logger.warning(f"Missing info (name/url) for subfeuillet Courbes (index {i}). Skipping.")
                continue
            
            gpkg_filename = f"Courbes_{feuillet_name}.gpkg"
            download_url = folder_url.rstrip('/') + f"/{gpkg_filename}"
            temp_gpkg_path = os.path.join(str(temp_dir), gpkg_filename)
            
            self.logger.info(f"Processing subfeuillet Courbes ({i+1}/{num_tiles}): {feuillet_name}")
            
            try:
                response = requests.get(download_url, stream=True, timeout=300)
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                with open(temp_gpkg_path, 'wb') as f, tqdm(
                    desc=f"  Downloading {gpkg_filename}",
                    total=total_size,
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024,
                    leave=False,
                    ncols=80
                ) as bar:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        bar.update(len(chunk))
                
                # Check download completeness
                if total_size != 0 and os.path.getsize(temp_gpkg_path) != total_size:
                    self.logger.error(f"Download incomplete {gpkg_filename}. Expected: {total_size}, Received: {os.path.getsize(temp_gpkg_path)}")
                    if os.path.exists(temp_gpkg_path):
                        try:
                            os.remove(temp_gpkg_path)
                            self.logger.debug(f"Partial file {temp_gpkg_path} deleted.")
                        except Exception as e_del_part_courbes:
                            self.logger.warning(f"Unable to delete partial courbes file {temp_gpkg_path}: {e_del_part_courbes}")
                    continue
                
                self.logger.info(f"Courbes .gpkg downloaded: {temp_gpkg_path}")
                downloaded_gpkg_paths.append(temp_gpkg_path)
                self.add_temp_file(temp_gpkg_path)
                
                # Read the GPKG file
                try:
                    couche_a_lire = self.data_gpkg_internal_layer
                    if not couche_a_lire:
                        layers_in_gpkg = fiona.listlayers(temp_gpkg_path)
                        if layers_in_gpkg:
                            couche_a_lire = layers_in_gpkg[0]
                        else:
                            self.logger.error(f"No layers found in {temp_gpkg_path}.")
                            continue
                    
                    self.logger.info(f"Reading layer '{couche_a_lire}' from {temp_gpkg_path}...")
                    gdf_courbes_tile = gpd.read_file(temp_gpkg_path, layer=couche_a_lire)
                    
                    if not gdf_courbes_tile.empty:
                        all_gdfs_courbes.append(gdf_courbes_tile)
                        self.logger.info(f"  {len(gdf_courbes_tile)} curve features read.")
                    else:
                        self.logger.info(f"  No features in layer '{couche_a_lire}'.")
                except Exception as e_read_gpkg:
                    self.logger.error(f"Error reading GPKG courbes {temp_gpkg_path}: {e_read_gpkg}")
                    
            except Exception as e:
                self.logger.error(f"Error downloading/reading GPKG courbes {feuillet_name}: {e}")
        
        if not all_gdfs_courbes:
            self.logger.warning(f"No courbes data collected for '{self.name}'.")
            return None
        
        self.logger.info(f"Concatenating courbes from {len(all_gdfs_courbes)} file(s)...")
        
        # Determine final CRS
        final_crs_courbes = None
        for gdf_c in all_gdfs_courbes:
            if gdf_c.crs:
                final_crs_courbes = gdf_c.crs
                break
        
        if final_crs_courbes is None:
            final_crs_courbes = "EPSG:32198"  # Fallback
            self.logger.warning(f"Using fallback CRS: {final_crs_courbes}")
        
        # Concatenate all data
        try:
            gdf_combined_courbes = gpd.GeoDataFrame(
                pd.concat(all_gdfs_courbes, ignore_index=True, join='outer'), 
                crs=final_crs_courbes
            )
        except Exception as e_concat_courbes:
            self.logger.error(f"Error concatenating courbes GDFs: {e_concat_courbes}")
            return None
        
        self.logger.info(f"Total {len(gdf_combined_courbes)} curve features combined.")
        
        # Save to temporary file
        tmp_file = f"temp_merged_{self.id}.geojson"
        temp_filepath = os.path.join(str(temp_dir), tmp_file)
        
        try:
            gdf_combined_courbes.to_file(temp_filepath, driver="GeoJSON")
            self.add_temp_file(temp_filepath)
            self.logger.info(f"Combined courbes saved to: {temp_filepath}")
            
            # Clean up downloaded GPKG files
            for gpkg_p in downloaded_gpkg_paths:
                if os.path.exists(gpkg_p):
                    try:
                        os.remove(gpkg_p)
                        self.logger.debug(f"Temporary GPKG file {gpkg_p} deleted.")
                    except Exception as e_del:
                        self.logger.warning(f"Unable to delete temporary GPKG {gpkg_p}: {e_del}")
            
            return temp_filepath
        except Exception as e_save_geojson:
            self.logger.error(f"Error saving courbes GeoJSON {temp_filepath}: {e_save_geojson}")
            return None
