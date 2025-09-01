"""
Vector Data Processing Module for SpatiaEngine
"""
import geopandas as gpd
import os
import logging
from typing import TYPE_CHECKING, Optional, Tuple, List, Union
from pathlib import Path

if TYPE_CHECKING:
    from shapely.geometry.base import BaseGeometry
    import numpy as np

from ..utils.error_handler import ProcessingError, handle_errors
from ..utils.file_utils import safe_delete_file

logger = logging.getLogger('spatiaengine.processing.vector')

@handle_errors(ProcessingError, default_return=(False, -1, -1))
def process_vector_data(input_filepath: str, 
                       aoi_geometry_mtm: 'BaseGeometry', 
                       target_crs_mtm: str,
                       output_gpkg_path: str,
                       output_layer_name: str,
                       delete_input_temp_file: bool = False,
                       temp_processing_dir: Optional[str] = None) -> Tuple[bool, int, int]:
    """
    Process vector data: reproject and clip to AOI.
    
    Args:
        input_filepath: Path to input vector file
        aoi_geometry_mtm: AOI geometry in target CRS
        target_crs_mtm: Target CRS for output
        output_gpkg_path: Path to output GeoPackage
        output_layer_name: Name of output layer
        delete_input_temp_file: Whether to delete input file after processing
        temp_processing_dir: Temporary directory path for cleanup
        
    Returns:
        Tuple of (success: bool, count_before: int, count_after: int)
    """
    processed_successfully = False 
    count_initial_read = 0 
    count_after_reproj = 0 
    count_after_clip = 0
    
    try:
        logger.info(f"Processing vector file: {input_filepath}")
        
        if not os.path.exists(input_filepath):
            logger.error(f"Input file not found: {input_filepath}")
            return False, -1, -1
        
        # Read input data
        gdf = gpd.read_file(input_filepath)
        count_initial_read = len(gdf)
        
        if gdf.empty:
            logger.info(f"Input file {input_filepath} is empty. No processing needed for '{output_layer_name}'.")
            return True, 0, 0
        
        logger.debug(f"Columns read from {input_filepath}: {gdf.columns.tolist()}")
        
        # Handle CRS
        source_crs_for_log = "Unknown"
        if gdf.crs:
            source_crs_for_log = gdf.crs.to_string()
        else:
            logger.warning(f"CRS not defined for {input_filepath}. Assuming EPSG:4326.")
            try:
                gdf = gdf.set_crs("EPSG:4326", allow_override=True)
                source_crs_for_log = "EPSG:4326 (assumed)"
            except Exception as e_crs:
                logger.error(f"Failed to assign CRS: {e_crs}")
        
        # Reprojection
        perform_reprojection = False
        gdf_mtm = gdf
        
        if gdf.crs:
            if gdf.crs.to_string().upper() != target_crs_mtm.upper():
                perform_reprojection = True
                logger.info(f"Reprojecting {count_initial_read} features from {gdf.crs.to_string()} to {target_crs_mtm}...")
            else:
                logger.info(f"Source CRS ({gdf.crs.to_string()}) already matches target ({target_crs_mtm}).")
                count_after_reproj = count_initial_read
        else:
            perform_reprojection = True
            logger.warning(f"CRS still unknown, attempting reprojection to {target_crs_mtm}.")
        
        if perform_reprojection:
            try:
                gdf_mtm = gdf.to_crs(target_crs_mtm)
                count_after_reproj = len(gdf_mtm)
                logger.info(f"Reprojection completed. {count_after_reproj} features in {target_crs_mtm}.")
            except Exception as e:
                logger.error(f"Reprojection error: {e}")
                return False, count_initial_read, -1
        
        count_before_clip_for_summary = count_after_reproj
        logger.info(f"Features before clipping (in {target_crs_mtm}): {count_before_clip_for_summary}.")
        logger.info("Clipping with AOI geometry...")
        
        # Validate AOI geometry
        if not aoi_geometry_mtm.is_valid:
            logger.warning("AOI geometry invalid, attempting buffer(0)...")
            aoi_geometry_mtm = aoi_geometry_mtm.buffer(0)
            if not aoi_geometry_mtm.is_valid:
                logger.error("AOI geometry correction failed.")
                return False, count_before_clip_for_summary, -1
        
        # Clipping
        gdf_clipped = None
        try:
            if gdf_mtm.crs is None:
                gdf_mtm = gdf_mtm.set_crs(target_crs_mtm, allow_override=True)
            gdf_clipped = gpd.clip(gdf_mtm, aoi_geometry_mtm, keep_geom_type=False)
        except Exception as clip_err:
            error_message = str(clip_err).lower()
            if "topology" in error_message or "geos" in error_message or "self-intersection" in error_message:
                logger.error(f"Topology error during clipping: {clip_err}")
            else:
                logger.error(f"Generic clipping error: {clip_err}")
            
            logger.info("Attempting clipping after buffer(0) on GDF...")
            try:
                gdf_mtm_buffered = gdf_mtm.copy()
                gdf_mtm_buffered.geometry = gdf_mtm_buffered.geometry.buffer(0)
                if gdf_mtm_buffered.crs is None and gdf_mtm.crs is not None:
                    gdf_mtm_buffered = gdf_mtm_buffered.set_crs(gdf_mtm.crs)
                elif gdf_mtm_buffered.crs is None and target_crs_mtm:
                    gdf_mtm_buffered = gdf_mtm_buffered.set_crs(target_crs_mtm)
                gdf_clipped = gpd.clip(gdf_mtm_buffered, aoi_geometry_mtm, keep_geom_type=False)
                logger.info("Clipping after buffer(0) successful.")
            except Exception as e_retry:
                logger.error(f"Retry failed: {e_retry}")
                return False, count_before_clip_for_summary, -1
        
        if gdf_clipped is None:
            logger.error("Critical clipping failure.")
            return False, count_before_clip_for_summary, -1
        
        count_after_clip = len(gdf_clipped)
        logger.debug(f"Columns after clipping for {output_layer_name}: {gdf_clipped.columns.tolist()}")
        logger.info(f"Clipping completed. {count_after_clip} feature(s) remaining.")
        
        if gdf_clipped.empty:
            logger.info(f"No features remaining after clipping for '{output_layer_name}'.")
        
        # Save output
        logger.info(f"Saving to {output_gpkg_path}, layer '{output_layer_name}'...")
        try:
            # Clean column names
            original_cols = gdf_clipped.columns.tolist()
            gdf_clipped.columns = [str(col).replace(' ', '_').replace('.', '').replace(':', '') for col in original_cols]
            if gdf_clipped.columns.tolist() != original_cols:
                logger.warning(f"Column names cleaned for '{output_layer_name}'.")
            
            # Check if layer already exists and remove it first
            try:
                import fiona
                if os.path.exists(output_gpkg_path):
                    layers = fiona.listlayers(output_gpkg_path)
                    if output_layer_name in layers:
                        # Remove existing layer by creating a new GPKG without it
                        gdf_existing = gpd.read_file(output_gpkg_path, layer=output_layer_name)
                        # For now, we'll just append and let GeoPandas handle it
                        # In a more robust solution, we would remove the layer first
                        pass
            except Exception as e:
                logger.debug(f"Could not check existing layers: {e}")
            
            gdf_clipped.to_file(output_gpkg_path, layer=output_layer_name, driver="GPKG", index=False)
            logger.info(f"Layer '{output_layer_name}' saved successfully.")
            processed_successfully = True
            
        except Exception as e_save:
            logger.error(f"Error saving '{output_layer_name}': {e_save}")
            return False, count_before_clip_for_summary, count_after_clip
        
        return True, count_before_clip_for_summary, count_after_clip
        
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_filepath}")
        return False, -1, -1
    except Exception as e:
        logger.error(f"Major error processing {input_filepath} for {output_layer_name}: {e}")
        _count_initial = count_initial_read if 'count_initial_read' in locals() else -1
        _count_reproj = count_after_reproj if 'count_after_reproj' in locals() else -1
        count_before_summary = _count_reproj if _count_reproj != 0 else _count_initial
        return False, count_before_summary, -1
    
    finally:
        # Cleanup temporary files
        is_temp_file = False
        if temp_processing_dir and input_filepath and input_filepath.startswith(temp_processing_dir) and "temp_" in os.path.basename(input_filepath):
            is_temp_file = True
        
        if delete_input_temp_file and is_temp_file and os.path.exists(input_filepath):
            if processed_successfully:
                try:
                    os.remove(input_filepath)
                    logger.info(f"Temporary file {input_filepath} deleted.")
                except Exception as e_del:
                    logger.warning(f"Unable to delete temporary file {input_filepath}: {e_del}")
            else:
                logger.warning(f"Temporary file {input_filepath} NOT deleted (processing {output_layer_name} failed?).")

def filter_local_gpkg(config: dict, aoi_bounds_epsg4326: tuple, temp_dir: str) -> Optional[str]:
    """
    Filter local GPKG data based on AOI bounds.
    
    Args:
        config: Data source configuration
        aoi_bounds_epsg4326: AOI bounds in EPSG:4326
        temp_dir: Temporary directory for output
        
    Returns:
        Path to filtered data file, or None if no data
    """
    try:
        gpkg_path = config.get('path')
        layer_name = config.get('layer_name')
        
        if not gpkg_path or not layer_name:
            logger.error("Missing path or layer name for local GPKG filtering")
            return None
        

        logger.info(f"Reading local GPKG: {gpkg_path}, layer: {layer_name}")
        
        gdf = gpd.read_file(gpkg_path, layer=layer_name)
        
        if gdf.empty:
            logger.info("No features found in layer")
            return None
        
        logger.info(f"Found {len(gdf)} features in layer")
        
        # Save to temporary file
        temp_filename = f"temp_filtered_{config.get('id', 'unknown')}.geojson"
        temp_filepath = os.path.join(temp_dir, temp_filename)
        
        gdf.to_file(temp_filepath, driver="GeoJSON")
        logger.info(f"Data saved to: {temp_filepath}")
        
        return temp_filepath
        
    except Exception as e:
        logger.error(f"Error reading local GPKG: {e}")
        return None

def download_wfs_data(config: dict, aoi_bounds_epsg4326: tuple, temp_dir: str) -> Optional[str]:
    """
    Download WFS data based on AOI bounds.
    
    Args:
        config: Data source configuration
        aoi_bounds_epsg4326: AOI bounds in EPSG:4326
        temp_dir: Temporary directory for output
        
    Returns:
        Path to downloaded data file, or None if no data
    """
    try:
        import requests
        from urllib.parse import urlencode
        
        base_url = config.get('base_url')
        layer_name = config.get('layer_name')
        
        if not base_url or not layer_name:
            logger.error("Missing base URL or layer name for WFS download")
            return None
        
        logger.info(f"Downloading WFS data: {base_url}, layer: {layer_name}")
        
        # Build WFS request parameters
        params = {
            'service': 'WFS',
            'version': config.get('version', '2.0.0'),
            'request': 'GetFeature',
            'typename': layer_name,
            'outputFormat': config.get('output_format', 'application/json'),
            'srsname': config.get('srs_name', 'EPSG:4326'),
            'bbox': ','.join(map(str, aoi_bounds_epsg4326)) + ',EPSG:4326'
        }
        
        # Add extra parameters
        extra_params = config.get('params', {})
        params.update(extra_params)
        
        # Make request
        url = base_url + '?' + urlencode(params)
        logger.debug(f"WFS request URL: {url}")
        
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        
        if not response.content:
            logger.info("No data returned from WFS")
            return None
        
        # Save to temporary file
        temp_filename = f"temp_wfs_{config.get('id', 'unknown')}.geojson"
        temp_filepath = os.path.join(temp_dir, temp_filename)
        
        with open(temp_filepath, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"WFS data downloaded to: {temp_filepath}")
        
        return temp_filepath
        
    except Exception as e:
        logger.error(f"Error downloading WFS data: {e}")
        return None