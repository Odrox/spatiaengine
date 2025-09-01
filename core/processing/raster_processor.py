"""
Raster Data Processing Module for SpatiaEngine
"""
import os
import logging
import subprocess
import time
from typing import List, Optional, Tuple, Any 
import rasterio
from rasterio.merge import merge as rasterio_merge
from rasterio.mask import mask as rasterio_mask
from rasterio.warp import calculate_default_transform, reproject, Resampling, transform_geom
from rasterio.windows import Window, from_bounds as window_from_bounds
import numpy as np
from shapely.geometry.base import BaseGeometry 
import geopandas as gpd 
import shutil 

from ..utils.error_handler import ProcessingError, handle_errors

logger = logging.getLogger('spatiaengine.processing.raster')

def merge_reproject_clip_rasters(
    raster_file_paths: List[str], 
    target_crs_str: str, 
    aoi_geometry_in_target_crs: BaseGeometry, 
    output_raster_path: str, 
    nodata_value: Optional[float] = None, 
    default_resampling_method: Resampling = Resampling.nearest 
    ) -> Optional[str]:
    """
    Merge, reproject, and clip raster files.
    
    Args:
        raster_file_paths: List of input raster file paths
        target_crs_str: Target CRS as string
        aoi_geometry_in_target_crs: AOI geometry in target CRS
        output_raster_path: Output raster file path
        nodata_value: NoData value for output
        default_resampling_method: Resampling method
        
    Returns:
        Path to output raster file, or None if failed
    """
    
    if not raster_file_paths:
        logger.warning("No raster files provided.")
        return None
    
    is_single_file = len(raster_file_paths) == 1
    op_desc = "Processing raster" if is_single_file else "Merging/Processing rasters"
    logger.info(f"Starting {op_desc} for: {raster_file_paths[0] if is_single_file else ', '.join(raster_file_paths)}")
    logger.info(f"Target CRS: {target_crs_str}")
    logger.info(f"Output file: {output_raster_path}")
    
    opened_source_datasets_for_processing: List[rasterio.DatasetReader] = [] 
    temp_files_created_this_run: List[str] = [] 

    try:
        target_crs_obj = rasterio.crs.CRS.from_string(target_crs_str)
        
        # Special handling for COTQ data
        if is_single_file and "COTQ_2022_V1.tif" in raster_file_paths[0]:
            logger.info(f"Optimized processing (clip before reproject) for: {raster_file_paths[0]}")
            raster_path = raster_file_paths[0]
            
            with rasterio.open(raster_path) as src_dataset:
                source_crs_obj = src_dataset.crs
                if source_crs_obj is None:
                    logger.warning(f"CRS not defined for {raster_path}. Assuming EPSG:4617.")
                    source_crs_obj = rasterio.crs.CRS.from_epsg(4617)
                
                # Transform AOI to source CRS
                import geopandas as gpd
                aoi_gdf_mtm = gpd.GeoDataFrame(geometry=[aoi_geometry_in_target_crs], crs=target_crs_str)
                aoi_gdf_source_crs = aoi_gdf_mtm.to_crs(source_crs_obj)
                aoi_bounds_source_crs = aoi_gdf_source_crs.total_bounds
                
                # Calculate window to read
                try:
                    window_to_read = window_from_bounds(*aoi_bounds_source_crs, transform=src_dataset.transform).round_offsets().round_lengths()
                    window_to_read = window_to_read.intersection(Window(0, 0, src_dataset.width, src_dataset.height))
                    if window_to_read.width <= 0 or window_to_read.height <= 0:
                        logger.warning(f"AOI does not intersect {raster_path}.")
                        return None
                except Exception as e_win:
                    logger.error(f"Error calculating read window for {raster_path}: {e_win}")
                    return None
                
                logger.info(f"Reading window: {window_to_read} from {raster_path}")
                data_window = src_dataset.read(window=window_to_read, boundless=True)
                window_transform = src_dataset.window_transform(window_to_read)
                current_nodata = src_dataset.nodata
                
                if nodata_value is None and current_nodata is not None:
                    nodata_value = current_nodata
                
                # Reproject data
                logger.info("Reprojecting clipped data...")
                dst_transform, dst_width, dst_height = calculate_default_transform(
                    source_crs_obj, target_crs_obj,
                    window_to_read.width, window_to_read.height,
                    *window_transform_bounds(window_transform, window_to_read.width, window_to_read.height)
                )
                
                dst_data = np.zeros((data_window.shape[0], dst_height, dst_width), dtype=data_window.dtype)
                
                reproject(
                    source=data_window,
                    destination=dst_data,
                    src_transform=window_transform,
                    src_crs=source_crs_obj,
                    dst_transform=dst_transform,
                    dst_crs=target_crs_obj,
                    resampling=default_resampling_method,
                    src_nodata=current_nodata,
                    dst_nodata=nodata_value
                )
                
                # Save output
                out_meta = src_dataset.meta.copy()
                out_meta.update({
                    "driver": "GTiff",
                    "height": dst_height,
                    "width": dst_width,
                    "transform": dst_transform,
                    "crs": target_crs_obj,
                    "nodata": nodata_value
                })
                
                with rasterio.open(output_raster_path, "w", **out_meta) as dest:
                    dest.write(dst_data)
                
                logger.info(f"Processed raster saved: {output_raster_path}")
                return output_raster_path
        
        # Standard processing for other rasters
        logger.info("Standard processing (merge -> reproject -> clip)")
        
        # Step 1: Merge if multiple files
        if len(raster_file_paths) > 1:
            logger.info("Merging input rasters...")
            src_datasets = []
            for path in raster_file_paths:
                try:
                    src = rasterio.open(path)
                    src_datasets.append(src)
                    opened_source_datasets_for_processing.append(src)
                except Exception as e:
                    logger.error(f"Error opening raster {path}: {e}")
                    continue
            
            if not src_datasets:
                logger.error("No valid raster datasets to merge.")
                return None
            
            try:
                merged_data, merged_transform = rasterio_merge(src_datasets)
                merged_meta = src_datasets[0].meta.copy()
                merged_meta.update({
                    "driver": "GTiff",
                    "height": merged_data.shape[1],
                    "width": merged_data.shape[2],
                    "transform": merged_transform,
                    "crs": src_datasets[0].crs
                })
                
                # Save unclipped merged raster
                temp_unclipped_path = output_raster_path.replace(".tif", "_unclipped.tif")
                with rasterio.open(temp_unclipped_path, "w", **merged_meta) as dest:
                    dest.write(merged_data)
                temp_files_created_this_run.append(temp_unclipped_path)
                logger.info(f"Merged raster saved temporarily: {temp_unclipped_path}")
                
                # Use merged raster for next steps
                raster_file_paths = [temp_unclipped_path]
                
            except Exception as e:
                logger.error(f"Error merging rasters: {e}")
                return None
            finally:
                # Close source datasets
                for src in src_datasets:
                    try:
                        src.close()
                    except:
                        pass
                opened_source_datasets_for_processing.clear()
        
        # Step 2: Reproject
        logger.info("Reprojecting raster data...")
        input_raster_path = raster_file_paths[0]
        
        # Read source data
        with rasterio.open(input_raster_path) as src:
            source_crs = src.crs
            if source_crs is None:
                logger.warning(f"Source CRS not defined. Assuming EPSG:4326.")
                source_crs = rasterio.crs.CRS.from_epsg(4326)
            
            # Try GDAL for proper reprojection between different CRS
            gdal_success = False
            dst_transform = None
            dst_width = None
            dst_height = None
            dst_data = None
            
            try:
                from osgeo import gdal
                import tempfile
                
                # Create temporary reprojected file
                temp_reproj_path = os.path.join(
                    os.path.dirname(output_raster_path),
                    f"temp_reproj_{os.path.basename(output_raster_path)}"
                )
                
                # Perform reprojection with GDAL Warp
                logger.info(f"Using GDAL Warp for reprojection from {source_crs} to {target_crs_obj}")
                gdal.Warp(
                    temp_reproj_path,
                    input_raster_path,
                    dstSRS=target_crs_obj.to_string(),
                    resampleAlg='near',  # nearest neighbor for speed
                    format='GTiff',
                    dstNodata=nodata_value
                )
                
                # Read the reprojected data
                with rasterio.open(temp_reproj_path) as reproj_src:
                    dst_transform = reproj_src.transform
                    dst_width = reproj_src.width
                    dst_height = reproj_src.height
                    dst_data = reproj_src.read()
                
                # Clean up temporary file
                try:
                    os.remove(temp_reproj_path)
                except Exception as e:
                    logger.warning(f"Could not remove temporary file {temp_reproj_path}: {e}")
                
                gdal_success = True
                logger.info("GDAL reprojection successful")
                    
            except Exception as gdal_error:
                logger.warning(f"GDAL reprojection failed: {gdal_error}")
            
            # If GDAL failed, fallback to rasterio reprojection
            if not gdal_success:
                logger.info("Falling back to rasterio reprojection")
                
                # Calculate transform for reprojection
                # For proper reprojection between different CRS, we need to calculate the correct output bounds
                from rasterio.warp import transform_bounds
                
                # Transform source bounds to target CRS to get proper output extent
                target_bounds = transform_bounds(source_crs, target_crs_obj, *src.bounds)
                
                # Calculate transform based on transformed bounds
                dst_transform, dst_width, dst_height = calculate_default_transform(
                    source_crs, target_crs_obj,
                    src.width, src.height,
                    *target_bounds
                )
                
                # Read and reproject data
                src_data = src.read()
                dst_data = np.zeros((src_data.shape[0], dst_height, dst_width), dtype=src_data.dtype)
                
                reproject(
                    source=src_data,
                    destination=dst_data,
                    src_transform=src.transform,
                    src_crs=source_crs,
                    dst_transform=dst_transform,
                    dst_crs=target_crs_obj,
                    resampling=default_resampling_method,
                    src_nodata=src.nodata,
                    dst_nodata=nodata_value
                )
            
            # Save reprojected data
            temp_reprojected_path = output_raster_path.replace(".tif", "_reprojected.tif")
            
            # Create metadata for reprojected data
            reprojected_meta = src.meta.copy()
            reprojected_meta.update({
                "driver": "GTiff",
                "height": dst_height,
                "width": dst_width,
                "transform": dst_transform,
                "crs": target_crs_obj,
                "nodata": nodata_value
            })
            
            with rasterio.open(temp_reprojected_path, "w", **reprojected_meta) as dest:
                dest.write(dst_data)
            temp_files_created_this_run.append(temp_reprojected_path)
            logger.info(f"Reprojected raster saved temporarily: {temp_reprojected_path}")
            
            # Step 3: Clip to AOI (use GDAL for MNT files, rasterio for others)
            # Check if this is an MNT file
            is_mnt_file = "MNT_" in os.path.basename(input_raster_path) and input_raster_path.endswith('.tif')
            
            if is_mnt_file:
                logger.info("Using GDAL for MNT clipping (more robust)")
                # Use GDAL for clipping MNT files
                try:
                    from osgeo import gdal, ogr, osr
                    import json
                    
                    # Create a temporary shapefile for the AOI
                    import tempfile
                    import geopandas as gpd
                    from shapely.geometry import mapping
                    
                    with tempfile.TemporaryDirectory() as temp_dir:
                        # Create AOI shapefile
                        aoi_gdf = gpd.GeoDataFrame(geometry=[aoi_geometry_in_target_crs], crs=target_crs_obj)
                        aoi_shp_path = os.path.join(temp_dir, "aoi_clip.shp")
                        aoi_gdf.to_file(aoi_shp_path)
                        
                        # Use GDAL Warp for clipping with pixel alignment
                        # Get AOI bounds for target alignment
                        aoi_bounds = aoi_geometry_in_target_crs.bounds
                        
                        # Calculate pixel-aligned bounds
                        pixel_size_x = 1.0  # MNT resolution
                        pixel_size_y = 1.0
                        
                        # Align bounds to pixel grid
                        aligned_left = round(aoi_bounds[0] / pixel_size_x) * pixel_size_x
                        aligned_bottom = round(aoi_bounds[1] / pixel_size_y) * pixel_size_y
                        aligned_right = round(aoi_bounds[2] / pixel_size_x) * pixel_size_x
                        aligned_top = round(aoi_bounds[3] / pixel_size_y) * pixel_size_y
                        
                        # Define output bounds
                        output_bounds = [aligned_left, aligned_bottom, aligned_right, aligned_top]
                        
                        gdal.Warp(
                            output_raster_path,
                            temp_reprojected_path,
                            cutlineDSName=aoi_shp_path,
                            cropToCutline=True,
                            dstNodata=nodata_value,
                            format='GTiff',
                            dstSRS=target_crs_obj.to_string(),
                            outputBounds=output_bounds,
                            xRes=pixel_size_x,
                            yRes=pixel_size_y,
                            resampleAlg='near'
                        )
                        
                        logger.info(f"MNT clipped with GDAL: {output_raster_path}")
                        return output_raster_path
                        
                except Exception as gdal_e:
                    logger.error(f"GDAL clipping failed: {gdal_e}")
                    # Fallback to copying without clipping
                    import shutil
                    shutil.copy2(temp_reprojected_path, output_raster_path)
                    logger.warning(f"MNT copied without clipping: {output_raster_path}")
                    return output_raster_path
            else:
                # Standard rasterio clipping for other raster types
                logger.info("Clipping raster to AOI...")
                with rasterio.open(temp_reprojected_path) as src:
                    # Convert AOI geometry to GeoJSON-like format
                    aoi_geojson = {
                        "type": "Feature",
                        "properties": {},
                        "geometry": transform_geom(
                            target_crs_obj,
                            target_crs_obj,
                            aoi_geometry_in_target_crs
                        )
                    }
                    
                    # Apply mask
                    try:
                        # Ensure src is a valid rasterio dataset
                        if not hasattr(src, 'nodata'):
                            logger.error(f"Invalid raster source passed to mask: {type(src)}")
                            return None
                        
                        # Debug: Check if src is properly opened
                        try:
                            logger.debug(f"Source dataset info - width: {src.width}, height: {src.height}")
                            logger.debug(f"Source dataset nodata: {src.nodata}")
                        except Exception as debug_e:
                            logger.error(f"Error accessing source dataset properties: {debug_e}")
                            return None
                        
                        out_image, out_transform = rasterio_mask(
                            [src],
                            [aoi_geojson['geometry']],
                            crop=True,
                            nodata=nodata_value
                        )
                    except Exception as e:
                        error_msg = str(e).lower()
                        if "free disk space" in error_msg or "disk space" in error_msg:
                            logger.error(f"Insufficient disk space for raster processing: {e}")
                            logger.info("Try freeing up disk space or set CHECK_DISK_FREE_SPACE=FALSE in GDAL config")
                        else:
                            logger.error(f"Error clipping raster: {e}")
                            logger.error(f"Source type: {type(src)}, Source attrs: {getattr(src, '__dict__', 'no __dict__')}")
                            # Additional debug info
                            try:
                                if hasattr(src, 'closed'):
                                    logger.error(f"Source closed: {src.closed}")
                            except:
                                pass
                        return None
                    
                    if out_image is None:
                        logger.error("Raster clipping failed.")
                        return None
                    
                    # Save final output
                    final_meta = src.meta.copy()
                    final_meta.update({
                        "driver": "GTiff",
                        "height": out_image.shape[1],
                        "width": out_image.shape[2],
                        "transform": out_transform,
                        "crs": target_crs_obj,
                        "nodata": nodata_value
                    })
                    
                    with rasterio.open(output_raster_path, "w", **final_meta) as dest:
                        dest.write(out_image)
                    
                    logger.info(f"Final processed raster saved: {output_raster_path}")
                    return output_raster_path
                # Standard clipping for other raster types
                logger.info("Clipping raster to AOI...")
                with rasterio.open(temp_reprojected_path) as src:
                    # Convert AOI geometry to GeoJSON-like format
                    aoi_geojson = {
                        "type": "Feature",
                        "properties": {},
                        "geometry": transform_geom(
                            target_crs_obj,
                            target_crs_obj,
                            aoi_geometry_in_target_crs
                        )
                    }
                    
                    # Apply mask
                    try:
                        # Ensure src is a valid rasterio dataset
                        if not hasattr(src, 'nodata'):
                            logger.error(f"Invalid raster source passed to mask: {type(src)}")
                            return None
                        
                        # Debug: Check if src is properly opened
                        try:
                            logger.debug(f"Source dataset info - width: {src.width}, height: {src.height}")
                            logger.debug(f"Source dataset nodata: {src.nodata}")
                        except Exception as debug_e:
                            logger.error(f"Error accessing source dataset properties: {debug_e}")
                            return None
                        
                        out_image, out_transform = rasterio_mask(
                            [src],
                            [aoi_geojson['geometry']],
                            crop=True,
                            nodata=nodata_value
                        )
                    except Exception as e:
                        error_msg = str(e).lower()
                        if "free disk space" in error_msg or "disk space" in error_msg:
                            logger.error(f"Insufficient disk space for raster processing: {e}")
                            logger.info("Try freeing up disk space or set CHECK_DISK_FREE_SPACE=FALSE in GDAL config")
                        else:
                            logger.error(f"Error clipping raster: {e}")
                            logger.error(f"Source type: {type(src)}, Source attrs: {getattr(src, '__dict__', 'no __dict__')}")
                            # Additional debug info
                            try:
                                if hasattr(src, 'closed'):
                                    logger.error(f"Source closed: {src.closed}")
                            except:
                                pass
                        return None
                    
                    if out_image is None:
                        logger.error("Raster clipping failed.")
                        return None
                    
                    # Save final output
                    final_meta = src.meta.copy()
                    final_meta.update({
                        "driver": "GTiff",
                        "height": out_image.shape[1],
                        "width": out_image.shape[2],
                        "transform": out_transform,
                        "crs": target_crs_obj,
                        "nodata": nodata_value
                    })
                    
                    with rasterio.open(output_raster_path, "w", **final_meta) as dest:
                        dest.write(out_image)
                    
                    logger.info(f"Final processed raster saved: {output_raster_path}")
                    return output_raster_path
    
    except Exception as e:
        logger.error(f"Major error processing raster: {e}")
        return None
    
    finally:
        # Cleanup
        for ds in opened_source_datasets_for_processing:
            if hasattr(ds, 'closed') and not ds.closed:
                try:
                    ds.close()
                except:
                    pass
        
        for temp_file in temp_files_created_this_run:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                    logger.debug(f"Temporary raster file {temp_file} deleted.")
                except Exception as e_del:
                    logger.warning(f"Unable to delete {temp_file}: {e_del}")

def generate_hillshade_gdal(input_mnt_path: str, output_hillshade_path: str, options: Optional[List[str]] = None) -> bool:
    """
    Generate hillshade from MNT using GDAL.
    
    Args:
        input_mnt_path: Input MNT raster path
        output_hillshade_path: Output hillshade raster path
        options: Additional GDAL options
        
    Returns:
        True if successful, False otherwise
    """
    # Wait for the input file to exist, with a timeout, to prevent race conditions
    wait_time = 0
    max_wait = 5  # seconds
    while not os.path.exists(input_mnt_path) and wait_time < max_wait:
        logger.debug(f"Waiting for input file {input_mnt_path} to become available...")
        time.sleep(0.5)
        wait_time += 0.5

    if not os.path.exists(input_mnt_path):
        logger.error(f"Input MNT file not found after waiting {max_wait} seconds: {input_mnt_path}")
        return False
    
    logger.info(f"Generating hillshade for: {input_mnt_path}")
    logger.info(f"Hillshade output: {output_hillshade_path}")
    
    command = [
        "gdaldem", "hillshade", 
        input_mnt_path, 
        output_hillshade_path, 
        "-of", "GTiff", 
        "-co", "COMPRESS=LZW", 
        "-co", "TILED=YES"
    ]
    
    if options:
        command.extend(options)
    
    logger.debug(f"GDAL command: {' '.join(command)}")
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False, encoding='utf-8')
        if result.returncode == 0:
            logger.info(f"Hillshade generated: {output_hillshade_path}")
            return True
        else:
            logger.error(f"GDAL hillshade error:\n  Stdout: {result.stdout}\n  Stderr: {result.stderr}")
            return False
    except FileNotFoundError:
        logger.error("GDAL 'gdaldem' not found.")
        return False
    except Exception as e:
        logger.error(f"GDAL hillshade error: {e}")
        return False


def delete_temp_files(file_paths: List[str]) -> None:
    """
    Delete temporary raster files.
    
    Args:
        file_paths: List of file paths to delete
    """
    if not file_paths:
        return
    
    logger.info(f"Cleaning up {len(file_paths)} temporary file(s)...")
    
    for f_path in file_paths:
        if f_path and os.path.exists(f_path):
            try:
                os.remove(f_path)
                logger.debug(f"Temporary file deleted: {f_path}")
            except Exception as e:
                logger.warning(f"Unable to delete {f_path}: {e}")
        elif f_path:
            logger.debug(f"Path {f_path} not found for deletion.")

def window_transform_bounds(transform, width, height):
    """Helper function to get bounds from window transform."""
    left = transform.c
    top = transform.f
    right = left + width * transform.a
    bottom = top + height * transform.e
    return left, bottom, right, top
