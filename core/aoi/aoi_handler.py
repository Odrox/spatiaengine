"""
Low-level AOI Handling Functions for SpatiaEngine
"""
import geopandas
from shapely.wkb import loads as wkb_loads
from osgeo import ogr, osr, gdal 
from pyproj import CRS 
from pyproj.exceptions import CRSError
import os
from typing import Union, Optional, Tuple, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    import numpy as np 

from ..utils.error_handler import AOIError, handle_errors

logger = logging.getLogger('spatiaengine.aoi.handler')

# Default paths (will be resolved through environment variables)
SNRC_50K_GPKG_PATH = os.path.join(os.getenv('GIS_REFERENCE_PATH', '.'), 'INDEX.gpkg')
SNRC_50K_LAYER_NAME = "CA_index_snrc_50k" 
SNRC_50K_COLUMN = "NTS_SNRC" 

MNT_20K_INDEX_GPKG_PATH = os.path.join(os.getenv('GIS_REFERENCE_PATH', '.'), 'INDEX.gpkg')
MNT_20K_INDEX_LAYER_NAME = "QC_index_url_snrc_mnt"
MNT_20K_FEUILLET_COLUMN = "feuillet" 
MNT_20K_URL_COLUMN = "lidar_url"    

def _normalize_code_for_20k_index(code: str) -> str:
    """Normalize code for 20k index matching."""
    code_upper = code.upper()
    if len(code_upper) == 8 and code_upper.startswith('0') and code_upper[3].isalpha():
        prefix_50k_part = code_upper[1:6]
        if len(prefix_50k_part) == 5 and prefix_50k_part[0:2].isdigit() and \
           prefix_50k_part[2].isalpha() and prefix_50k_part[3:5].isdigit():
            return code_upper[1:] 
    elif len(code_upper) == 7 and code_upper[2].isalpha():
        prefix_50k_part = code_upper[0:5]
        if len(prefix_50k_part) == 5 and prefix_50k_part[0:2].isdigit() and \
           prefix_50k_part[2].isalpha() and prefix_50k_part[3:5].isdigit():
            return code_upper 
    return code_upper

@handle_errors(AOIError, default_return=None)
def get_snrc_50k_bounds_gdal(snrc_50k_code: str, target_crs_str: str = "EPSG:4326") -> Optional[geopandas.GeoDataFrame]:
    """
    Get bounds for SNRC 50k code using GDAL.
    
    Args:
        snrc_50k_code: SNRC 50k code
        target_crs_str: Target CRS for output
        
    Returns:
        GeoDataFrame with geometry, or None if failed
    """
    logger.info(f"Reading 50k index: {snrc_50k_code} from {SNRC_50K_GPKG_PATH}")
    
    if not os.path.exists(SNRC_50K_GPKG_PATH):
        logger.error(f"50k index file '{SNRC_50K_GPKG_PATH}' not found.")
        return None
    
    ogr.RegisterAll()
    dataSource = ogr.Open(SNRC_50K_GPKG_PATH, 0)
    if dataSource is None:
        logger.error(f"GDAL/OGR: Unable to open 50k index {SNRC_50K_GPKG_PATH}")
        return None
    
    layer = dataSource.GetLayerByName(SNRC_50K_LAYER_NAME)
    if layer is None:
        logger.error(f"GDAL/OGR: Layer '{SNRC_50K_LAYER_NAME}' not found in index.")
        dataSource.Destroy()
        return None
    
    filter_expression = f"UPPER({SNRC_50K_COLUMN}) = '{snrc_50k_code.upper()}'" 
    layer.SetAttributeFilter(filter_expression)
    feature = layer.GetNextFeature()
    found_geometry = None
    source_srs_from_layer = None
    
    if feature:
        geom_ogr = feature.GetGeometryRef()
        if geom_ogr:
            srs_ogr = geom_ogr.GetSpatialReference()
            if srs_ogr:
                source_srs_from_layer = CRS.from_wkt(srs_ogr.ExportToWkt())
            geom_wkb_data = geom_ogr.ExportToWkb()
            geom_wkb = bytes(geom_wkb_data) if isinstance(geom_wkb_data, bytearray) else geom_wkb_data
            found_geometry = wkb_loads(geom_wkb)
        feature.Destroy() 
    
    layer.ResetReading()
    layer = None
    dataSource.Destroy()
    dataSource = None
    
    if not found_geometry:
        logger.warning(f"50k sheet {snrc_50k_code} not found.")
        return None
    
    if source_srs_from_layer is None:
        logger.warning(f"CRS for 50k index not determined for {snrc_50k_code}, assuming EPSG:4269.")
        source_srs_from_layer = CRS.from_epsg(4269)
    
    gdf = geopandas.GeoDataFrame([{'code_snrc_50k': snrc_50k_code, 'geometry': found_geometry}], crs=source_srs_from_layer)
    
    try:
        target_crs = CRS.from_user_input(target_crs_str)
        if gdf.crs.to_string().upper() != target_crs.to_string().upper():
            gdf = gdf.to_crs(target_crs)
        logger.info(f"50k sheet {snrc_50k_code} found and projected to {target_crs_str}.")
        return gdf
    except Exception as e:
        logger.error(f"Error reprojecting 50k sheet {snrc_50k_code}: {e}")
        return None

@handle_errors(AOIError, default_return=None)
def get_mnt_20k_subfeuillet_data_gdal(subfeuillet_20k_code_input: str, target_crs_str: str = "EPSG:32198") -> Optional[geopandas.GeoDataFrame]:
    """
    Get MNT 20k sub-sheet data using GDAL.
    
    Args:
        subfeuillet_20k_code_input: 20k sub-sheet code
        target_crs_str: Target CRS for output
        
    Returns:
        GeoDataFrame with data, or None if failed
    """
    normalized_subfeuillet_code = _normalize_code_for_20k_index(subfeuillet_20k_code_input)
    logger.info(f"Reading MNT 20k index: '{subfeuillet_20k_code_input}' (normalized: '{normalized_subfeuillet_code}')")
    
    if not os.path.exists(MNT_20K_INDEX_GPKG_PATH):
        logger.error(f"MNT 20k index file '{MNT_20K_INDEX_GPKG_PATH}' not found.")
        return None
    
    ogr.RegisterAll()
    dataSource = ogr.Open(MNT_20K_INDEX_GPKG_PATH, 0)
    if dataSource is None:
        logger.error(f"GDAL/OGR: Unable to open MNT 20k index {MNT_20K_INDEX_GPKG_PATH}")
        return None
    
    layer = dataSource.GetLayerByName(MNT_20K_INDEX_LAYER_NAME)
    if layer is None:
        logger.error(f"GDAL/OGR: Layer '{MNT_20K_INDEX_LAYER_NAME}' not found in MNT index.")
        dataSource.Destroy()
        return None
    
    filter_expression = f"UPPER({MNT_20K_FEUILLET_COLUMN}) = '{normalized_subfeuillet_code.upper()}'"
    logger.debug(f"MNT 20k filter: {filter_expression}")
    layer.SetAttributeFilter(filter_expression)
    feature = layer.GetNextFeature()
    attributes = {}
    found_geometry = None
    source_srs_from_layer = None
    
    if feature:
        geom_ogr = feature.GetGeometryRef()
        if geom_ogr:
            srs_ogr = geom_ogr.GetSpatialReference()
            if srs_ogr:
                source_srs_from_layer = CRS.from_wkt(srs_ogr.ExportToWkt())
            else:
                logger.warning(f"CRS not found for geometry {normalized_subfeuillet_code}.")
            geom_wkb_data = geom_ogr.ExportToWkb()
            geom_wkb = bytes(geom_wkb_data) if isinstance(geom_wkb_data, bytearray) else geom_wkb_data
            found_geometry = wkb_loads(geom_wkb)
        for i in range(feature.GetFieldCount()):
            field_defn = feature.GetFieldDefnRef(i)
            attributes[field_defn.GetNameRef()] = feature.GetField(i)
        feature.Destroy()
    
    layer.ResetReading()
    layer = None
    dataSource.Destroy()
    dataSource = None
    
    if not found_geometry:
        logger.warning(f"MNT 20k sub-sheet '{normalized_subfeuillet_code}' not found.")
        return None
    
    if source_srs_from_layer is None:
        logger.warning(f"MNT 20k index CRS not determined, assuming EPSG:32198.")
        source_srs_from_layer = CRS.from_epsg(32198) 
    
    data_for_gdf = {**attributes, 'geometry': found_geometry}
    gdf = geopandas.GeoDataFrame([data_for_gdf], crs=source_srs_from_layer)
    
    try:
        target_crs = CRS.from_user_input(target_crs_str)
        if gdf.crs.to_string().upper() != target_crs.to_string().upper():
            gdf = gdf.to_crs(target_crs)
        logger.info(f"MNT 20k data {normalized_subfeuillet_code} found and prepared in {target_crs_str}.")
        return gdf
    except Exception as e:
        logger.error(f"Error reprojecting MNT 20k {normalized_subfeuillet_code}: {e}")
        return None

@handle_errors(AOIError, default_return=None)
def get_kml_bounds(kml_path: str, target_crs_str: str = "EPSG:4326") -> Optional[geopandas.GeoDataFrame]:
    """
    Get bounds from KML file.
    
    Args:
        kml_path: Path to KML file
        target_crs_str: Target CRS for output
        
    Returns:
        GeoDataFrame with bounds, or None if failed
    """
    logger.info(f"Reading KML: {kml_path}")
    
    if not os.path.exists(kml_path):
        logger.error(f"KML file not found: {kml_path}")
        return None
    
    try:
        gdf_kml = geopandas.read_file(kml_path)
        if gdf_kml.empty:
            logger.warning(f"No geometry in KML: {kml_path}")
            return None
        
        if gdf_kml.crs is None:
            logger.warning(f"KML CRS not defined for {kml_path}, assuming EPSG:4326.")
            gdf_kml.set_crs("EPSG:4326", inplace=True)
        
        unified_geom = gdf_kml.unary_union
        if unified_geom is None or unified_geom.is_empty:
            logger.warning(f"Union of KML geometry from {kml_path} is empty.")
            return None
        
        gdf_bounds = geopandas.GeoDataFrame([{'id': 1, 'geometry': unified_geom}], crs=gdf_kml.crs)
        target_crs = CRS.from_user_input(target_crs_str)
        
        if gdf_bounds.crs.to_string().upper() != target_crs.to_string().upper():
            gdf_bounds = gdf_bounds.to_crs(target_crs)
        
        logger.info(f"KML bounds {kml_path} found and projected to {target_crs_str}.")
        return gdf_bounds
    except Exception as e:
        logger.error(f"Error reading KML {kml_path}: {e}")
        return None

def get_mtm_nad83_crs_from_bounds(bounds: Optional[Union[Tuple[float,float,float,float], 'np.ndarray']]) -> Optional[str]:
    """
    Get appropriate MTM NAD83 CRS from bounds.
    
    Args:
        bounds: (minx, miny, maxx, maxy) bounds
        
    Returns:
        EPSG code string, or None if failed
    """
    if bounds is None or not hasattr(bounds, '__len__') or len(bounds) != 4:
        logger.error(f"Invalid bounds for MTM: {bounds}")
        return None
    
    minx, _, maxx, _ = bounds
    center_lon = (minx + maxx) / 2
    
    # Quebec MTM zones
    if -58.5 <= center_lon < -55.5:
        return "EPSG:32183" 
    if -61.5 <= center_lon < -58.5:
        return "EPSG:32184" 
    if -64.5 <= center_lon < -61.5:
        return "EPSG:32185" 
    if -67.5 <= center_lon < -64.5:
        return "EPSG:32186" 
    if -70.5 <= center_lon < -67.5:
        return "EPSG:32187" 
    if -73.5 <= center_lon < -70.5:
        return "EPSG:32188" 
    if -76.5 <= center_lon < -73.5:
        return "EPSG:32189" 
    if -79.5 <= center_lon < -76.5:
        return "EPSG:32190" 
    
    logger.warning(f"Longitude {center_lon}° outside Quebec MTM zones.")
    return None