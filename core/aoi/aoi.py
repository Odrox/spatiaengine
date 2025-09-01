"""
Area of Interest (AOI) Management for SpatiaEngine
"""
import geopandas
import pandas as pd 
from shapely.geometry import box, shape
from shapely.ops import unary_union 
from pyproj import CRS
from pyproj.exceptions import CRSError
import os
import logging 
from typing import List, Tuple, Optional, TYPE_CHECKING, Dict, Union

if TYPE_CHECKING:
    from shapely.geometry.base import BaseGeometry 
    import numpy as np 

from ..utils.error_handler import AOIError, handle_errors
from .aoi_handler import (
    get_snrc_50k_bounds_gdal,      
    get_mnt_20k_subfeuillet_data_gdal, 
    get_kml_bounds, 
    get_mtm_nad83_crs_from_bounds,
    MNT_20K_INDEX_GPKG_PATH,     
    MNT_20K_INDEX_LAYER_NAME,    
    MNT_20K_FEUILLET_COLUMN      
)

# Define valid MTM CRS codes for Quebec
VALID_MTM_CRS = {
    '32181': 'MTM zone 1 (EPSG:32181)',
    '32182': 'MTM zone 2 (EPSG:32182)',
    '32183': 'MTM zone 3 (EPSG:32183)',
    '32184': 'MTM zone 4 (EPSG:32184)',
    '32185': 'MTM zone 5 (EPSG:32185)',
    '32186': 'MTM zone 6 (EPSG:32186)',
    '32187': 'MTM zone 7 (EPSG:32187)',
    '32188': 'MTM zone 8 (EPSG:32188)',
    '32189': 'MTM zone 9 (EPSG:32189)',
    '32190': 'MTM zone 10 (EPSG:32190)'
}

logger = logging.getLogger('spatiaengine.aoi')

class Aoi:
    """Area of Interest management class."""
    
    @staticmethod
    def is_valid_mtm_crs(crs: Union[str, int, CRS]) -> bool:
        """
        Check if CRS is a valid MTM projection for Quebec.
        
        Args:
            crs: CRS to check
            
        Returns:
            bool: True if valid MTM CRS, False otherwise
        """
        try:
            crs_obj = CRS(crs)
            epsg_code = crs_obj.to_epsg()
            return str(epsg_code) in VALID_MTM_CRS
        except Exception as e:
            logger.warning(f"Error checking CRS {crs}: {e}")
            return False

    @staticmethod
    def get_mtm_zone_from_bounds(bounds: Tuple[float, float, float, float]) -> str:
        """
        Determine appropriate MTM zone from geographic bounds.
        
        Args:
            bounds: (minx, miny, maxx, maxy) in geographic coordinates
            
        Returns:
            str: EPSG code of appropriate MTM zone
        """
        # Calculate center of bounds
        center_lon = (bounds[0] + bounds[2]) / 2
        center_lat = (bounds[1] + bounds[3]) / 2
        
        # Determine MTM zone based on longitude
        # Quebec MTM zones: 7 (-72 to -69), 8 (-75 to -72), 9 (-78 to -75)
        if -72 <= center_lon < -69:  # Zone 7 (Quebec City area)
            return '32187'
        elif -75 <= center_lon < -72:  # Zone 8 (Montreal area)
            return '32188'
        elif -78 <= center_lon < -75:  # Zone 9
            return '32189'
        elif -81 <= center_lon < -78:  # Zone 10
            return '32190'
        else:
            # Default to zone 8 (most populated)
            return '32188'

    def __init__(self, custom_crs: Optional[str] = None):
        """
        Initialize AOI.
        
        Args:
            custom_crs: Custom MTM CRS to use
        """
        logger.debug("AOI __init__ called.")
        self.definition_type: Optional[str] = None 
        self.input_references: List[str] = []    
        self.combined_gdf_epsg4326: Optional[geopandas.GeoDataFrame] = None
        self.combined_geometry_epsg4326: Optional['BaseGeometry'] = None 
        self.bounds_epsg4326: Optional[Tuple[float, float, float, float]] = None
        
        # Handle custom CRS
        self.use_custom_crs = False
        self.target_mtm_crs = None
        
        if custom_crs:
            if self.is_valid_mtm_crs(custom_crs):
                self.target_mtm_crs = str(CRS(custom_crs).to_epsg())
                self.use_custom_crs = True
                logger.info(f"Using custom MTM projection: {self.target_mtm_crs} ({VALID_MTM_CRS.get(self.target_mtm_crs, 'Unknown')})")
            else:
                logger.warning(f"Projection {custom_crs} is not a valid MTM projection for Quebec. "
                              f"Using automatic MTM projection based on location.")
        
        self.combined_geometry_mtm: Optional['BaseGeometry'] = None
        self.subfeuillet_20k_data_gdfs: List[geopandas.GeoDataFrame] = []

    def _is_code_20k(self, code: str) -> bool:
        """Check if code is a 1:20k sub-sheet."""
        code_upper = code.upper()
        if len(code_upper) in [7, 8] and code_upper[-2:] in ["NE", "NO", "SE", "SO", "NW", "SW"]:
            prefix_len = len(code_upper) - 2
            prefix = code_upper[:prefix_len]
            # Check if prefix (50k part) is valid
            if (prefix_len == 5 and prefix[0:2].isdigit() and prefix[2].isalpha() and prefix[3:5].isdigit()) or \
               (prefix_len == 6 and prefix[0:3].isdigit() and prefix[3].isalpha() and prefix[4:6].isdigit()):
                return True
        return False

    def _normalize_50k_code_for_20k_index(self, code_50k: str) -> str:
        """Normalize 50k code for 20k index matching."""
        code_upper = code_50k.upper()
        if len(code_upper) == 6 and code_upper.startswith('0'):
            return code_upper[1:]
        return code_upper

    @handle_errors(AOIError, default_return=False)
    def define_from_snrc_codes(self, snrc_codes_input: List[str]) -> bool:
        """
        Define AOI from SNRC codes.
        
        Args:
            snrc_codes_input: List of SNRC codes
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Defining AOI from SNRC codes: {snrc_codes_input}")
        if not snrc_codes_input:
            logger.error("AOI: No SNRC codes provided.")
            return False
        
        self.definition_type = "SNRC"
        self.input_references = [c.upper() for c in snrc_codes_input]
        
        collected_20k_gdfs: List[geopandas.GeoDataFrame] = []
        common_crs_for_union = "EPSG:32198"  # CRS of MNT 20k index

        for code_input_orig in self.input_references:
            code_input = code_input_orig.upper()
            
            if self._is_code_20k(code_input):
                logger.info(f"Processing 1:20k SNRC code directly: {code_input}")
                gdf_20k = get_mnt_20k_subfeuillet_data_gdal(code_input, target_crs_str=common_crs_for_union)
                if gdf_20k is not None and not gdf_20k.empty:
                    collected_20k_gdfs.append(gdf_20k)
                else:
                    logger.warning(f"AOI: Unable to get geometry for 1:20k sub-sheet {code_input}.")
            else:  # 1:50k code (or unrecognized as 20k)
                logger.info(f"Processing 1:50k SNRC code: {code_input}. Searching for 1:20k sub-sheets...")
                
                # Normalize 50k code for 20k index matching
                normalized_50k_prefix = self._normalize_50k_code_for_20k_index(code_input)
                logger.debug(f"Normalized 50k prefix for 20k search: {normalized_50k_prefix}")

                try:
                    logger.debug(f"Reading MNT 20k index: {MNT_20K_INDEX_GPKG_PATH}, layer {MNT_20K_INDEX_LAYER_NAME}")
                    index_mnt_gdf_full = geopandas.read_file(MNT_20K_INDEX_GPKG_PATH, layer=MNT_20K_INDEX_LAYER_NAME)
                    
                    if index_mnt_gdf_full.crs is None: 
                        logger.warning(f"CRS of MNT 20k index not defined. Assuming {common_crs_for_union}.")
                        index_mnt_gdf_full = index_mnt_gdf_full.set_crs(common_crs_for_union)
                    elif index_mnt_gdf_full.crs.to_string().upper() != common_crs_for_union.upper():
                        logger.info(f"Reprojecting MNT 20k index from {index_mnt_gdf_full.crs} to {common_crs_for_union}")
                        index_mnt_gdf_full = index_mnt_gdf_full.to_crs(common_crs_for_union)
                    
                    # Filter 20k index for sub-sheets that start with the 50k prefix
                    target_prefix_len = len(normalized_50k_prefix)
                    intersecting_20k_tiles = index_mnt_gdf_full[
                        index_mnt_gdf_full[MNT_20K_FEUILLET_COLUMN].str.upper().str.startswith(normalized_50k_prefix) &
                        (index_mnt_gdf_full[MNT_20K_FEUILLET_COLUMN].str.len() == target_prefix_len + 2)
                    ]
                    
                    if intersecting_20k_tiles.empty:
                        logger.warning(f"AOI: No 1:20k sub-sheets found for prefix {normalized_50k_prefix} (derived from {code_input}).")
                        continue
                    
                    tile_names = intersecting_20k_tiles[MNT_20K_FEUILLET_COLUMN].tolist()
                    logger.info(f"Found {len(intersecting_20k_tiles)} 1:20k sub-sheet(s) for {code_input}: {tile_names}")
                    
                    # Add each sub-sheet GDF individually
                    for i in range(len(intersecting_20k_tiles)):
                        collected_20k_gdfs.append(intersecting_20k_tiles.iloc[[i]].copy())

                except Exception as e_index_read:
                    logger.error(f"Error reading or filtering MNT 20k index for {code_input}: {e_index_read}")
                    continue 

        if not collected_20k_gdfs:
            logger.error("AOI: No valid 1:20k sub-sheet geometries could be determined.")
            return False

        logger.info(f"AOI - {len(collected_20k_gdfs)} GeoDataFrames of 1:20k sub-sheets ready for final union.")
        self.subfeuillet_20k_data_gdfs = [gdf.copy() for gdf in collected_20k_gdfs] 
        try:
            # All GDFs should be in common_crs_for_union (EPSG:32198)
            geoms_for_union = [gdf.geometry.iloc[0] for gdf in collected_20k_gdfs if not gdf.empty and gdf.geometry.iloc[0] is not None]
            if not geoms_for_union:
                logger.error("AOI: No valid geometries to union.")
                return False
            unified_20k_geometry = unary_union(geoms_for_union)
            if unified_20k_geometry is None or unified_20k_geometry.is_empty:
                 logger.error("AOI: Union of 1:20k geometries empty/None.")
                 return False
            self.combined_gdf_epsg4326 = geopandas.GeoDataFrame(
                [{'id': 1, 'description': 'Combined 1:20k AOI', 'geometry': unified_20k_geometry}], 
                crs=common_crs_for_union 
            )
            self.bounds_epsg4326 = self.combined_gdf_epsg4326.to_crs("EPSG:4326").total_bounds
            logger.info(f"AOI (based on 1:20k) defined. EPSG:4326 bounds: {self.bounds_epsg4326}")
        except Exception as e: 
            logger.error(f"AOI: Final 1:20k union failed: {e}")
            return False
        return self._finalize_definition()

    @handle_errors(AOIError, default_return=False)
    def define_from_kml_file(self, kml_filepath: str) -> bool:
        """
        Define AOI from KML file.
        
        Args:
            kml_filepath: Path to KML file
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Defining AOI from KML file: {kml_filepath}")
        if not kml_filepath or not os.path.exists(kml_filepath):
            logger.error(f"AOI: KML file not found: {kml_filepath}")
            return False
        self.definition_type = "KML"
        self.input_references = [kml_filepath]
        try:
            gdf_kml = get_kml_bounds(kml_filepath, target_crs_str="EPSG:4326") 
            if gdf_kml is None or gdf_kml.empty:
                logger.error(f"AOI: KML reading failed/empty: {kml_filepath}")
                return False
            self.combined_gdf_epsg4326 = gdf_kml 
            if not self.combined_gdf_epsg4326.empty:
                geom = self.combined_gdf_epsg4326.geometry.iloc[0]
                if geom is None or geom.is_empty:
                    logger.error("AOI: KML geometry empty/None.")
                    return False
                self.combined_geometry_epsg4326 = geom 
            else:
                logger.error(f"AOI: KML GDF empty: {kml_filepath}")
                return False
            self.bounds_epsg4326 = self.combined_gdf_epsg4326.total_bounds
            logger.info(f"AOI defined (KML): {kml_filepath}")
            
            # Find MNT 1:20k sub-sheets for KML AOI
            logger.info("Identifying MNT 1:20k sub-sheets for KML AOI...")
            try:
                index_mnt_gdf = geopandas.read_file(MNT_20K_INDEX_GPKG_PATH, layer=MNT_20K_INDEX_LAYER_NAME)
                if index_mnt_gdf.crs is None:
                    index_mnt_gdf = index_mnt_gdf.set_crs("EPSG:32198")
                # Reproject KML geometry to index CRS
                kml_geom_reproj = geopandas.GeoSeries([self.combined_geometry_epsg4326], crs="EPSG:4326").to_crs(index_mnt_gdf.crs).iloc[0]
                intersecting_20k_tiles = index_mnt_gdf[index_mnt_gdf.intersects(kml_geom_reproj)]
                if not intersecting_20k_tiles.empty:
                    self.subfeuillet_20k_data_gdfs = [intersecting_20k_tiles.iloc[[i]].copy() for i in range(len(intersecting_20k_tiles))]
                    logger.info(f"Found {len(self.subfeuillet_20k_data_gdfs)} MNT 1:20k sub-sheet(s) for KML AOI.")
                else:
                    logger.warning("No MNT 1:20k sub-sheets intersect KML AOI.")
            except Exception as e_kml_mnt_index:
                logger.error(f"Error identifying MNT sub-sheets for KML AOI: {e_kml_mnt_index}")
            
            return self._finalize_definition()
        except Exception as e:
            logger.error(f"AOI: KML reading error: {e}")
            return False

    def _finalize_definition(self) -> bool:
        """
        Finalize AOI definition with reprojection.
        
        Returns:
            bool: True if successful, False otherwise
        """
        logger.debug("AOI _finalize_definition called.")
        if self.combined_gdf_epsg4326 is None or self.bounds_epsg4326 is None: 
            logger.error("AOI: Finalization impossible - geometry or bounds not defined.")
            return False
            
        # Validate custom CRS if used
        if self.use_custom_crs and self.target_mtm_crs:
            if not self.is_valid_mtm_crs(self.target_mtm_crs):
                logger.warning(f"Custom projection {self.target_mtm_crs} is not a valid MTM projection. "
                              f"Using automatic MTM projection based on location.")
                self.use_custom_crs = False
                self.target_mtm_crs = None
        
        # Determine MTM projection if needed
        if not self.target_mtm_crs:
            self.target_mtm_crs = self.get_mtm_zone_from_bounds(self.bounds_epsg4326)
            if not self.target_mtm_crs: 
                logger.error("AOI: Unable to determine MTM zone from bounds.")
                return False
            logger.info(f"MTM projection determined automatically: {self.target_mtm_crs} ({VALID_MTM_CRS.get(self.target_mtm_crs, 'Unknown')})")
        
        # Ensure CRS is properly formatted
        try:
            # If CRS is numeric, add EPSG: prefix
            if isinstance(self.target_mtm_crs, (int, str)) and str(self.target_mtm_crs).isdigit():
                self.target_mtm_crs = f"EPSG:{self.target_mtm_crs}"
                
            # Validate and normalize CRS
            crs_obj = CRS(self.target_mtm_crs)
            epsg_code = crs_obj.to_epsg()
            if epsg_code is None:
                raise ValueError(f"Unable to determine EPSG code for {self.target_mtm_crs}")
                
            self.target_mtm_crs = f"EPSG:{epsg_code}"  # Normalize format
            logger.info(f"Using projection: {self.target_mtm_crs} ({crs_obj.name})")
        except Exception as e:
            logger.error(f"AOI: Invalid CRS format {self.target_mtm_crs}: {e}")
            # Try fallback method
            try:
                self.target_mtm_crs = f"EPSG:{self.get_mtm_zone_from_bounds(self.bounds_epsg4326)}"
                logger.warning(f"Using fallback projection: {self.target_mtm_crs}")
            except Exception as fallback_error:
                logger.error(f"Fallback projection failed: {fallback_error}")
                return False
            
        # Reproject to target CRS
        logger.debug(f"Reprojecting geometry to {self.target_mtm_crs}...")
        try:
            if self.combined_gdf_epsg4326.crs is None: 
                logger.error("AOI: Source CRS not defined.")
                return False
                
            # Check if reprojection is needed
            if str(self.combined_gdf_epsg4326.crs).upper() == self.target_mtm_crs.upper():
                logger.info("Geometry is already in target CRS. No reprojection needed.")
                self.combined_geometry_mtm = self.combined_gdf_epsg4326.geometry.iloc[0]
                return True
                
            # Perform reprojection
            gdf_mtm = self.combined_gdf_epsg4326.to_crs(self.target_mtm_crs)
            if not gdf_mtm.empty:
                geom_mtm = gdf_mtm.geometry.iloc[0]
                if geom_mtm is None or geom_mtm.is_empty: 
                    logger.error("AOI: Geometry empty after reprojection.")
                    return False
                self.combined_geometry_mtm = geom_mtm 
                logger.info(f"AOI geometry reprojected successfully to {self.target_mtm_crs}.")
                return True
            else: 
                logger.error(f"AOI: No geometry after reprojection to {self.target_mtm_crs}.")
                return False
        except Exception as e: 
            logger.error(f"AOI: Reprojection to {self.target_mtm_crs} failed: {e}")
            return False

    def get_display_name(self) -> str:
        """Get display name for AOI."""
        if self.definition_type == "SNRC":
            if len(self.input_references) == 1:
                return self.input_references[0].replace("/", "_")
            elif len(self.input_references) > 1:
                return f"{self.input_references[0].replace('/', '_')}_and_{len(self.input_references)-1}_others"
        elif self.definition_type == "KML" and self.input_references:
            return os.path.splitext(os.path.basename(self.input_references[0]))[0].replace(" ", "_")
        return "unknown_aoi"

    def get_type_prefix(self) -> str:
        """Get type prefix for AOI."""
        return self.definition_type if self.definition_type else "GEN"