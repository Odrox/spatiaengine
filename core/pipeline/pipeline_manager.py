"""
Pipeline Manager for SpatiaEngine - Orchestrates the complete data processing workflow
"""
import os
import shutil
from datetime import datetime
import logging 
from typing import List, Dict, Optional, Tuple, Any, Union
import textwrap 
import yaml 
import geopandas 

from ..config.config_manager import ConfigManager
from ..config.validators import ConfigValidator
from ..aoi.aoi import Aoi
from ..datasources.factory import DataSourceFactory
from ..datasources.base import DataSource
from ..datasources.mnt_lidar import MNTLiDARSource
from ..datasources.courbes_niveau import CourbesNiveauSource
from ..datasources.raster import LocalRasterDataSource
from ..processing.vector_processor import process_vector_data
from ..processing.raster_processor import merge_reproject_clip_rasters, generate_hillshade_gdal, delete_temp_files
from ..qgis.qgis_manager import QGISManager
from ..utils.error_handler import SpatiaEngineError, ConfigurationError, ProcessingError, handle_errors
from ..utils.logging_utils import setup_dual_logging
from ..utils.file_utils import ensure_directory, safe_delete_file
from ..ui.progress_display import ProgressDisplay

logger = logging.getLogger('spatiaengine.pipeline')

def get_mtm_short_name(mtm_epsg_code: Optional[str]) -> str:
    """Get short name for MTM zone."""
    if not mtm_epsg_code or "EPSG:" not in mtm_epsg_code:
        return "UnknownZone" 
    try:
        mtm_zone_map = {
            "32183": "3", "32184": "4", "32185": "5", "32186": "6",
            "32187": "7", "32188": "8", "32189": "9", "32190": "10"
        }
        code_epsg_num_str = mtm_epsg_code.split(":")[1]
        return mtm_zone_map.get(code_epsg_num_str, f"EPSG{code_epsg_num_str}")
    except Exception:
        return "ErrorZone"

class PipelineManager:
    """Main pipeline manager for orchestrating data processing."""
    
    def __init__(self, project_id: str, output_base_dir: str, custom_crs: Optional[str] = None):
        """
        Initialize pipeline manager.
        
        Args:
            project_id: Project identifier
            output_base_dir: Base output directory
            custom_crs: Custom CRS for projection
        """
        self.project_id: str = project_id
        self.output_base_dir: str = output_base_dir
        self.custom_crs: Optional[str] = custom_crs
        self.aoi: Optional[Aoi] = None
        self.list_of_datasources: List[DataSource] = []
        self.project_main_folder_name: Optional[str] = None
        self.project_output_dir: Optional[str] = None
        self.imagery_output_subdir: Optional[str] = None
        self.temp_files_dir: Optional[str] = None
        self.output_vector_gpkg_filepath: Optional[str] = None 
        self.processing_summary: List[Dict[str, Any]] = []
        
        logger.info(f"PipelineManager initialized for Project ID: {self.project_id}")
        if self.custom_crs:
            logger.info(f"Using custom projection: {self.custom_crs}")

    @handle_errors(SpatiaEngineError, default_return=False)
    def setup_aoi(self, aoi_definition_type: str, aoi_input: Any) -> bool:
        """
        Set up AOI for processing.
        
        Args:
            aoi_definition_type: Type of AOI definition (SNRC, KML)
            aoi_input: AOI input data
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.debug(f"PipelineManager setup_aoi called with type='{aoi_definition_type}'.")
        aoi_success = False
        
        try:
            # Create AOI instance with custom CRS if defined
            self.aoi = Aoi(custom_crs=self.custom_crs)
            logger.debug(f"AOI instance created: {self.aoi}")
            
            if aoi_definition_type == "SNRC":
                if isinstance(aoi_input, list) and aoi_input: 
                    aoi_success = self.aoi.define_from_snrc_codes(aoi_input)
                else: 
                    logger.error("Invalid SNRC input.")
                    aoi_success = False 
            elif aoi_definition_type == "KML":
                if isinstance(aoi_input, str): 
                    aoi_success = self.aoi.define_from_kml_file(aoi_input)
                else: 
                    logger.error("Invalid KML input.")
                    aoi_success = False
            else: 
                logger.error(f"Unknown AOI type: {aoi_definition_type}")
                aoi_success = False
                
        except Exception as e: 
            logger.error(f"AOI setup error: {e}")
            self.aoi = None
            return False
            
        if not aoi_success: 
            self.aoi = None
            logger.error("AOI setup failed.")
            return False
            
        logger.info(f"AOI '{self.aoi.get_display_name()}' defined. Target: {self.aoi.target_mtm_crs}")
        return True

    @handle_errors(SpatiaEngineError, default_return=False)
    def prepare_project_structure(self) -> bool:
        """
        Prepare project directory structure.
        
        Returns:
            bool: True if successful, False otherwise
        """
        logger.debug("PipelineManager prepare_project_structure called.")
        
        if not self.aoi or not self.aoi.target_mtm_crs or not self.project_id: 
            logger.error("Project structure preparation: AOI/MTM/ProjectID missing.")
            return False
        
        mtm_zone_number = get_mtm_short_name(self.aoi.target_mtm_crs)
        date_str_yymmdd = datetime.now().strftime("%y%m%d") 
        
        self.project_main_folder_name = f"{self.project_id}_GIS_extract_MTM{mtm_zone_number}_{date_str_yymmdd}"
        self.project_output_dir = os.path.join(self.output_base_dir, self.project_main_folder_name)
        imagery_subdir_name = f"{self.project_id}_extract_imagery_MTM{mtm_zone_number}_{date_str_yymmdd}"
        self.imagery_output_subdir = os.path.join(self.project_output_dir, imagery_subdir_name)
        self.temp_files_dir = os.path.join(self.project_output_dir, "temp_files") 
        self.output_vector_gpkg_filepath = os.path.join(self.project_output_dir, f"{self.project_main_folder_name}.gpkg")
        
        try:
            ensure_directory(self.project_output_dir)
            ensure_directory(self.imagery_output_subdir)
            ensure_directory(self.temp_files_dir)
            
            logger.info(f"Project folder: {self.project_output_dir}")
            logger.info(f"Imagery subfolder: {self.imagery_output_subdir}")
            logger.info(f"Vector GPKG: {self.output_vector_gpkg_filepath}")
            return True
        except Exception as e:
            logger.error(f"Error creating directories: {e}")
            return False

    @handle_errors(SpatiaEngineError, default_return=False)
    def load_datasources_from_config(self, config_manager: ConfigManager) -> bool:
        """
        Load data sources from configuration.
        
        Args:
            config_manager: Configuration manager instance
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.debug("PipelineManager load_datasources_from_config called.")
        
        
        raw_source_configs = config_manager.get_datasources()
        self.config_manager = config_manager  # Store config manager for later use
        if not raw_source_configs:
            logger.warning("No data source configurations provided.")
            self.list_of_datasources = []
            return True
        self.list_of_datasources = []
        for config_dict in raw_source_configs:
            ds_object = DataSourceFactory.create_datasource(config_dict)
            if ds_object:
                self.list_of_datasources.append(ds_object)
            else:
                logger.warning(f"Failed to create DataSource for config: {config_dict.get('id', 'UNKNOWN_ID')}")
        
        self.list_of_datasources.sort()
        logger.info(f"{len(self.list_of_datasources)} sources initialized/sorted.")
        return True

    @handle_errors(SpatiaEngineError, default_return=None)
    def run(self) -> Optional[List[Dict[str, Any]]]:
        """
        Run the complete pipeline.
        
        Returns:
            List of processing summary dictionaries, or None if failed
        """
        logger.debug("PipelineManager run called.")
        
        if not self.aoi or not self.output_vector_gpkg_filepath or not self.imagery_output_subdir or \
           self.temp_files_dir is None or not self.project_output_dir or not self.aoi.target_mtm_crs:
            logger.error("Pipeline not initialized. Execution cancelled.")
            return None
        
        self.processing_summary = [] 
        # Initialize progress display
        progress = ProgressDisplay(len(self.list_of_datasources))
        progress.start_process("Data Source Processing Pipeline")
        
        logger.info("\n" + "="*25 + " STARTING DATA SOURCE PROCESSING PIPELINE " + "="*25)
        
        # Process AOI layer first
        self._process_aoi_layer()
        
        # Process all data sources
        for i, ds_object in enumerate(self.list_of_datasources, 1):
            source_summary = {
                "id": ds_object.id,
                "name": ds_object.name,
                "type": ds_object.type,
                "enabled": ds_object.enabled,
                "status": "Ignored (not enabled)",
                "items_source_in_aoi_bbox": "N/A",
                "items_processed_final": "N/A",
                "priority_level": ds_object.priority
            }
            
            # Check if enabled
            if not ds_object.enabled:
                logger.info(f"Source {ds_object.id} ({ds_object.name}) disabled in config - ignored")
                self.processing_summary.append(source_summary)
                continue
            
            # Special source (always ignored in main pipeline)
            if ds_object.id == "snrc_index_local_50k":
                logger.debug(f"Special source {ds_object.id} ignored in pipeline")
                continue

            # Start progress step
            progress.start_step(i, f"Processing {ds_object.name}")
            
            # Detailed logging for debugging
            logger.debug(f"Processing source {ds_object.id} (type: {ds_object.type})")
            
            # Visual separator
            log_header_message = (
                f"\n{'-'*78}\n"
                f">>> STARTING SOURCE: {ds_object.name} (ID: {ds_object.id}, Type: {ds_object.type})\n"
                f"{'-'*78}"
            )
            logger.info(log_header_message)
            
            current_step_success = False
            source_summary["status"] = "Fetch Failed"
            
            if self.aoi.combined_geometry_mtm is None:
                logger.error(f"MTM AOI geometry not defined for '{ds_object.name}'.")
                source_summary["status"] = "Failed (MTM AOI missing)"
                progress.complete_step(i, False, "MTM AOI missing")
                self.processing_summary.append(source_summary)
                continue
                continue 
            
            logger.info(f"FETCH step: Calling ds_object.fetch_data...")
            
            # Determine fetch argument based on source type
            if isinstance(ds_object, (MNTLiDARSource, CourbesNiveauSource)):
                fetch_arg = self.aoi
            elif isinstance(ds_object, LocalRasterDataSource):
                fetch_arg = self.aoi
            else:
                fetch_arg = self.aoi.bounds_epsg4326 if self.aoi.bounds_epsg4326 is not None and hasattr(self.aoi.bounds_epsg4326, '__len__') and len(self.aoi.bounds_epsg4326) == 4 else None
            
            if fetch_arg is None and not isinstance(ds_object, (MNTLiDARSource, CourbesNiveauSource, LocalRasterDataSource)):
                logger.error(f"EPSG4326 AOI bounds not available/invalid for '{ds_object.name}'.")
                source_summary["status"] = "Failed (AOI bounds fetch)"
                self.processing_summary.append(source_summary)
                continue
            
            fetched_data_result = ds_object.fetch_data(fetch_arg, self.temp_files_dir)
            logger.info(f"FETCH step: Result for '{ds_object.name}': {fetched_data_result}")
            
            # Count items in source
            if isinstance(fetched_data_result, list):
                source_summary["items_source_in_aoi_bbox"] = len(fetched_data_result) if fetched_data_result else 0
            elif isinstance(fetched_data_result, str) and os.path.exists(fetched_data_result):
                if fetched_data_result.lower().endswith(".geojson"):
                    try:
                        import geopandas as gpd
                        gdf_temp = gpd.read_file(fetched_data_result)
                        source_summary["items_source_in_aoi_bbox"] = len(gdf_temp)
                    except:
                        source_summary["items_source_in_aoi_bbox"] = "GeoJSON created" 
                else:
                    source_summary["items_source_in_aoi_bbox"] = "1 (raster)" 
            elif fetched_data_result is None:
                source_summary["items_source_in_aoi_bbox"] = 0
            else:
                source_summary["items_source_in_aoi_bbox"] = "Invalid path"
            
            source_summary["status"] = "Processing Failed"
            
            # Process based on source type
            if isinstance(ds_object, MNTLiDARSource) and isinstance(fetched_data_result, list) and fetched_data_result:
                valid_tif_paths = [p for p in fetched_data_result if p and os.path.exists(p)]
                source_summary["items_source_in_aoi_bbox"] = f"{len(valid_tif_paths)} MNT tile(s)"
                
                if not valid_tif_paths:
                    logger.error(f"No valid MNT TIF files after fetch for '{ds_object.name}'.")
                else:
                    logger.info(f"Processing {len(valid_tif_paths)} MNT files...")
                    mtm_zone_fn = get_mtm_short_name(self.aoi.target_mtm_crs)
                    date_fn = datetime.now().strftime("%y%m%d")
                    mnt_base_sfx = ds_object.output_name_mnt
                    hs_base_sfx = ds_object.output_name_hillshade
                    
                    # Special naming for single 20k sub-sheet
                    if len(valid_tif_paths) == 1 and self.aoi.subfeuillet_20k_data_gdfs and len(self.aoi.subfeuillet_20k_data_gdfs) == 1 and hasattr(ds_object, 'index_feuillet_column') and ds_object.index_feuillet_column:
                        try: 
                            feuillet_col_name = ds_object.index_feuillet_column 
                            mnt_base_sfx = f"MNT_{self.aoi.subfeuillet_20k_data_gdfs[0][feuillet_col_name].iloc[0]}"
                            hs_base_sfx = f"Hillshade_{self.aoi.subfeuillet_20k_data_gdfs[0][feuillet_col_name].iloc[0]}"
                        except Exception as e_name_mnt:
                            logger.warning(f"Unable to extract 20k sub-sheet name: {e_name_mnt}, using default name.")
                    
                    final_mnt_fn = f"{self.project_id}_{mnt_base_sfx}_MTM{mtm_zone_fn}_{date_fn}.tif"
                    final_mnt_path = os.path.join(self.imagery_output_subdir or "", final_mnt_fn)
                    final_hs_fn = f"{self.project_id}_{hs_base_sfx}_MTM{mtm_zone_fn}_{date_fn}.tif"
                    final_hs_path = os.path.join(self.imagery_output_subdir or "", final_hs_fn)
                    
                    merged_path = merge_reproject_clip_rasters(valid_tif_paths, self.aoi.target_mtm_crs, self.aoi.combined_geometry_mtm, final_mnt_path)
                    if merged_path: 
                        logger.info(f"Final MNT TIF: {merged_path}")
                        source_summary["processed_items_in_aoi"] = f"MNT: {os.path.basename(final_mnt_path)}"
                        hs_ok = generate_hillshade_gdal(merged_path, final_hs_path)
                        if hs_ok:
                            logger.info(f"Hillshade TIF: {final_hs_path}")
                            source_summary["processed_items_in_aoi"] += f", HS: {os.path.basename(final_hs_path)}"
                        else:
                            logger.error(f"Failed to generate hillshade for {merged_path}.")
                        current_step_success = True 
                    else:
                        logger.error(f"Failed to process MNT raster for '{ds_object.name}'.")
                    
                    if hasattr(ds_object, 'temp_raster_files'):
                        delete_temp_files(ds_object.temp_raster_files) 
            
            elif isinstance(ds_object, LocalRasterDataSource) and isinstance(fetched_data_result, str) and os.path.exists(fetched_data_result):
                logger.info(f"Processing local raster: {fetched_data_result}")
                mtm_zone_fn = get_mtm_short_name(self.aoi.target_mtm_crs)
                date_fn = datetime.now().strftime("%y%m%d")
                raster_base = ds_object.output_name_raster
                final_raster_fn = f"{self.project_id}_{raster_base}_MTM{mtm_zone_fn}_{date_fn}.tif"
                final_raster_path = os.path.join(self.imagery_output_subdir or "", final_raster_fn)
                
                processed_path = merge_reproject_clip_rasters([fetched_data_result], self.aoi.target_mtm_crs, self.aoi.combined_geometry_mtm, final_raster_path)
                if processed_path:
                    logger.info(f"Local raster processed: {processed_path}")
                    current_step_success = True
                    source_summary["processed_items_in_aoi"] = f"Raster: {os.path.basename(final_raster_path)}"
                else:
                    logger.error(f"Failed to process local raster '{ds_object.name}'.")
            
            elif fetched_data_result == "IS_AOI_INDEX_LAYER":
                current_step_success = True
                source_summary["status"] = "Index (handled)"
            
            elif isinstance(fetched_data_result, str) and os.path.exists(fetched_data_result): 
                process_success, count_before, count_after = process_vector_data(
                    fetched_data_result, 
                    self.aoi.combined_geometry_mtm, 
                    self.aoi.target_mtm_crs, 
                    self.output_vector_gpkg_filepath, 
                    ds_object.output_layer_name, 
                    delete_input_temp_file=True, 
                    temp_processing_dir=self.temp_files_dir
                )
                current_step_success = process_success
                source_summary["items_source_in_aoi_bbox"] = count_before if count_before != -1 else source_summary["items_source_in_aoi_bbox"]
                if current_step_success:
                    source_summary["processed_items_in_aoi"] = f"{count_after} (of {count_before})"
            
            elif fetched_data_result is None: 
                current_step_success = True
                source_summary["status"] = "Success (no data in AOI)"
                source_summary["items_source_in_aoi_bbox"] = 0
                source_summary["processed_items_in_aoi"] = 0
            
            else:
                current_step_success = False
            
            if current_step_success and source_summary["status"] not in ["Index (handled)", "Success (no data in AOI)"]:
                source_summary["status"] = "Success"
                progress.complete_step(i, True, f"Processed {source_summary['processed_items_in_aoi']}")
            elif not current_step_success and source_summary["status"] not in ["Failed (MTM AOI missing)", "Failed (AOI bounds fetch)"]:
                source_summary["status"] = "Processing Failed"
                progress.complete_step(i, False, "Processing failed")
            else:
                progress.complete_step(i, current_step_success, source_summary["status"])
            
            self.processing_summary.append(source_summary)
            logger.info(f"Final status for '{ds_object.name}': {source_summary['status']}")
        
        progress.finish_process(True, "All data sources processed")
        self.display_summary()
        self.cleanup()
        
        # Generate QGIS project if enabled
        if self.config_manager.get_project_info().get('generate_qgis_project', True):
            self._generate_qgis_project()
        
        logger.info("--- Data source processing pipeline completed ---")
        return self.processing_summary

    def _process_aoi_layer(self) -> None:
        """Process AOI layer."""
        logger.debug("PipelineManager _process_aoi_layer called.")
        
        if not self.aoi or not self.output_vector_gpkg_filepath:
            return 
        
        ds_cfg = next((ds for ds in self.list_of_datasources if ds.id == "snrc_index_local_50k" and ds.enabled), None)
        if ds_cfg:
            logger.info(f"Saving AOI extent (Source: {ds_cfg.name})")
            status = "Failed (AOI Extent Save)"
            items_final = 0
            items_bruts = 0
            
            try:
                if self.aoi.combined_gdf_epsg4326 is not None and self.aoi.target_mtm_crs is not None:
                    items_bruts = 1 if not self.aoi.combined_gdf_epsg4326.empty else 0
                    aoi_to_save = self.aoi.combined_gdf_epsg4326.to_crs(self.aoi.target_mtm_crs)
                    if not aoi_to_save.empty: 
                        aoi_to_save.to_file(self.output_vector_gpkg_filepath, layer=ds_cfg.output_layer_name, driver="GPKG", index=False)
                        logger.info(f"AOI extent saved: '{ds_cfg.output_layer_name}'.")
                        status = "Success"
                        items_final = len(aoi_to_save)
                    else:
                        logger.warning("AOI extent empty after reprojection.")
                        status = "Success (empty)"
                        items_final = 0
                else:
                    logger.error("Unable to save AOI: GDF/CRS missing.")
            except Exception as e:
                logger.error(f"Error saving AOI: {e}")
            
            self.processing_summary.append({
                "id": ds_cfg.id,
                "name": ds_cfg.name,
                "type": ds_cfg.type,
                "enabled": ds_cfg.enabled,
                "status": status,
                "items_source_in_aoi_bbox": items_bruts,
                "items_processed_final": items_final,
                "priority_level": ds_cfg.priority if hasattr(ds_cfg, 'priority') else 999
            })
        else:
            logger.info("SNRC index source not found/enabled.")

    def display_summary(self) -> None:
        """Display processing summary."""
        logger.info("\n\n" + "="*35 + " FINAL PROCESSING SUMMARY " + "="*35 + "\n")
        
        if not self.processing_summary:
            logger.info("No data sources were processed or configured for summary.")
            logger.info("="*150)
            return
        
        col_widths = {"name_id": 55, "type": 22, "status": 28, "raw": 18, "final": 35}
        header_template = "| {:<{w_name}} | {:<{w_type}} | {:<{w_status}} | {:<{w_raw}} | {:<{w_final}} |"
        header_str = header_template.format(
            "Source Name (ID)", "Type", "Final Status", "Items/Features Read", "Items/Result Final AOI",
            w_name=col_widths["name_id"], w_type=col_widths["type"], w_status=col_widths["status"],
            w_raw=col_widths["raw"], w_final=col_widths["final"]
        )
        sep_line = "|{s_name}|{s_type}|{s_status}|{s_raw}|{s_final}|".format(
            s_name="-"*col_widths["name_id"] + "-",
            s_type="-"*col_widths["type"] + "-",
            s_status="-"*col_widths["status"] + "-",
            s_raw="-"*col_widths["raw"] + "-",
            s_final="-"*col_widths["final"] + "-"
        )
        
        logger.info(header_str)
        logger.info(sep_line)
        
        # Sort by priority and special handling for AOI index
        self.processing_summary.sort(key=lambda x: (x.get("id") == "snrc_index_local_50k", x.get("priority_level", 99), x.get("id")))
        
        for item in self.processing_summary:
            name_id_full = f"{item.get('name', 'N/A')} ({item.get('id', 'N/A')})"
            name_id_display = textwrap.shorten(name_id_full, width=col_widths["name_id"] - 1, placeholder="...")
            line_str = header_template.format(
                name_id_display,
                str(item.get('type', 'N/A'))[:col_widths["type" ]-1],
                str(item.get('status', 'N/A'))[:col_widths["status"]-1],
                str(item.get('items_source_in_aoi_bbox', 'N/A'))[:col_widths["raw"]-1],
                str(item.get('processed_items_in_aoi', 'N/A'))[:col_widths["final"]-1],
                w_name=col_widths["name_id"], w_type=col_widths["type"], w_status=col_widths["status"],
                w_raw=col_widths["raw"], w_final=col_widths["final"]
            )
            logger.info(line_str)
        
        logger.info(sep_line + "\n")

    def cleanup(self) -> None:
        """Clean up temporary files."""
        logger.debug("PipelineManager cleanup called.")
        logger.info("Cleaning up temporary files...")
        
        if self.temp_files_dir and os.path.exists(self.temp_files_dir):
            files_in_temp = os.listdir(self.temp_files_dir)
            specific_temp_files_to_delete = [
                os.path.join(self.temp_files_dir, f) for f in files_in_temp
                if f.startswith("temp_filtered_") or f.startswith("temp_indexed_") or
                   f.startswith("temp_wfs_") or f.startswith("temp_reproj_") or
                   f.startswith("temp_unclipped_mosaic_")
            ]
            
            if specific_temp_files_to_delete:
                logger.info(f"Deleting {len(specific_temp_files_to_delete)} specific temporary file(s)...")
                for f_path in specific_temp_files_to_delete:
                    try:
                        os.remove(f_path)
                        logger.debug(f"Specific temporary file deleted: {f_path}")
                    except Exception as e:
                        logger.warning(f"Unable to delete {f_path}: {e}")
            
            try:
                if not os.listdir(self.temp_files_dir): 
                    logger.info(f"Deleting now-empty temporary folder: {self.temp_files_dir}")
                    shutil.rmtree(self.temp_files_dir)
                else:
                    logger.warning(f"Temporary folder {self.temp_files_dir} still contains: {os.listdir(self.temp_files_dir)}.")
            except Exception as e_rm_temp_dir:
                logger.error(f"Error deleting temporary folder {self.temp_files_dir}: {e_rm_temp_dir}")
        else:
            logger.info("No temporary folder specified or folder already deleted.")

    def _generate_qgis_project(self) -> None:
        """
        Generate QGIS project with symbology.
        """
        try:
            logger.info("Generating QGIS project with symbology...")
            
            # Check if output files exist before generating QGIS project
            vector_file_exists = os.path.exists(self.output_vector_gpkg_filepath)
            imagery_dir_exists = os.path.exists(self.imagery_output_subdir)
            
            if not vector_file_exists and not imagery_dir_exists:
                logger.warning("No vector data or imagery found. Skipping QGIS project generation.")
                return
            
            if not vector_file_exists:
                logger.warning(f"Vector GeoPackage file not found: {self.output_vector_gpkg_filepath}")
            
            if not imagery_dir_exists:
                logger.warning(f"Imagery directory not found: {self.imagery_output_subdir}")
            
            # Initialize QGIS manager
            qgis_manager = QGISManager(self.project_output_dir)
            
            # Create project structure and copy styles
            qgis_manager.create_qgis_project_structure()
            qgis_manager.copy_qml_styles()

            # Generate QGIS project
            success = qgis_manager.create_qgis_project_file(
                self.output_vector_gpkg_filepath,
                self.imagery_output_subdir,
                self.config_manager
            )
            
            if success:
                logger.info("QGIS project generated successfully")
                logger.info(f"QGIS project location: {qgis_manager.qgis_project_dir}")
            else:
                logger.warning("Failed to generate QGIS project")
                
        except Exception as e:
            logger.error(f"Error generating QGIS project: {e}")
