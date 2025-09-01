"""
QGIS Manager for SpatiaEngine - Handles QGIS project creation and symbology using the QGIS API.
"""
import os
import logging
import shutil
import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple

from qgis.core import (
    QgsApplication,
    QgsProject,
    QgsVectorLayer,
    QgsRasterLayer,
    QgsCoordinateReferenceSystem,
)

from ..utils.error_handler import SpatiaEngineError, handle_errors
from ..config.config_manager import ConfigManager

logger = logging.getLogger('spatiaengine.qgis')

# --- QGIS Application Management ---
def _init_qgis_app():
    """Initializes QgsApplication if not already running."""
    if QgsApplication.instance() is None:
        logger.info("Initializing QgsApplication...")
        qgis_prefix_path = os.getenv("QGIS_PREFIX_PATH")
        if not qgis_prefix_path:
            logger.critical("CRITICAL: QGIS_PREFIX_PATH environment variable not set.")
            logger.critical("Please set this variable to point to your QGIS installation prefix.")
            raise SpatiaEngineError("QGIS environment not configured. Exiting.")

        QgsApplication.setPrefixPath(qgis_prefix_path, True)
        app = QgsApplication([], False)
        app.initQgis()
        return app
    logger.debug("QgsApplication already initialized.")
    return QgsApplication.instance()

class QGISManager:
    """Manages QGIS project creation and symbology application via the QGIS API."""

    def __init__(self, project_output_dir: str, qml_styles_dir: Optional[str] = None):
        self.project_output_dir = Path(project_output_dir)
        
        # Use environment variables for the default QML path
        default_qml_path = os.path.expandvars("$GIS_DB_PATH/QML")
        
        if qml_styles_dir:
            self.source_qml_styles_dir = Path(os.path.expandvars(qml_styles_dir))
        else:
            self.source_qml_styles_dir = Path(default_qml_path)

        self.qgis_project_dir = self.project_output_dir / "qgis_project"
        self.qml_output_dir = self.qgis_project_dir / "qml_styles"
        logger.info(f"QGISManager initialized for project: {self.project_output_dir}")

    def create_qgis_project_structure(self) -> bool:
        """Creates the necessary directory structure for the QGIS project."""
        logger.info("Creating QGIS project directory structure...")
        try:
            self.qgis_project_dir.mkdir(exist_ok=True)
            self.qml_output_dir.mkdir(exist_ok=True)
            logger.debug(f"Ensured directory exists: {self.qgis_project_dir}")
            logger.debug(f"Ensured directory exists: {self.qml_output_dir}")
            return True
        except Exception as e:
            logger.error(f"Failed to create project structure: {e}")
            return False

    def copy_qml_styles(self) -> bool:
        """Copies QML style files from the source directory to the project's style directory."""
        logger.info(f"Copying QML styles from {self.source_qml_styles_dir}")
        if not self.source_qml_styles_dir or not self.source_qml_styles_dir.exists():
            logger.warning("Source QML styles directory not found. Skipping copy.")
            return False
        try:
            for qml_file in self.source_qml_styles_dir.glob('*.qml'):
                shutil.copy(qml_file, self.qml_output_dir)
            logger.info(f"Successfully copied QML files to {self.qml_output_dir}")
            return True
        except Exception as e:
            logger.error(f"Failed to copy QML styles: {e}")
            return False

    @handle_errors(SpatiaEngineError, default_return=False)
    def create_qgis_project_file(self, vector_gpkg_path: str, imagery_dir: str, config_manager: ConfigManager) -> bool:
        gpkg_path = Path(vector_gpkg_path)
        if not gpkg_path.exists():
            logger.error(f"GeoPackage not found at: {gpkg_path}")
            return False

        project = QgsProject.instance()
        project.clear()
        project.setTitle(gpkg_path.stem)
        
        self._add_layers_from_gpkg(project, gpkg_path)
        self._add_raster_layers_from_dir(project, Path(imagery_dir), config_manager)

        if project.layerTreeRoot().children():
            first_layer = project.layerTreeRoot().children()[0].layer()
            if first_layer and first_layer.crs().isValid():
                project.setCrs(first_layer.crs())
            else:
                project.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))
        
        project_path = self.qgis_project_dir / f"{gpkg_path.stem}.qgz"
        project.setFileName(str(project_path))

        if project.write():
            logger.info(f"QGIS project successfully created at: {project_path}")
            project.clear()
            return True
        else:
            logger.error(f"Failed to write QGIS project file to: {project_path}")
            return False

    def _add_layers_from_gpkg(self, project: QgsProject, gpkg_path: Path):
        """Adds vector and tile layers from a GeoPackage to the project."""
        logger.info(f"Scanning GeoPackage: {gpkg_path}")
        
        vector_layers = self._list_gpkg_features(gpkg_path)
        logger.info(f"Found {len(vector_layers)} vector layers to process.")
        for table_name, _ in vector_layers:
            logger.info(f"--> Processing vector layer: {table_name}")
            uri = f"{gpkg_path.resolve()}|layername={table_name}"
            layer = QgsVectorLayer(uri, table_name, "ogr")
            
            logger.debug(f"    Layer created for {table_name}. Checking validity...")
            if layer.isValid():
                project.addMapLayer(layer)
                self._apply_qml_style(layer, table_name)
                logger.info(f"    [SUCCESS] Loaded and styled: {table_name}")
            else:
                logger.warning(f"    [FAILURE] Invalid vector layer: {table_name}")
            del layer

        tile_layers = self._list_gpkg_tiles(gpkg_path)
        logger.info(f"Found {len(tile_layers)} tile layers to process.")
        for table_name in tile_layers:
            logger.info(f"--> Processing tile layer: {table_name}")
            uri = f"GPKG:{gpkg_path.resolve()}:{table_name}"
            layer = QgsRasterLayer(uri, table_name, "gdal")

            logger.debug(f"    Layer created for {table_name}. Checking validity...")
            if layer.isValid():
                project.addMapLayer(layer)
                self._apply_qml_style(layer, table_name)
                logger.info(f"    [SUCCESS] Loaded and styled: {table_name}")
            else:
                logger.warning(f"    [FAILURE] Invalid tile layer: {table_name}")
            del layer

    def _add_raster_layers_from_dir(self, project: QgsProject, imagery_dir: Path, config_manager: ConfigManager):
        if not imagery_dir.exists():
            return
        logger.info(f"Scanning for external rasters in: {imagery_dir}")

        # Create a mapping from a stable part of the name (from config) to qml_id
        raster_style_map = {}
        raster_sources = config_manager.get_datasources(type_filter=['local_raster', 'mnt_lidar_quebec'])
        for source in raster_sources:
            qml_id = source.get('qml_id')
            if qml_id:
                # Map all possible output names to the intended qml_id
                if source.get('output_name_raster'):
                    raster_style_map[source['output_name_raster']] = qml_id
                if source.get('output_name_mnt'):
                    raster_style_map[source['output_name_mnt']] = qml_id
                if source.get('output_name_hillshade'):
                    # Assuming hillshade might use the same QML or a generic one if not specified
                    raster_style_map[source['output_name_hillshade']] = qml_id

        for file_path in imagery_dir.glob('*.tif'):
            file_stem = file_path.stem
            layer = QgsRasterLayer(str(file_path.resolve()), file_stem, "gdal")
            
            if layer.isValid():
                project.addMapLayer(layer)
                
                # Default style is the filename itself
                style_name = file_stem
                # Check if any key from our map is a substring of the filename
                for key, qml_id_val in raster_style_map.items():
                    if key in file_stem:
                        style_name = qml_id_val
                        break  # Found the correct style, no need to check further

                self._apply_qml_style(layer, style_name)
                log_msg = f"Loaded raster: {file_stem}"
                if style_name != file_stem:
                    log_msg += f" (Attempting to style with '{style_name}.qml')"
                else:
                    log_msg += f" (No specific QML ID found, using default name for style)"
                logger.info(log_msg)
            else:
                logger.warning(f"Failed to load external raster: {file_path}")
            del layer

    def _apply_qml_style(self, layer, layer_name: str):
        qml_path = self.qml_output_dir / f"{layer_name}.qml"
        if qml_path.exists():
            try:
                layer.loadNamedStyle(str(qml_path))
            except Exception as e:
                logger.error(f"Exception applying QML style to {layer_name}: {e}")

    def _list_gpkg_features(self, gpkg_path: Path) -> List[Tuple[str, str]]:
        try:
            with sqlite3.connect(str(gpkg_path)) as con:
                c = con.cursor()
                c.execute("PRAGMA table_info(gpkg_geometry_columns)")
                cols = [row[1].lower() for row in c.fetchall()]
                geom_col_name = "column_name" if "column_name" in cols else "geometry_column"
                sql = f'''SELECT c.table_name, gc.{geom_col_name} FROM gpkg_contents c JOIN gpkg_geometry_columns gc ON c.table_name = gc.table_name WHERE LOWER(c.data_type) = \'features\' ORDER BY c.table_name'''
                return con.execute(sql).fetchall()
        except Exception as e:
            logger.error(f"Could not list features from {gpkg_path}: {e}")
            return []

    def _list_gpkg_tiles(self, gpkg_path: Path) -> List[str]:
        try:
            with sqlite3.connect(str(gpkg_path)) as con:
                sql = "SELECT table_name FROM gpkg_contents WHERE LOWER(data_type) IN ('tiles', 'tile pyramid user data') ORDER BY table_name"
                return [row[0] for row in con.execute(sql).fetchall()]
        except Exception as e:
            logger.error(f"Could not list tiles from {gpkg_path}: {e}")
            return []
