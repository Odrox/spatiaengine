import typer
import sys
import os
import logging
from qgis.core import QgsApplication
from core.pipeline.main import run_pipeline_from_config
from core.utils.logging_utils import setup_dual_logging

app = typer.Typer()

def _init_qgis_app():
    """Initializes QgsApplication if not already running."""
    if QgsApplication.instance() is None:
        qgis_prefix_path = os.getenv("QGIS_PREFIX_PATH")
        if not qgis_prefix_path:
            # Use logging if available, otherwise print
            logging.critical("CRITICAL: QGIS_PREFIX_PATH environment variable not set.")
            logging.critical("Please set this variable to point to your QGIS installation prefix.")
            sys.exit("QGIS environment not configured. Exiting.")

        QgsApplication.setPrefixPath(qgis_prefix_path, True)
        qgs = QgsApplication([], False)
        qgs.initQgis()
        return qgs
    return QgsApplication.instance()

@app.command()
def run(
    config_file: str = typer.Argument(
        "config/sources.yaml", 
        help="Path to the configuration YAML file."
    ),
    output_dir: str = typer.Option(
        "output_data", 
        "--output-dir", 
        "-o", 
        help="Base directory for output files."
    ),
    log_level: str = typer.Option(
        "INFO", 
        "--log-level", 
        "-l", 
        help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)."
    ),
    custom_crs: str = typer.Option(
        None, 
        "--custom-crs", 
        help="Custom CRS for projection (e.g., EPSG:32188)."
    )
):
    """
    Run the SpatiaEngine GIS Data Processing Pipeline.
    """
    setup_dual_logging(terminal_level=log_level.upper(), file_level="DEBUG")
    
    qgs = _init_qgis_app()
    
    try:
        success = run_pipeline_from_config(
            config_path=config_file,
            output_base_dir=output_dir,
            custom_crs=custom_crs
        )
        
        if not success:
            print("Pipeline execution failed. Check logs for details.")
            raise typer.Exit(code=1)
        
        print("Pipeline executed successfully.")
    finally:
        # Ensure QGIS application is properly closed to prevent crashes
        if qgs is not None:
            qgs.exitQgis()

if __name__ == "__main__":
    app()
