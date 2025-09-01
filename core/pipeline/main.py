"""
Main Pipeline Entry Point for SpatiaEngine
"""
import os
import sys
import logging
from typing import Optional, Dict, Any
import yaml

from .pipeline_manager import PipelineManager
from ..config.config_manager import ConfigManager
from ..utils.logging_utils import setup_dual_logging
from ..utils.error_handler import SpatiaEngineError, handle_errors
from ..ui.progress_display import ProgressDisplay

logger = logging.getLogger('spatiaengine.pipeline.main')

@handle_errors(SpatiaEngineError, default_return=False)
def run_pipeline_from_config(config_path: str, 
                           output_base_dir: str = "output_data",
                           custom_crs: Optional[str] = None) -> bool:
    """
    Run the complete pipeline from configuration file.
    
    Args:
        config_path: Path to configuration YAML file
        output_base_dir: Base directory for output
        custom_crs: Custom CRS for projection (optional)
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Initialize progress display
    progress = ProgressDisplay(7)  # 7 main steps
    progress.start_process("SpatiaEngine Pipeline Execution")
    
    logger.info("Starting SpatiaEngine pipeline execution")
    logger.info(f"Configuration file: {config_path}")
    logger.info(f"Output base directory: {output_base_dir}")
    
    try:
        # Step 1: Load configuration
        progress.start_step(1, "Loading configuration")
        config_manager = ConfigManager(config_path)
        project_info = config_manager.get_project_info()
        project_id = project_info.get("id", "DEFAULT_PROJECT")
        
        logger.info(f"Loaded configuration - Project ID: {project_id}")
        progress.complete_step(1, True, f"Project ID: {project_id}")
        
        # Display projection configuration
        projection_config = config_manager.get_projection_config()
        if projection_config.get("target_crs"):
            logger.info(f"Custom projection: {projection_config['target_crs']}")
        
        # Step 2: Create pipeline manager
        progress.start_step(2, "Initializing pipeline manager")
        pipeline = PipelineManager(project_id, output_base_dir, custom_crs)
        progress.complete_step(2, True)
        
        # Step 3: Set up AOI
        progress.start_step(3, "Setting up AOI")
        aoi_config = config_manager.get_aoi_config()
        aoi_type = aoi_config.get("type")
        aoi_definition = aoi_config.get("definition")
        
        if not pipeline.setup_aoi(aoi_type, aoi_definition):
            logger.error("Failed to set up AOI")
            progress.complete_step(3, False, "AOI setup failed")
            return False
        
        progress.complete_step(3, True, f"AOI type: {aoi_type}")
        
        # Step 4: Prepare project structure
        progress.start_step(4, "Preparing project structure")
        if not pipeline.prepare_project_structure():
            logger.error("Failed to prepare project structure")
            progress.complete_step(4, False, "Structure preparation failed")
            return False
        
        progress.complete_step(4, True)
        
        # Step 5: Load data sources
        progress.start_step(5, "Loading data sources")
        if not pipeline.load_datasources_from_config(config_manager):
            logger.error("Failed to load data sources")
            progress.complete_step(5, False, "Data source loading failed")
            return False
        
        progress.complete_step(5, True)
        
        # Step 6: Run pipeline
        progress.start_step(6, "Running data processing pipeline")
        summary = pipeline.run()
        if summary is None:
            logger.error("Pipeline execution failed")
            progress.complete_step(6, False, "Pipeline execution failed")
            return False
        
        progress.complete_step(6, True)
        
        # Step 7: Finalize
        progress.start_step(7, "Finalizing process")
        logger.info("Pipeline execution completed successfully")
        progress.complete_step(7, True)
        
        progress.finish_process(True, "All steps completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return False

def main():
    """Main entry point for command-line execution."""
    import argparse
    
    parser = argparse.ArgumentParser(description="SpatiaEngine GIS Data Processing Pipeline")
    parser.add_argument(
        "--config", 
        "-c", 
        required=True, 
        help="Path to configuration YAML file"
    )
    parser.add_argument(
        "--output-dir", 
        "-o", 
        default="output_data", 
        help="Base directory for output files"
    )
    parser.add_argument(
        "--log-level", 
        "-l", 
        default="INFO", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level"
    )
    parser.add_argument(
        "--custom-crs",
        help="Custom CRS for projection (e.g., EPSG:32188)"
    )
    
    args = parser.parse_args()
    
    # Set up logging
    setup_dual_logging(terminal_level=args.log_level, file_level="DEBUG")
    
    # Run pipeline
    success = run_pipeline_from_config(
        args.config, 
        args.output_dir, 
        args.custom_crs
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()