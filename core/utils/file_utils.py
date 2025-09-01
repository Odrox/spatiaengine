"""
File Utility Functions for SpatiaEngine
"""
import os
import shutil
import tempfile
from pathlib import Path
from typing import Union, Optional, List
import logging

logger = logging.getLogger('spatiaengine.utils.file')

def resolve_path(path: Union[str, Path]) -> Path:
    """
    Resolve a path with environment variable expansion.
    
    Args:
        path: Path string or Path object
        
    Returns:
        Resolved Path object
    """
    if isinstance(path, Path):
        path_str = str(path)
    else:
        path_str = path
    
    # Expand environment variables
    expanded_path = os.path.expandvars(path_str)
    return Path(expanded_path).resolve()

def ensure_directory(path: Union[str, Path]) -> bool:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path
        
    Returns:
        True if directory exists or was created, False on error
    """
    try:
        path_obj = resolve_path(path)
        path_obj.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Failed to create directory {path}: {e}")
        return False

def safe_delete_file(filepath: Union[str, Path]) -> bool:
    """
    Safely delete a file if it exists.
    
    Args:
        filepath: Path to file to delete
        
    Returns:
        True if file was deleted or didn't exist, False on error
    """
    try:
        path_obj = resolve_path(filepath)
        if path_obj.exists() and path_obj.is_file():
            path_obj.unlink()
            logger.debug(f"Deleted file: {filepath}")
        return True
    except Exception as e:
        logger.warning(f"Failed to delete file {filepath}: {e}")
        return False

def safe_delete_directory(dirpath: Union[str, Path]) -> bool:
    """
    Safely delete a directory if it exists.
    
    Args:
        dirpath: Path to directory to delete
        
    Returns:
        True if directory was deleted or didn't exist, False on error
    """
    try:
        path_obj = resolve_path(dirpath)
        if path_obj.exists() and path_obj.is_dir():
            shutil.rmtree(path_obj)
            logger.debug(f"Deleted directory: {dirpath}")
        return True
    except Exception as e:
        logger.warning(f"Failed to delete directory {dirpath}: {e}")
        return False

def get_temp_dir(prefix: str = "spatiaengine_") -> Path:
    """
    Get a temporary directory for processing.
    
    Args:
        prefix: Prefix for temporary directory name
        
    Returns:
        Path to temporary directory
    """
    temp_dir = Path(tempfile.mkdtemp(prefix=prefix))
    logger.debug(f"Created temporary directory: {temp_dir}")
    return temp_dir

def cleanup_temp_files(file_paths: List[Union[str, Path]]) -> None:
    """
    Clean up temporary files.
    
    Args:
        file_paths: List of file paths to delete
    """
    for filepath in file_paths:
        safe_delete_file(filepath)

def is_safe_path(base_path: Union[str, Path], target_path: Union[str, Path]) -> bool:
    """
    Check if a target path is within a base path (security check).
    
    Args:
        base_path: Base directory path
        target_path: Target file/directory path
        
    Returns:
        True if target path is within base path, False otherwise
    """
    try:
        base = resolve_path(base_path).resolve()
        target = resolve_path(target_path).resolve()
        return target.is_relative_to(base)
    except Exception:
        return False

def copy_file_safe(src: Union[str, Path], dst: Union[str, Path]) -> bool:
    """
    Safely copy a file from source to destination.
    
    Args:
        src: Source file path
        dst: Destination file path
        
    Returns:
        True if copy was successful, False otherwise
    """
    try:
        src_path = resolve_path(src)
        dst_path = resolve_path(dst)
        
        # Ensure destination directory exists
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy file
        shutil.copy2(src_path, dst_path)
        logger.debug(f"Copied {src} to {dst}")
        return True
    except Exception as e:
        logger.error(f"Failed to copy {src} to {dst}: {e}")
        return False

def get_file_size(filepath: Union[str, Path]) -> Optional[int]:
    """
    Get the size of a file in bytes.
    
    Args:
        filepath: Path to file
        
    Returns:
        File size in bytes, or None if file doesn't exist
    """
    try:
        path_obj = resolve_path(filepath)
        if path_obj.exists() and path_obj.is_file():
            return path_obj.stat().st_size
        return None
    except Exception as e:
        logger.warning(f"Failed to get size of {filepath}: {e}")
        return None

def list_files_recursive(directory: Union[str, Path], 
                        pattern: Optional[str] = None) -> List[Path]:
    """
    List all files in a directory recursively.
    
    Args:
        directory: Directory to search
        pattern: Optional file pattern (e.g., "*.gpkg")
        
    Returns:
        List of file paths
    """
    try:
        dir_path = resolve_path(directory)
        if not dir_path.exists() or not dir_path.is_dir():
            return []
        
        if pattern:
            return list(dir_path.rglob(pattern))
        else:
            return [f for f in dir_path.rglob("*") if f.is_file()]
    except Exception as e:
        logger.error(f"Failed to list files in {directory}: {e}")
        return []