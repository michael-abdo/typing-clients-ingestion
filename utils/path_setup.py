#!/usr/bin/env python3
"""
Path Setup Utilities - Centralized path manipulation for tests and scripts
Eliminates duplicate sys.path manipulation across the codebase
"""

import sys
import os
from pathlib import Path


def setup_project_path():
    """
    Add project root to sys.path for imports
    
    Replaces the common pattern:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    Usage:
        from utils.path_setup import setup_project_path
        setup_project_path()
    """
    current_file = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(current_file))
    
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


def setup_test_path():
    """
    Setup path specifically for test files
    Adds both project root and utils directory to path
    """
    current_file = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(current_file))
    utils_dir = os.path.join(project_root, 'utils')
    
    for path in [project_root, utils_dir]:
        if path not in sys.path:
            sys.path.insert(0, path)


def get_project_root() -> Path:
    """
    Get the project root directory as a Path object
    
    Returns:
        Path object pointing to the project root
    """
    current_file = Path(__file__).resolve()
    return current_file.parent.parent


def get_utils_dir() -> Path:
    """
    Get the utils directory as a Path object
    
    Returns:
        Path object pointing to the utils directory
    """
    return get_project_root() / 'utils'


def get_tests_dir() -> Path:
    """
    Get the tests directory as a Path object
    
    Returns:
        Path object pointing to the tests directory
    """
    return get_project_root() / 'tests'


def get_config_dir() -> Path:
    """
    Get the config directory as a Path object
    
    Returns:
        Path object pointing to the config directory
    """
    return get_project_root() / 'config'


def get_outputs_dir() -> Path:
    """
    Get the outputs directory as a Path object
    
    Returns:
        Path object pointing to the outputs directory
    """
    return get_project_root() / 'outputs'


def ensure_directory_exists(directory: Path) -> Path:
    """
    Ensure a directory exists, creating it if necessary
    
    Args:
        directory: Path object for the directory
        
    Returns:
        The same Path object (for chaining)
    """
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def setup_all_paths():
    """
    Setup all common paths for comprehensive access
    Adds project root, utils, tests, and config directories to sys.path
    """
    project_root = get_project_root()
    
    paths_to_add = [
        str(project_root),
        str(project_root / 'utils'),
        str(project_root / 'tests'),
        str(project_root / 'config')
    ]
    
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)


# Convenience function that replaces the most common pattern
def init_project_imports():
    """
    Initialize project for imports - call this at the top of scripts/tests
    
    This replaces the common pattern found in test files:
        import sys
        import os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
    Usage:
        from utils.path_setup import init_project_imports
        init_project_imports()
        
        # Now you can import project modules
        from utils.config import get_config
    """
    setup_project_path()


if __name__ == "__main__":
    # Test the path setup utilities
    print("=== Path Setup Utilities Test ===")
    
    print(f"Project root: {get_project_root()}")
    print(f"Utils directory: {get_utils_dir()}")
    print(f"Tests directory: {get_tests_dir()}")
    print(f"Config directory: {get_config_dir()}")
    print(f"Outputs directory: {get_outputs_dir()}")
    
    print("\nTesting path setup:")
    original_path_length = len(sys.path)
    setup_project_path()
    print(f"✓ Project path added to sys.path ({len(sys.path) - original_path_length} new paths)")
    
    print("\nTesting comprehensive path setup:")
    setup_all_paths()
    print(f"✓ All paths added to sys.path (total: {len(sys.path)} paths)")
    
    print("\n=== Path setup utilities ready for use! ===")