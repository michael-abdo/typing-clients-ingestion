#!/usr/bin/env python3
"""
Mass Download Feature Module

Provides YouTube channel bulk download functionality with:
- Person/Video database schema
- Channel discovery and enumeration  
- Mass download coordination
- S3 streaming integration
"""

__version__ = "1.0.0"
__author__ = "Mass Download Team"

# Fail-fast imports - validate critical dependencies immediately
try:
    import sys
    import os
    from pathlib import Path
    
    # Add parent directory to path for relative imports
    current_dir = Path(__file__).parent
    parent_dir = current_dir.parent
    if str(parent_dir) not in sys.path:
        sys.path.insert(0, str(parent_dir))
        
except ImportError as e:
    raise ImportError(
        f"CRITICAL: Failed to import required system modules. "
        f"Mass download feature cannot initialize. Error: {e}"
    ) from e

# Validate Python version (fail-fast)
if sys.version_info < (3, 8):
    raise RuntimeError(
        f"CRITICAL: Python 3.8+ required for mass download feature. "
        f"Current version: {sys.version_info.major}.{sys.version_info.minor}"
    )

# Module exports - will be populated as modules are implemented
__all__ = [
    "database_schema",
    "channel_discovery", 
    "input_handler",
    "mass_coordinator",
    "progress_tracker"
]

# Validation flag for testing
_VALIDATION_PASSED = True

def validate_environment():
    """
    Fail-fast environment validation.
    
    Raises:
        RuntimeError: If environment validation fails
    """
    global _VALIDATION_PASSED
    
    try:
        # Check required directories exist
        required_dirs = ['utils', 'config', 'docs']
        for dir_name in required_dirs:
            dir_path = parent_dir / dir_name
            if not dir_path.exists():
                raise FileNotFoundError(f"Required directory missing: {dir_path}")
        
        _VALIDATION_PASSED = True
        return True
        
    except Exception as e:
        _VALIDATION_PASSED = False
        raise RuntimeError(
            f"CRITICAL: Environment validation failed. "
            f"Mass download feature cannot operate safely. Error: {e}"
        ) from e

# Run validation on import (fail-fast)
validate_environment()