#!/usr/bin/env python3
"""
DEPRECATED: Pipeline logging functionality has been consolidated into utils/logging_config.py

This file provides backward compatibility by redirecting imports.
DRY: Functionality absorbed into utils/logging_config.py (Phase 3C)
"""
import warnings

# Issue deprecation warning
warnings.warn(
    "utils/logger.py is deprecated. Use utils/logging_config.py instead. "
    "All functionality has been consolidated there.",
    DeprecationWarning,
    stacklevel=2
)

# Redirect all imports to the consolidated module
from .logging_config import (
    DualLogger,
    PipelineLogger,
    get_pipeline_logger,
    pipeline_run,
    setup_component_logging
)

__all__ = [
    'DualLogger',
    'PipelineLogger', 
    'get_pipeline_logger',
    'pipeline_run',
    'setup_component_logging'
]