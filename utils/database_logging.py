#!/usr/bin/env python3
"""
Database Operations Logging Module

Provides detailed logging for database migration with fail-fast, fail-loud, fail-safely principles.
All database operations are logged with timestamps, operation details, and performance metrics.
"""

import os
import time
import logging
import json
import traceback
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable

# Setup specialized database logger
def setup_database_logger():
    """Set up specialized logger for database operations."""
    
    # Create logs directory
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Create database-specific log file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d")
    log_file = logs_dir / f"database_migration_{timestamp}.log"
    
    # Configure database logger
    db_logger = logging.getLogger('database_migration')
    db_logger.setLevel(logging.DEBUG)
    
    # Remove existing handlers to avoid duplicates
    db_logger.handlers.clear()
    
    # File handler for detailed logs
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    
    # Console handler for important messages
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Detailed formatter for file
    file_formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Simpler formatter for console
    console_formatter = logging.Formatter(
        '🗄️  [%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%H:%M:%S'
    )
    
    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)
    
    db_logger.addHandler(file_handler)
    db_logger.addHandler(console_handler)
    
    # Prevent propagation to root logger
    db_logger.propagate = False
    
    return db_logger

# Global database logger
db_logger = setup_database_logger()

class DatabaseOperationLogger:
    """Context manager for logging database operations with performance tracking."""
    
    def __init__(self, operation: str, details: Optional[Dict[str, Any]] = None):
        """Initialize operation logger."""
        self.operation = operation
        self.details = details or {}
        self.start_time = None
        self.end_time = None
        
    def __enter__(self):
        """Start operation logging."""
        self.start_time = time.time()
        
        db_logger.info(f"🚀 STARTING: {self.operation}")
        if self.details:
            db_logger.debug(f"📝 Details: {json.dumps(self.details, indent=2)}")
        
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """End operation logging."""
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        
        if exc_type is None:
            db_logger.info(f"✅ COMPLETED: {self.operation} (took {duration:.3f}s)")
        else:
            db_logger.error(f"❌ FAILED: {self.operation} (took {duration:.3f}s)")
            db_logger.error(f"💥 Error: {exc_type.__name__}: {exc_val}")
            db_logger.debug(f"📋 Traceback: {traceback.format_exc()}")
        
        # Log performance warning for slow operations
        if duration > 5.0:
            db_logger.warning(f"⚠️ SLOW OPERATION: {self.operation} took {duration:.3f}s")

def log_database_operation(operation_name: str, include_args: bool = True):
    """Decorator for logging database operations."""
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Prepare operation details
            details = {}
            if include_args:
                # Safely include arguments (excluding sensitive data)
                safe_args = []
                for arg in args:
                    if isinstance(arg, str) and len(arg) > 100:
                        safe_args.append(f"{arg[:100]}... (truncated)")
                    elif 'password' in str(arg).lower():
                        safe_args.append("[REDACTED]")
                    else:
                        safe_args.append(str(arg)[:200])
                
                safe_kwargs = {}
                for k, v in kwargs.items():
                    if 'password' in k.lower():
                        safe_kwargs[k] = "[REDACTED]"
                    elif isinstance(v, str) and len(v) > 100:
                        safe_kwargs[k] = f"{v[:100]}... (truncated)"
                    else:
                        safe_kwargs[k] = str(v)[:200]
                
                details = {
                    'function': func.__name__,
                    'args': safe_args,
                    'kwargs': safe_kwargs
                }
            
            with DatabaseOperationLogger(operation_name, details):
                return func(*args, **kwargs)
        
        return wrapper
    return decorator

def log_data_consistency_check(csv_count: int, db_count: int, table_name: str):
    """Log data consistency check results."""
    
    if csv_count == db_count:
        db_logger.info(f"✅ CONSISTENCY CHECK PASSED: {table_name} - CSV: {csv_count}, DB: {db_count}")
    else:
        db_logger.error(f"❌ CONSISTENCY CHECK FAILED: {table_name} - CSV: {csv_count}, DB: {db_count}")
        db_logger.error(f"💥 CRITICAL: Data mismatch detected - failing fast!")
        raise ValueError(f"Data consistency check failed for {table_name}")

def log_dual_write_comparison(csv_data: Dict, db_data: Dict, record_id: Any):
    """Log dual-write comparison results."""
    
    mismatches = []
    
    for key in csv_data.keys():
        csv_val = csv_data.get(key)
        db_val = db_data.get(key)
        
        if csv_val != db_val:
            mismatches.append({
                'field': key,
                'csv_value': str(csv_val)[:100],
                'db_value': str(db_val)[:100]
            })
    
    if mismatches:
        db_logger.error(f"❌ DUAL-WRITE MISMATCH: Record {record_id}")
        for mismatch in mismatches:
            db_logger.error(f"   Field '{mismatch['field']}': CSV='{mismatch['csv_value']}' vs DB='{mismatch['db_value']}'")
        
        # FAIL LOUD - this is critical
        raise ValueError(f"Dual-write validation failed for record {record_id}")
    else:
        db_logger.debug(f"✅ DUAL-WRITE MATCH: Record {record_id}")

def log_fallback_usage(operation: str, reason: str):
    """Log database fallback to CSV usage."""
    
    db_logger.warning(f"🔄 FALLBACK TO CSV: {operation}")
    db_logger.warning(f"   Reason: {reason}")
    db_logger.warning(f"   🚨 DATABASE OPERATION FAILED - USING CSV BACKUP")

def log_migration_phase(phase: str, status: str, details: Optional[str] = None):
    """Log migration phase status."""
    
    status_emoji = {
        'starting': '🚀',
        'in_progress': '⏳',
        'completed': '✅',
        'failed': '❌',
        'rollback': '🔄'
    }
    
    emoji = status_emoji.get(status, '📝')
    
    db_logger.info(f"{emoji} MIGRATION PHASE: {phase} - {status.upper()}")
    if details:
        db_logger.info(f"   Details: {details}")

def log_schema_operation(table_name: str, operation: str, schema: Optional[Dict] = None):
    """Log database schema operations."""
    
    db_logger.info(f"🏗️  SCHEMA OPERATION: {operation} - {table_name}")
    if schema:
        db_logger.debug(f"📋 Schema: {json.dumps(schema, indent=2)}")

def log_performance_metrics(operation: str, records_processed: int, duration: float):
    """Log performance metrics for database operations."""
    
    records_per_second = records_processed / duration if duration > 0 else 0
    
    db_logger.info(f"📊 PERFORMANCE: {operation}")
    db_logger.info(f"   Records processed: {records_processed}")
    db_logger.info(f"   Duration: {duration:.3f}s")
    db_logger.info(f"   Rate: {records_per_second:.2f} records/second")
    
    # Performance warnings
    if records_per_second < 10 and records_processed > 100:
        db_logger.warning(f"⚠️ SLOW PERFORMANCE: {records_per_second:.2f} records/second")

def create_migration_summary():
    """Create migration summary log."""
    
    timestamp = datetime.now().isoformat()
    
    summary = {
        'migration_timestamp': timestamp,
        'database_config': {
            'host': 'localhost',
            'port': 5432,
            'database': 'typing_clients_uuid',
            'user': 'migration_user'
        },
        'log_file': f"logs/database_migration_{datetime.now().strftime('%Y%m%d')}.log",
        'emergency_rollback_available': os.path.exists('emergency_csv_rollback.py')
    }
    
    db_logger.info("📋 MIGRATION SUMMARY")
    db_logger.info(json.dumps(summary, indent=2))
    
    return summary

def log_emergency_situation(situation: str, action: str):
    """Log emergency situations during migration."""
    
    db_logger.critical(f"🚨 EMERGENCY: {situation}")
    db_logger.critical(f"🛠️  ACTION TAKEN: {action}")
    db_logger.critical(f"⏰ TIMESTAMP: {datetime.now().isoformat()}")

# Test the logging system
if __name__ == "__main__":
    # Test basic logging
    db_logger.info("🧪 Testing database logging system")
    
    # Test operation logger
    with DatabaseOperationLogger("Test Operation", {"test": "data"}):
        time.sleep(0.1)  # Simulate work
        db_logger.info("Work completed")
    
    # Test decorator
    @log_database_operation("Test Function")
    def test_function(param1, param2="default"):
        return f"Processed {param1} with {param2}"
    
    result = test_function("test_data", param2="custom_value")
    db_logger.info(f"Function result: {result}")
    
    # Test consistency check (this should pass)
    log_data_consistency_check(100, 100, "test_table")
    
    # Test migration phase logging
    log_migration_phase("Phase 1", "starting", "Dual-write implementation")
    log_migration_phase("Phase 1", "completed", "All tests passed")
    
    # Test performance logging
    log_performance_metrics("CSV Import", 1000, 2.5)
    
    # Create migration summary
    create_migration_summary()
    
    db_logger.info("✅ Database logging system test completed")