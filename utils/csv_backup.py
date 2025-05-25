#!/usr/bin/env python3
"""
CSV backup utilities for maintaining data integrity.
Provides automatic backup functionality for CSV files.
"""
import os
import shutil
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, Union, List, Dict, Any
import gzip

try:
    from logging_config import get_logger
    from config import get_config
    from file_lock import FileLock
except ImportError:
    from .logging_config import get_logger
    from .config import get_config
    from .file_lock import FileLock

# Setup module logger
logger = get_logger(__name__)

# Get configuration
config = get_config()


class CSVBackupManager:
    """Manages automatic backups of CSV files"""
    
    def __init__(self, backup_dir: Optional[Union[str, Path]] = None, 
                 max_backups: Optional[int] = None,
                 compress_backups: Optional[bool] = None):
        """
        Initialize CSV backup manager.
        
        Args:
            backup_dir: Directory to store backups (defaults to 'backups' in project root)
            max_backups: Maximum number of backups to keep per file (defaults from config)
            compress_backups: Whether to compress backups with gzip (defaults from config)
        """
        self.backup_dir = Path(backup_dir) if backup_dir else Path(config.get('csv_backup.backup_dir', 'backups'))
        self.max_backups = max_backups if max_backups is not None else config.get('csv_backup.max_backups', 10)
        self.compress_backups = compress_backups if compress_backups is not None else config.get('csv_backup.compress', True)
        
        # Create backup directory if it doesn't exist
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"CSV backup manager initialized with directory: {self.backup_dir}")
    
    def backup_file(self, csv_file: Union[str, Path], description: Optional[str] = None) -> Optional[Path]:
        """
        Create a backup of a CSV file.
        
        Args:
            csv_file: Path to the CSV file to backup
            description: Optional description for the backup (e.g., "before_update", "daily")
        
        Returns:
            Path to the created backup file, or None if backup failed
        """
        csv_path = Path(csv_file)
        
        # Check if file exists
        if not csv_path.exists():
            logger.warning(f"Cannot backup non-existent file: {csv_path}")
            return None
        
        # Create subdirectory for this file's backups
        file_backup_dir = self.backup_dir / csv_path.stem
        file_backup_dir.mkdir(exist_ok=True)
        
        # Generate backup filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{csv_path.stem}_{timestamp}"
        
        if description:
            # Sanitize description for filename
            safe_description = description.replace(' ', '_').replace('/', '_')
            backup_name += f"_{safe_description}"
        
        # Determine file extension
        if self.compress_backups:
            backup_name += ".csv.gz"
        else:
            backup_name += ".csv"
        
        backup_path = file_backup_dir / backup_name
        
        try:
            # Use file lock to ensure safe read
            with FileLock(f"{csv_path}.backup_lock", timeout=30):
                if self.compress_backups:
                    # Compress while copying
                    with open(csv_path, 'rb') as f_in:
                        with gzip.open(backup_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    logger.info(f"Created compressed backup: {backup_path}")
                else:
                    # Simple copy
                    shutil.copy2(csv_path, backup_path)
                    logger.info(f"Created backup: {backup_path}")
            
            # Clean up old backups
            self._cleanup_old_backups(file_backup_dir)
            
            return backup_path
            
        except Exception as e:
            logger.error(f"Failed to backup {csv_path}: {e}")
            return None
    
    def _cleanup_old_backups(self, backup_dir: Path):
        """Remove old backups exceeding max_backups limit"""
        if self.max_backups <= 0:
            return
        
        # List all backups, sorted by modification time (oldest first)
        backups = sorted(backup_dir.glob("*.csv*"), key=lambda p: p.stat().st_mtime)
        
        # Remove oldest backups if we exceed the limit
        while len(backups) > self.max_backups:
            oldest = backups.pop(0)
            try:
                oldest.unlink()
                logger.info(f"Removed old backup: {oldest}")
            except Exception as e:
                logger.error(f"Failed to remove old backup {oldest}: {e}")
    
    def restore_backup(self, backup_path: Union[str, Path], target_path: Union[str, Path], 
                      create_restore_backup: bool = True) -> bool:
        """
        Restore a CSV file from backup.
        
        Args:
            backup_path: Path to the backup file
            target_path: Path where to restore the file
            create_restore_backup: Whether to backup current file before restoring
        
        Returns:
            True if restore was successful, False otherwise
        """
        backup_path = Path(backup_path)
        target_path = Path(target_path)
        
        if not backup_path.exists():
            logger.error(f"Backup file not found: {backup_path}")
            return False
        
        try:
            # Create backup of current file if requested and it exists
            if create_restore_backup and target_path.exists():
                self.backup_file(target_path, description="before_restore")
            
            # Use file lock for safe write
            with FileLock(f"{target_path}.restore_lock", timeout=30):
                if backup_path.suffix == '.gz':
                    # Decompress while restoring
                    with gzip.open(backup_path, 'rb') as f_in:
                        with open(target_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    logger.info(f"Restored from compressed backup: {backup_path} -> {target_path}")
                else:
                    # Simple copy
                    shutil.copy2(backup_path, target_path)
                    logger.info(f"Restored from backup: {backup_path} -> {target_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore backup {backup_path}: {e}")
            return False
    
    def list_backups(self, csv_file: Union[str, Path]) -> List[Dict[str, Any]]:
        """
        List all available backups for a CSV file.
        
        Args:
            csv_file: Path to the CSV file
        
        Returns:
            List of backup info dictionaries with keys: path, size, created, compressed
        """
        csv_path = Path(csv_file)
        file_backup_dir = self.backup_dir / csv_path.stem
        
        if not file_backup_dir.exists():
            return []
        
        backups = []
        for backup_path in sorted(file_backup_dir.glob("*.csv*"), 
                                 key=lambda p: p.stat().st_mtime, 
                                 reverse=True):
            stat = backup_path.stat()
            backups.append({
                'path': str(backup_path),
                'size': stat.st_size,
                'created': datetime.fromtimestamp(stat.st_mtime),
                'compressed': backup_path.suffix == '.gz'
            })
        
        return backups
    
    def auto_backup_before_write(self, csv_file: Union[str, Path]) -> Optional[Path]:
        """
        Convenience method to create automatic backup before writing to CSV.
        
        Args:
            csv_file: Path to the CSV file
        
        Returns:
            Path to backup file or None
        """
        return self.backup_file(csv_file, description="auto_before_write")


# Global backup manager instance
_backup_manager = None


def get_backup_manager() -> CSVBackupManager:
    """Get the global backup manager instance"""
    global _backup_manager
    if _backup_manager is None:
        _backup_manager = CSVBackupManager()
    return _backup_manager


def backup_csv(csv_file: Union[str, Path], description: Optional[str] = None) -> Optional[Path]:
    """
    Quick function to backup a CSV file.
    
    Args:
        csv_file: Path to CSV file
        description: Optional backup description
    
    Returns:
        Path to backup or None
    """
    manager = get_backup_manager()
    return manager.backup_file(csv_file, description)


def restore_csv(backup_path: Union[str, Path], target_path: Union[str, Path]) -> bool:
    """
    Quick function to restore a CSV from backup.
    
    Args:
        backup_path: Path to backup file
        target_path: Where to restore the file
    
    Returns:
        True if successful
    """
    manager = get_backup_manager()
    return manager.restore_backup(backup_path, target_path)


if __name__ == "__main__":
    # Test the backup system
    import tempfile
    
    logger.info("Testing CSV backup system...")
    
    # Create a test CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        test_file = Path(f.name)
        f.write("name,value\n")
        f.write("test1,100\n")
        f.write("test2,200\n")
    
    try:
        # Test backup
        manager = CSVBackupManager(backup_dir="test_backups", max_backups=3)
        
        # Create multiple backups
        for i in range(5):
            backup_path = manager.backup_file(test_file, description=f"test_{i}")
            if backup_path:
                logger.info(f"Created backup {i}: {backup_path}")
            time.sleep(1)  # Ensure different timestamps
        
        # List backups
        backups = manager.list_backups(test_file)
        logger.info(f"Found {len(backups)} backups (should be max 3)")
        
        for backup in backups:
            logger.info(f"  - {backup['created']}: {backup['path']} "
                       f"({backup['size']} bytes, compressed={backup['compressed']})")
        
        # Test restore
        if backups:
            restore_target = test_file.with_suffix('.restored.csv')
            if manager.restore_backup(backups[0]['path'], restore_target):
                logger.info(f"Successfully restored to {restore_target}")
                
                # Verify content
                with open(restore_target, 'r') as f:
                    content = f.read()
                    logger.info(f"Restored content:\n{content}")
                
                restore_target.unlink()
        
        logger.info("CSV backup system test completed successfully!")
        
    finally:
        # Cleanup
        test_file.unlink()
        shutil.rmtree("test_backups", ignore_errors=True)