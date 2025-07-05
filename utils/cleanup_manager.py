#!/usr/bin/env python3
"""
Cleanup Manager - Centralized file cleanup functionality
Consolidates all cleanup scripts into a single, configurable module
"""

import os
import shutil
import glob
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime, timedelta

try:
    from config import get_config
    from logging_config import get_logger
except ImportError:
    from .config import get_config
    from .logging_config import get_logger

# Setup module logger
logger = get_logger(__name__)

class CleanupManager:
    """Centralized cleanup manager for temporary files, backups, and directories"""
    
    def __init__(self, base_dir: Optional[str] = None):
        """
        Initialize cleanup manager
        
        Args:
            base_dir: Base directory for cleanup operations (defaults to current working directory)
        """
        self.base_dir = Path(base_dir) if base_dir else Path.cwd()
        self.config = get_config()
        self.cleanup_stats = {
            'files_removed': 0,
            'dirs_removed': 0,
            'errors': 0,
            'total_size_freed': 0
        }
    
    def cleanup_temporary_files(self, file_patterns: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Remove temporary files based on patterns
        
        Args:
            file_patterns: List of file patterns to remove (defaults from config)
            
        Returns:
            Dictionary with cleanup statistics
        """
        if file_patterns is None:
            file_patterns = self.config.get('cleanup.temp_file_patterns', [
                'temp_*.py',
                '*_temp.py', 
                'cleanup_*.py',
                'execute_cleanup.py',
                'simple_cleanup.py',
                '*.tmp',
                '*.temp',
                '__pycache__',
                '*.pyc'
            ])
        
        logger.info(f"🧹 Cleaning up temporary files with patterns: {file_patterns}")
        
        removed_files = []
        total_size = 0
        
        for pattern in file_patterns:
            matches = glob.glob(str(self.base_dir / pattern), recursive=True)
            for file_path in matches:
                file_path = Path(file_path)
                if file_path.exists():
                    try:
                        if file_path.is_file():
                            size = file_path.stat().st_size
                            file_path.unlink()
                            removed_files.append(str(file_path))
                            total_size += size
                            logger.info(f"✅ Removed file: {file_path}")
                        elif file_path.is_dir() and file_path.name == '__pycache__':
                            shutil.rmtree(file_path)
                            removed_files.append(str(file_path))
                            logger.info(f"✅ Removed directory: {file_path}")
                    except Exception as e:
                        logger.error(f"❌ Failed to remove {file_path}: {e}")
                        self.cleanup_stats['errors'] += 1
        
        self.cleanup_stats['files_removed'] += len(removed_files)
        self.cleanup_stats['total_size_freed'] += total_size
        
        logger.info(f"🎯 Temporary cleanup complete: {len(removed_files)} items removed, {total_size / 1024:.1f} KB freed")
        
        return {
            'files_removed': len(removed_files),
            'total_size_freed': total_size,
            'removed_items': removed_files
        }
    
    def cleanup_backup_files(self, older_than_days: int = 30, backup_patterns: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Remove old backup files
        
        Args:
            older_than_days: Remove backups older than this many days
            backup_patterns: List of backup file patterns
            
        Returns:
            Dictionary with cleanup statistics
        """
        if backup_patterns is None:
            backup_patterns = self.config.get('cleanup.backup_patterns', [
                '*.backup',
                '*.backup_*',
                'backup_*',
                'backups/**/*.gz',
                '*.csv.backup_*'
            ])
        
        logger.info(f"🗄️ Cleaning up backup files older than {older_than_days} days")
        
        cutoff_date = datetime.now() - timedelta(days=older_than_days)
        removed_files = []
        total_size = 0
        
        for pattern in backup_patterns:
            matches = glob.glob(str(self.base_dir / pattern), recursive=True)
            for file_path in matches:
                file_path = Path(file_path)
                if file_path.exists() and file_path.is_file():
                    try:
                        # Check file modification time
                        mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if mod_time < cutoff_date:
                            size = file_path.stat().st_size
                            file_path.unlink()
                            removed_files.append(str(file_path))
                            total_size += size
                            logger.info(f"✅ Removed old backup: {file_path}")
                    except Exception as e:
                        logger.error(f"❌ Failed to remove backup {file_path}: {e}")
                        self.cleanup_stats['errors'] += 1
        
        self.cleanup_stats['files_removed'] += len(removed_files)
        self.cleanup_stats['total_size_freed'] += total_size
        
        logger.info(f"🎯 Backup cleanup complete: {len(removed_files)} files removed, {total_size / 1024:.1f} KB freed")
        
        return {
            'files_removed': len(removed_files),
            'total_size_freed': total_size,
            'removed_items': removed_files
        }
    
    def cleanup_empty_directories(self, base_dirs: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Remove empty directories
        
        Args:
            base_dirs: List of base directories to check (defaults to common directories)
            
        Returns:
            Dictionary with cleanup statistics
        """
        if base_dirs is None:
            base_dirs = self.config.get('cleanup.empty_dir_patterns', [
                'cache',
                'tmp',
                'temp',
                'logs',
                'html_cache'
            ])
        
        logger.info(f"📁 Cleaning up empty directories in: {base_dirs}")
        
        removed_dirs = []
        
        for base_dir in base_dirs:
            dir_path = self.base_dir / base_dir
            if dir_path.exists() and dir_path.is_dir():
                try:
                    # Walk directory tree bottom-up to remove empty dirs
                    for root, dirs, files in os.walk(dir_path, topdown=False):
                        root_path = Path(root)
                        if not files and not dirs:  # Directory is empty
                            root_path.rmdir()
                            removed_dirs.append(str(root_path))
                            logger.info(f"✅ Removed empty directory: {root_path}")
                except Exception as e:
                    logger.error(f"❌ Failed to remove empty directories in {dir_path}: {e}")
                    self.cleanup_stats['errors'] += 1
        
        self.cleanup_stats['dirs_removed'] += len(removed_dirs)
        
        logger.info(f"🎯 Empty directory cleanup complete: {len(removed_dirs)} directories removed")
        
        return {
            'dirs_removed': len(removed_dirs),
            'removed_items': removed_dirs
        }
    
    def cleanup_by_file_list(self, file_list: List[str]) -> Dict[str, int]:
        """
        Remove specific files by list
        
        Args:
            file_list: List of specific file paths to remove
            
        Returns:
            Dictionary with cleanup statistics
        """
        logger.info(f"📝 Cleaning up specific files: {len(file_list)} items")
        
        removed_files = []
        total_size = 0
        
        for file_name in file_list:
            file_path = self.base_dir / file_name
            if file_path.exists():
                try:
                    if file_path.is_file():
                        size = file_path.stat().st_size
                        file_path.unlink()
                        total_size += size
                    elif file_path.is_dir():
                        shutil.rmtree(file_path)
                    
                    removed_files.append(file_name)
                    logger.info(f"✅ Removed: {file_name}")
                except Exception as e:
                    logger.error(f"❌ Failed to remove {file_name}: {e}")
                    self.cleanup_stats['errors'] += 1
            else:
                logger.info(f"⚪ Not found: {file_name}")
        
        self.cleanup_stats['files_removed'] += len(removed_files)
        self.cleanup_stats['total_size_freed'] += total_size
        
        return {
            'files_removed': len(removed_files),
            'total_size_freed': total_size,
            'removed_items': removed_files
        }
    
    def run_full_cleanup(self, config_section: str = "default") -> Dict[str, int]:
        """
        Run comprehensive cleanup using configuration
        
        Args:
            config_section: Configuration section to use for cleanup settings
            
        Returns:
            Dictionary with total cleanup statistics
        """
        logger.info(f"🚀 Starting full cleanup with config section: {config_section}")
        
        # Reset stats
        self.cleanup_stats = {'files_removed': 0, 'dirs_removed': 0, 'errors': 0, 'total_size_freed': 0}
        
        # Get cleanup configuration
        cleanup_config = self.config.get_section(f"cleanup.{config_section}")
        if not cleanup_config:
            cleanup_config = {
                'temp_files': True,
                'old_backups': True,
                'empty_dirs': True,
                'backup_retention_days': 30
            }
        
        # Run cleanup operations
        if cleanup_config.get('temp_files', True):
            self.cleanup_temporary_files()
        
        if cleanup_config.get('old_backups', True):
            retention_days = cleanup_config.get('backup_retention_days', 30)
            self.cleanup_backup_files(older_than_days=retention_days)
        
        if cleanup_config.get('empty_dirs', True):
            self.cleanup_empty_directories()
        
        # Final summary
        logger.info(f"🎯 Full cleanup complete:")
        logger.info(f"   Files removed: {self.cleanup_stats['files_removed']}")
        logger.info(f"   Directories removed: {self.cleanup_stats['dirs_removed']}")
        logger.info(f"   Total size freed: {self.cleanup_stats['total_size_freed'] / 1024:.1f} KB")
        logger.info(f"   Errors: {self.cleanup_stats['errors']}")
        
        return self.cleanup_stats.copy()


# Convenience functions for backward compatibility with existing cleanup scripts
def cleanup_temp_files_legacy():
    """Legacy cleanup function for compatibility with old cleanup scripts"""
    temp_files = [
        'make_repo_private.py',
        'run_git_setup.py', 
        'GITHUB_SETUP_COMMANDS.md',
        'cleanup_temp_files.py',
        'temp_cleanup.py',
        'execute_cleanup.py',
        'final_cleanup.py',
        'simple_cleanup.py',
        'run_cleanup.py'
    ]
    
    manager = CleanupManager()
    return manager.cleanup_by_file_list(temp_files)


def main():
    """CLI interface for cleanup manager"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Cleanup Manager - Centralized file cleanup")
    parser.add_argument('--temp-files', action='store_true', help='Clean up temporary files')
    parser.add_argument('--old-backups', action='store_true', help='Clean up old backup files')
    parser.add_argument('--empty-dirs', action='store_true', help='Remove empty directories')
    parser.add_argument('--full', action='store_true', help='Run full cleanup')
    parser.add_argument('--legacy', action='store_true', help='Run legacy cleanup (specific file list)')
    parser.add_argument('--config-section', default='default', help='Configuration section to use')
    parser.add_argument('--backup-days', type=int, default=30, help='Days to keep backup files')
    
    args = parser.parse_args()
    
    manager = CleanupManager()
    
    if args.legacy:
        stats = cleanup_temp_files_legacy()
    elif args.full:
        stats = manager.run_full_cleanup(args.config_section)
    else:
        stats = {'files_removed': 0, 'dirs_removed': 0, 'total_size_freed': 0}
        
        if args.temp_files:
            temp_stats = manager.cleanup_temporary_files()
            stats['files_removed'] += temp_stats['files_removed']
            stats['total_size_freed'] += temp_stats['total_size_freed']
        
        if args.old_backups:
            backup_stats = manager.cleanup_backup_files(older_than_days=args.backup_days)
            stats['files_removed'] += backup_stats['files_removed']
            stats['total_size_freed'] += backup_stats['total_size_freed']
        
        if args.empty_dirs:
            dir_stats = manager.cleanup_empty_directories()
            stats['dirs_removed'] += dir_stats['dirs_removed']
    
    print(f"\n🎯 Cleanup Summary:")
    print(f"   Files removed: {stats.get('files_removed', 0)}")
    print(f"   Directories removed: {stats.get('dirs_removed', 0)}")
    print(f"   Total size freed: {stats.get('total_size_freed', 0) / 1024:.1f} KB")


if __name__ == "__main__":
    main()