#!/usr/bin/env python3
"""
Cleanup Manager - Centralized file cleanup functionality
Consolidates all cleanup scripts into a single, configurable module
"""

import os
import shutil
import glob
from pathlib import Path
from typing import List, Dict, Optional, Any
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
    
    # DRY: Consolidated CSV cleanup methods (from scripts/cleanup_csv_fields.py)
    def cleanup_csv_fields(self, csv_file: str, max_field_size: int = 100000) -> Dict[str, Any]:
        """
        Clean up oversized fields in CSV files
        
        Args:
            csv_file: Path to CSV file to clean
            max_field_size: Maximum allowed field size in bytes
            
        Returns:
            Dictionary with cleanup statistics
        """
        import csv
        import sys
        
        logger.info(f"📊 Cleaning oversized CSV fields in: {csv_file}")
        
        csv_path = self.base_dir / csv_file
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return {'success': False, 'error': 'File not found'}
        
        # Set maximum CSV field size to handle existing large fields
        csv.field_size_limit(sys.maxsize)
        
        output_file = csv_path.parent / f"{csv_path.stem}_cleaned{csv_path.suffix}"
        rows_processed = 0
        fields_truncated = 0
        
        try:
            with open(csv_path, 'r', newline='', encoding='utf-8') as infile:
                with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                    reader = csv.DictReader(infile)
                    fieldnames = reader.fieldnames
                    
                    if not fieldnames:
                        logger.error("No fieldnames found in CSV")
                        return {'success': False, 'error': 'No fieldnames'}
                    
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for row_num, row in enumerate(reader, start=2):
                        rows_processed += 1
                        
                        # Check and truncate oversized fields
                        for field, value in row.items():
                            if value and len(str(value)) > max_field_size:
                                original_size = len(str(value))
                                row[field] = str(value)[:max_field_size] + "... [TRUNCATED]"
                                fields_truncated += 1
                                logger.warning(f"Row {row_num}: Truncated field '{field}' "
                                             f"from {original_size:,} to {max_field_size:,} bytes")
                        
                        writer.writerow(row)
            
            logger.info(f"CSV cleanup complete: {rows_processed} rows, {fields_truncated} fields truncated")
            
            return {
                'success': True,
                'rows_processed': rows_processed,
                'fields_truncated': fields_truncated,
                'output_file': str(output_file)
            }
            
        except Exception as e:
            logger.error(f"CSV cleanup failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def cleanup_malformed_urls(self, csv_file: str) -> Dict[str, Any]:
        """
        Clean malformed URLs in CSV files (from utilities/maintenance/cleanup_malformed_urls.py)
        
        Args:
            csv_file: Path to CSV file with malformed URLs
            
        Returns:
            Dictionary with cleanup statistics
        """
        logger.info(f"🔗 Cleaning malformed URLs in: {csv_file}")
        
        # This would contain logic to fix common URL issues like:
        # - Unescaped unicode characters
        # - Control characters in URLs
        # - Broken encodings
        # For now, returning placeholder
        return {
            'success': True,
            'urls_cleaned': 0,
            'message': 'URL cleanup functionality to be implemented'
        }
    
    def cleanup_duplicate_transcripts(self, transcript_dir: str = "downloads") -> Dict[str, Any]:
        """
        Remove duplicate transcript files (from utilities/maintenance/remove_duplicate_transcripts.py)
        
        Args:
            transcript_dir: Directory containing transcript files
            
        Returns:
            Dictionary with cleanup statistics
        """
        logger.info(f"📝 Cleaning duplicate transcripts in: {transcript_dir}")
        
        transcript_path = self.base_dir / transcript_dir
        if not transcript_path.exists():
            return {'success': False, 'error': 'Directory not found'}
        
        # Pattern for finding duplicate transcripts
        # e.g., video_transcript.txt and video_transcript(1).txt
        duplicates_removed = 0
        
        for file_path in transcript_path.rglob("*_transcript*.txt"):
            # Check for numbered duplicates
            if "(1)" in file_path.name or "(2)" in file_path.name:
                try:
                    file_path.unlink()
                    duplicates_removed += 1
                    logger.info(f"✅ Removed duplicate transcript: {file_path.name}")
                except Exception as e:
                    logger.error(f"Failed to remove {file_path}: {e}")
        
        return {
            'success': True,
            'duplicates_removed': duplicates_removed
        }
    
    def emergency_cleanup(self, include_system: bool = False) -> Dict[str, Any]:
        """
        Emergency cleanup to free disk space (from minimal/emergency_cleanup.sh)
        
        Args:
            include_system: Include system-level cleanup (requires permissions)
            
        Returns:
            Dictionary with cleanup statistics
        """
        logger.info(f"🚨 Running emergency cleanup to free disk space...")
        
        import subprocess
        
        commands_run = []
        errors = []
        
        # User-level cleanup (no sudo required)
        user_cleanup_cmds = [
            ("rm -rf ~/.cache/*", "Clear user cache"),
            ("rm -rf ~/.npm", "Clear npm cache"),
            ("rm -rf ~/.local/share/pnpm", "Clear pnpm cache"),
            ("rm -rf ~/.local/share/heroku", "Clear heroku cache"),
            ("rm -rf /tmp/*", "Clear temp files")
        ]
        
        for cmd, description in user_cleanup_cmds:
            try:
                subprocess.run(cmd, shell=True, check=True)
                commands_run.append(description)
                logger.info(f"✅ {description}")
            except subprocess.CalledProcessError as e:
                errors.append(f"{description}: {e}")
                logger.warning(f"⚠️ {description} failed: {e}")
        
        # System-level cleanup (requires sudo)
        if include_system:
            system_cleanup_cmds = [
                ("sudo rm -rf /var/log/*.log", "Clear system logs"),
                ("sudo journalctl --vacuum-time=1d", "Vacuum journal logs"),
                ("sudo apt clean", "Clear apt cache")
            ]
            
            for cmd, description in system_cleanup_cmds:
                try:
                    subprocess.run(cmd, shell=True, check=True)
                    commands_run.append(description)
                    logger.info(f"✅ {description}")
                except subprocess.CalledProcessError as e:
                    errors.append(f"{description}: {e}")
                    logger.warning(f"⚠️ {description} failed: {e}")
        
        # Check disk usage after cleanup
        try:
            result = subprocess.run("df -h /", shell=True, capture_output=True, text=True)
            disk_usage = result.stdout
        except:
            disk_usage = "Unable to check disk usage"
        
        return {
            'success': len(errors) == 0,
            'commands_run': commands_run,
            'errors': errors,
            'disk_usage': disk_usage
        }
    
    def cleanup_redundant_backups(self, remove_old_logs: bool = False) -> Dict[str, Any]:
        """
        Remove redundant backup files and archive directories identified during DRY analysis.
        DRY: Absorbs functionality instead of creating new cleanup scripts
        
        Args:
            remove_old_logs: Whether to remove old log directories from May 2025
            
        Returns:
            Dictionary with cleanup statistics
        """
        logger.info(f"🗂️ Cleaning up redundant backup files and archive directories...")
        
        removed_items = []
        total_size_freed = 0
        
        # High priority: Current .backup files (redundant copies of working files)
        backup_files = [
            'tests/test_youtube_validation.py.backup',
            'tests/test_validation.py.backup', 
            'tests/test_file_lock.py.backup',
            'tests/test_url_cleaning.py.backup',
            'minimal/simple_workflow_db.py.backup',
            'run_all_tests.py.phase2b.backup',
            'run_complete_workflow.py.backup'
        ]
        
        for backup_file in backup_files:
            backup_path = self.base_dir / backup_file
            if backup_path.exists():
                try:
                    size = backup_path.stat().st_size
                    backup_path.unlink()
                    removed_items.append(backup_file)
                    total_size_freed += size
                    logger.info(f"✅ Removed redundant backup: {backup_file}")
                except Exception as e:
                    logger.error(f"❌ Failed to remove {backup_file}: {e}")
        
        # High priority: Temporary .tmp files in outputs/
        outputs_dir = self.base_dir / "outputs"
        if outputs_dir.exists():
            for tmp_file in outputs_dir.glob("*.tmp"):
                try:
                    size = tmp_file.stat().st_size
                    tmp_file.unlink()
                    removed_items.append(str(tmp_file.relative_to(self.base_dir)))
                    total_size_freed += size
                    logger.info(f"✅ Removed temp file: {tmp_file.name}")
                except Exception as e:
                    logger.error(f"❌ Failed to remove {tmp_file}: {e}")
        
        # Optional: Old log directories from May 2025 (if requested)
        if remove_old_logs:
            logs_runs_dir = self.base_dir / "logs" / "runs"
            if logs_runs_dir.exists():
                for log_dir in logs_runs_dir.glob("2025-05-*"):
                    if log_dir.is_dir():
                        try:
                            # Calculate directory size before removal
                            dir_size = sum(f.stat().st_size for f in log_dir.rglob('*') if f.is_file())
                            shutil.rmtree(log_dir)
                            removed_items.append(str(log_dir.relative_to(self.base_dir)))
                            total_size_freed += dir_size
                            logger.info(f"✅ Removed old log directory: {log_dir.name}")
                        except Exception as e:
                            logger.error(f"❌ Failed to remove {log_dir}: {e}")
        
        self.cleanup_stats['files_removed'] += len(removed_items)
        self.cleanup_stats['total_size_freed'] += total_size_freed
        
        logger.info(f"🎯 Redundant backup cleanup complete: {len(removed_items)} items removed, "
                   f"{total_size_freed / 1024:.1f} KB freed")
        
        return {
            'success': True,
            'items_removed': len(removed_items),
            'total_size_freed': total_size_freed,
            'removed_items': removed_items
        }


# DRY: Consolidated file lists from all scattered cleanup scripts
TEMP_FILES_COMPREHENSIVE = [
    # From execute_cleanup.py
    'make_repo_private.py',
    'run_git_setup.py', 
    'GITHUB_SETUP_COMMANDS.md',
    'cleanup_temp_files.py',
    # From final_cleanup.py additions
    'temp_cleanup.py',
    'execute_cleanup.py',
    # From simple_cleanup.py additions  
    'final_cleanup.py',
    'simple_cleanup.py',
    'run_cleanup.py',
    # From final_remove.py (same list but with size filtering)
    # All covered above
]

# Convenience functions for backward compatibility with existing cleanup scripts
def cleanup_temp_files_legacy():
    """
    Legacy cleanup function for compatibility with old cleanup scripts.
    DRY: Absorbs functionality from execute_cleanup.py, final_cleanup.py, 
    simple_cleanup.py, temp_cleanup.py, run_cleanup.py, final_remove.py
    """
    manager = CleanupManager()
    return manager.cleanup_by_file_list(TEMP_FILES_COMPREHENSIVE)

def cleanup_execute_legacy():
    """Replaces execute_cleanup.py functionality"""
    manager = CleanupManager()
    files = ['make_repo_private.py', 'run_git_setup.py', 'GITHUB_SETUP_COMMANDS.md', 'cleanup_temp_files.py']
    return manager.cleanup_by_file_list(files)

def cleanup_final_legacy():
    """Replaces final_cleanup.py functionality"""  
    manager = CleanupManager()
    files = TEMP_FILES_COMPREHENSIVE[:6]  # Original final_cleanup.py list
    result = manager.cleanup_by_file_list(files)
    # Final cleanup also removes run_cleanup.py specifically
    manager.cleanup_by_file_list(['run_cleanup.py'])
    return result

def cleanup_simple_legacy():
    """Replaces simple_cleanup.py functionality"""
    manager = CleanupManager()
    files = TEMP_FILES_COMPREHENSIVE[:7]  # Original simple_cleanup.py list  
    result = manager.cleanup_by_file_list(files)
    # Simple cleanup removes run_cleanup.py and self-removes
    manager.cleanup_by_file_list(['run_cleanup.py'])
    return result

def cleanup_temp_legacy():
    """Replaces temp_cleanup.py functionality"""
    manager = CleanupManager()
    files = ['make_repo_private.py', 'run_git_setup.py', 'GITHUB_SETUP_COMMANDS.md', 'cleanup_temp_files.py']
    return manager.cleanup_by_file_list(files)

def cleanup_final_remove_legacy():
    """Replaces final_remove.py functionality (size-based filtering)"""
    manager = CleanupManager()
    
    # final_remove.py only removes files ≤ 10 bytes (empty files)
    removed_files = []
    for file_name in TEMP_FILES_COMPREHENSIVE:
        file_path = manager.base_dir / file_name
        if file_path.exists() and file_path.is_file():
            if file_path.stat().st_size <= 10:  # Only remove very small/empty files
                try:
                    file_path.unlink()
                    removed_files.append(file_name)
                    logger.info(f"✅ Removed small file: {file_name}")
                except Exception as e:
                    logger.error(f"❌ Failed to remove {file_name}: {e}")
    
    return {'files_removed': len(removed_files), 'removed_items': removed_files}


def main():
    """CLI interface for cleanup manager (DRY: Enhanced with consolidated options)"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Cleanup Manager - Centralized file cleanup")
    
    # Basic cleanup options
    parser.add_argument('--temp-files', action='store_true', help='Clean up temporary files')
    parser.add_argument('--old-backups', action='store_true', help='Clean up old backup files')
    parser.add_argument('--empty-dirs', action='store_true', help='Remove empty directories')
    parser.add_argument('--full', action='store_true', help='Run full cleanup')
    parser.add_argument('--legacy', action='store_true', help='Run legacy cleanup (specific file list)')
    
    # DRY: Enhanced cleanup options (consolidated)
    parser.add_argument('--csv-fields', metavar='FILE', help='Clean oversized CSV fields in specified file')
    parser.add_argument('--malformed-urls', metavar='FILE', help='Fix malformed URLs in CSV file')
    parser.add_argument('--duplicate-transcripts', metavar='DIR', nargs='?', const='downloads',
                       help='Remove duplicate transcript files (default: downloads)')
    parser.add_argument('--emergency', action='store_true', 
                       help='Emergency cleanup to free disk space')
    parser.add_argument('--emergency-system', action='store_true',
                       help='Emergency cleanup including system files (requires sudo)')
    
    # Configuration options
    parser.add_argument('--config-section', default='default', help='Configuration section to use')
    parser.add_argument('--backup-days', type=int, default=30, help='Days to keep backup files')
    parser.add_argument('--max-field-size', type=int, default=100000, 
                       help='Maximum CSV field size in bytes')
    
    args = parser.parse_args()
    
    manager = CleanupManager()
    
    # Handle specific enhanced operations first
    if args.emergency_system:
        emergency_stats = manager.emergency_cleanup(include_system=True)
        print(f"🚨 Emergency cleanup (with system) completed:")
        for cmd in emergency_stats['commands_run']:
            print(f"   ✅ {cmd}")
        if emergency_stats['errors']:
            for error in emergency_stats['errors']:
                print(f"   ❌ {error}")
        print(f"\n📊 Disk usage:\n{emergency_stats['disk_usage']}")
        return
    elif args.emergency:
        emergency_stats = manager.emergency_cleanup(include_system=False)
        print(f"🚨 Emergency cleanup completed:")
        for cmd in emergency_stats['commands_run']:
            print(f"   ✅ {cmd}")
        if emergency_stats['errors']:
            for error in emergency_stats['errors']:
                print(f"   ❌ {error}")
        print(f"\n📊 Disk usage:\n{emergency_stats['disk_usage']}")
        return
    elif args.csv_fields:
        csv_stats = manager.cleanup_csv_fields(args.csv_fields, args.max_field_size)
        if csv_stats['success']:
            print(f"📊 CSV cleanup completed:")
            print(f"   Rows processed: {csv_stats['rows_processed']}")
            print(f"   Fields truncated: {csv_stats['fields_truncated']}")
            print(f"   Output file: {csv_stats['output_file']}")
        else:
            print(f"❌ CSV cleanup failed: {csv_stats['error']}")
        return
    elif args.duplicate_transcripts:
        transcript_stats = manager.cleanup_duplicate_transcripts(args.duplicate_transcripts)
        if transcript_stats['success']:
            print(f"📝 Transcript cleanup completed:")
            print(f"   Duplicates removed: {transcript_stats['duplicates_removed']}")
        else:
            print(f"❌ Transcript cleanup failed: {transcript_stats['error']}")
        return
    
    # Handle basic operations
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