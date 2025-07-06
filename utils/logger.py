#!/usr/bin/env python3
"""
Comprehensive logging utility for the web scraping pipeline.
Provides dual output (terminal + file) and organized log management.
"""
import os
import sys
import logging
import json
import csv
from datetime import datetime
from pathlib import Path
import shutil
from contextlib import contextmanager
import subprocess

class DualLogger:
    """Logger that outputs to both terminal and file"""
    
    def __init__(self, name, log_file, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # File handler
        fh = logging.FileHandler(log_file)
        fh.setLevel(level)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        
        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        # Simpler format for console
        console_formatter = logging.Formatter('[%(levelname)s] %(message)s')
        ch.setFormatter(console_formatter)
        self.logger.addHandler(ch)
        
        self.file_path = log_file
        
    def info(self, msg):
        self.logger.info(msg)
        
    def error(self, msg):
        self.logger.error(msg)
        
    def warning(self, msg):
        self.logger.warning(msg)
        
    def debug(self, msg):
        self.logger.debug(msg)
        
    def success(self, msg):
        """Custom success level - shows as INFO but with clear marking"""
        self.logger.info(f"✅ {msg}")

class PipelineLogger:
    """Main logger class for the entire pipeline"""
    
    def __init__(self):
        self.logs_dir = Path("logs")
        self.runs_dir = self.logs_dir / "runs"
        self.history_file = self.logs_dir / "history.csv"
        
        # Ensure directories exist
        self.logs_dir.mkdir(exist_ok=True)
        self.runs_dir.mkdir(exist_ok=True)
        
        # Initialize history file if it doesn't exist
        if not self.history_file.exists():
            with open(self.history_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'run_id', 'start_time', 'end_time', 'duration_seconds',
                    'status', 'rows_processed', 'youtube_downloads', 
                    'drive_downloads', 'errors', 'log_path'
                ])
        
        self.current_run = None
        self.loggers = {}
        self.run_stats = {
            'start_time': None,
            'end_time': None,
            'rows_processed': 0,
            'youtube_downloads': 0,
            'drive_downloads': 0,
            'errors': 0,
            'status': 'running'
        }
        
    def start_run(self):
        """Start a new logging run"""
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        self.current_run = self.runs_dir / timestamp
        self.current_run.mkdir(exist_ok=True)
        
        # Update latest symlink with robust error handling (DRY fix)
        latest_link = self.runs_dir / "latest"
        try:
            if latest_link.exists() or latest_link.is_symlink():
                latest_link.unlink()
        except (OSError, FileNotFoundError):
            pass  # Ignore if already removed or doesn't exist
        
        try:
            latest_link.symlink_to(timestamp)
        except FileExistsError:
            # If symlink still exists, force remove and retry once
            try:
                latest_link.unlink()
                latest_link.symlink_to(timestamp)
            except (OSError, FileExistsError):
                pass  # If it still fails, continue without symlink
        
        # Initialize run stats
        self.run_stats['start_time'] = datetime.now()
        self.run_stats['run_id'] = timestamp
        
        # Create main logger
        main_log = self.current_run / "main.log"
        self.loggers['main'] = DualLogger('MAIN', main_log)
        
        # Create component loggers
        self.loggers['scraper'] = DualLogger('SCRAPER', self.current_run / "scraper.log")
        self.loggers['youtube'] = DualLogger('YOUTUBE', self.current_run / "youtube.log")
        self.loggers['drive'] = DualLogger('DRIVE', self.current_run / "drive.log")
        self.loggers['errors'] = DualLogger('ERRORS', self.current_run / "errors.log", logging.ERROR)
        
        self.loggers['main'].info(f"Starting pipeline run: {timestamp}")
        self.loggers['main'].info(f"Logs directory: {self.current_run}")
        
        return timestamp
        
    def get_logger(self, component='main'):
        """Get a specific component logger"""
        return self.loggers.get(component, self.loggers['main'])
        
    def log_subprocess(self, command, description=None, component='main'):
        """Run a subprocess and log its output"""
        logger = self.get_logger(component)
        
        if description:
            logger.info(f"{'=' * 80}")
            logger.info(description)
            logger.info(f"{'=' * 80}")
        
        logger.info(f"Running command: {' '.join(command) if isinstance(command, list) else command}")
        
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Log output in real-time
        for line in process.stdout:
            line = line.rstrip()
            if line:
                logger.info(f"  > {line}")
        
        process.wait()
        
        if process.returncode != 0:
            logger.error(f"Command failed with exit code {process.returncode}")
            self.log_error(f"Subprocess failed: {command}")
            
        return process.returncode
        
    def log_error(self, msg):
        """Log an error to both the component logger and error log"""
        self.loggers['errors'].error(msg)
        self.run_stats['errors'] += 1
        
    def update_stats(self, **kwargs):
        """Update run statistics"""
        self.run_stats.update(kwargs)
        
    def end_run(self, status='completed'):
        """End the current logging run and save statistics"""
        if not self.current_run:
            return
            
        self.run_stats['end_time'] = datetime.now()
        self.run_stats['status'] = status
        
        duration = (self.run_stats['end_time'] - self.run_stats['start_time']).total_seconds()
        
        # Save summary JSON
        summary_file = self.current_run / "summary.json"
        with open(summary_file, 'w') as f:
            json.dump({
                **self.run_stats,
                'start_time': self.run_stats['start_time'].isoformat(),
                'end_time': self.run_stats['end_time'].isoformat(),
                'duration_seconds': duration,
                'log_path': str(self.current_run)
            }, f, indent=2)
        
        # Update history CSV
        with open(self.history_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                self.run_stats['run_id'],
                self.run_stats['start_time'].isoformat(),
                self.run_stats['end_time'].isoformat(),
                duration,
                status,
                self.run_stats['rows_processed'],
                self.run_stats['youtube_downloads'],
                self.run_stats['drive_downloads'],
                self.run_stats['errors'],
                str(self.current_run)
            ])
        
        # Log final summary
        self.loggers['main'].info(f"{'=' * 80}")
        self.loggers['main'].info("Pipeline Run Summary:")
        self.loggers['main'].info(f"  Duration: {duration:.2f} seconds")
        self.loggers['main'].info(f"  Status: {status}")
        self.loggers['main'].info(f"  Rows processed: {self.run_stats['rows_processed']}")
        self.loggers['main'].info(f"  YouTube downloads: {self.run_stats['youtube_downloads']}")
        self.loggers['main'].info(f"  Drive downloads: {self.run_stats['drive_downloads']}")
        self.loggers['main'].info(f"  Errors: {self.run_stats['errors']}")
        self.loggers['main'].info(f"  Logs saved to: {self.current_run}")
        self.loggers['main'].info(f"{'=' * 80}")

# Global logger instance
_pipeline_logger = None

def get_pipeline_logger():
    """Get the global pipeline logger instance"""
    global _pipeline_logger
    if _pipeline_logger is None:
        _pipeline_logger = PipelineLogger()
    return _pipeline_logger

@contextmanager
def pipeline_run():
    """Context manager for a pipeline run"""
    logger = get_pipeline_logger()
    try:
        run_id = logger.start_run()
        yield logger
        logger.end_run('completed')
    except Exception as e:
        logger.log_error(f"Pipeline failed: {str(e)}")
        logger.end_run('failed')
        raise

def setup_component_logging(component_name):
    """Setup logging for a specific component when run standalone"""
    logger = get_pipeline_logger()
    if not logger.current_run:
        logger.start_run()
    return logger.get_logger(component_name)