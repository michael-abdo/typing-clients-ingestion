#!/usr/bin/env python3
"""
Orphaned File Recovery Tracker
Comprehensive logging and tracking system for UUID file recovery process
"""

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd

class OrphanedFileRecoveryTracker:
    """Tracks the orphaned file recovery process with detailed logging"""
    
    def __init__(self):
        self.session_id = str(uuid.uuid4())[:8]
        self.start_time = datetime.now()
        
        # Create recovery session directory
        self.session_dir = Path(f"recovery_session_{self.session_id}_{self.start_time.strftime('%Y%m%d_%H%M%S')}")
        self.session_dir.mkdir(exist_ok=True)
        
        # Initialize tracking data
        self.session_data = {
            'session_id': self.session_id,
            'start_time': self.start_time.isoformat(),
            'phases': {
                'sam_pipeline': {'status': 'pending', 'mappings': [], 'errors': []},
                'drive_ids': {'status': 'pending', 'mappings': [], 'errors': []},
                'size_correlation': {'status': 'pending', 'mappings': [], 'errors': []},
                'pattern_analysis': {'status': 'pending', 'mappings': [], 'errors': []}
            },
            'statistics': {
                'total_orphaned_files': 0,
                'total_mapped_files': 0,
                'confidence_distribution': {},
                'error_count': 0
            },
            'log_entries': []
        }
        
        # Create log file
        self.log_file = self.session_dir / "recovery_log.txt"
        
        self.log("INFO", "Recovery session started", {
            'session_id': self.session_id,
            'timestamp': self.start_time.isoformat()
        })
    
    def log(self, level: str, message: str, data: Optional[Dict] = None):
        """Log an entry with timestamp and optional data"""
        timestamp = datetime.now().isoformat()
        
        log_entry = {
            'timestamp': timestamp,
            'level': level,
            'message': message,
            'data': data or {}
        }
        
        self.session_data['log_entries'].append(log_entry)
        
        # Write to log file
        with open(self.log_file, 'a') as f:
            log_line = f"[{timestamp}] {level}: {message}"
            if data:
                log_line += f" | Data: {json.dumps(data, default=str)}"
            f.write(log_line + "\n")
        
        # Print to console
        print(f"[{level}] {message}")
        if data and level in ['ERROR', 'WARNING']:
            print(f"    Data: {data}")
    
    def start_phase(self, phase_name: str):
        """Mark the start of a recovery phase"""
        if phase_name in self.session_data['phases']:
            self.session_data['phases'][phase_name]['status'] = 'in_progress'
            self.session_data['phases'][phase_name]['start_time'] = datetime.now().isoformat()
            
            self.log("INFO", f"Starting phase: {phase_name}")
    
    def complete_phase(self, phase_name: str, mappings_count: int = 0):
        """Mark the completion of a recovery phase"""
        if phase_name in self.session_data['phases']:
            self.session_data['phases'][phase_name]['status'] = 'completed'
            self.session_data['phases'][phase_name]['end_time'] = datetime.now().isoformat()
            self.session_data['phases'][phase_name]['mappings_count'] = mappings_count
            
            self.log("INFO", f"Completed phase: {phase_name}", {
                'mappings_added': mappings_count
            })
    
    def add_mapping(self, phase_name: str, mapping_data: Dict):
        """Record a successful file mapping"""
        if phase_name in self.session_data['phases']:
            mapping_record = {
                'timestamp': datetime.now().isoformat(),
                'uuid': mapping_data.get('uuid'),
                'client_id': mapping_data.get('client_id'),
                'filename': mapping_data.get('filename'),
                'confidence': mapping_data.get('confidence', 'high'),
                'method': mapping_data.get('method'),
                'validation_passed': mapping_data.get('validation_passed', True)
            }
            
            self.session_data['phases'][phase_name]['mappings'].append(mapping_record)
            self.session_data['statistics']['total_mapped_files'] += 1
            
            self.log("SUCCESS", f"Mapped file in {phase_name}", mapping_record)
    
    def add_error(self, phase_name: str, error_data: Dict):
        """Record an error during recovery"""
        error_record = {
            'timestamp': datetime.now().isoformat(),
            'error_type': error_data.get('error_type'),
            'error_message': error_data.get('error_message'),
            'context': error_data.get('context', {})
        }
        
        if phase_name in self.session_data['phases']:
            self.session_data['phases'][phase_name]['errors'].append(error_record)
        
        self.session_data['statistics']['error_count'] += 1
        
        self.log("ERROR", f"Error in {phase_name}", error_record)
    
    def update_statistics(self, stats_update: Dict):
        """Update session statistics"""
        self.session_data['statistics'].update(stats_update)
        self.log("INFO", "Statistics updated", stats_update)
    
    def save_session(self):
        """Save the current session data to file"""
        session_file = self.session_dir / "session_data.json"
        
        # Update end time
        self.session_data['end_time'] = datetime.now().isoformat()
        self.session_data['duration_minutes'] = (
            datetime.now() - self.start_time
        ).total_seconds() / 60
        
        with open(session_file, 'w') as f:
            json.dump(self.session_data, f, indent=2, default=str)
        
        self.log("INFO", "Session data saved", {
            'file': str(session_file),
            'total_mappings': self.session_data['statistics']['total_mapped_files'],
            'total_errors': self.session_data['statistics']['error_count']
        })
    
    def generate_report(self) -> str:
        """Generate a comprehensive recovery report"""
        report_lines = [
            "=" * 80,
            "ORPHANED FILE RECOVERY REPORT",
            "=" * 80,
            f"Session ID: {self.session_id}",
            f"Start Time: {self.session_data['start_time']}",
            f"Duration: {self.session_data.get('duration_minutes', 0):.1f} minutes",
            "",
            "PHASE SUMMARY:",
            "-" * 40
        ]
        
        for phase_name, phase_data in self.session_data['phases'].items():
            status = phase_data['status']
            mappings_count = len(phase_data['mappings'])
            errors_count = len(phase_data['errors'])
            
            report_lines.extend([
                f"{phase_name.upper()}:",
                f"  Status: {status}",
                f"  Mappings: {mappings_count}",
                f"  Errors: {errors_count}"
            ])
        
        report_lines.extend([
            "",
            "OVERALL STATISTICS:",
            "-" * 40,
            f"Total files mapped: {self.session_data['statistics']['total_mapped_files']}",
            f"Total errors: {self.session_data['statistics']['error_count']}",
            "",
            "=" * 80
        ])
        
        report_text = "\n".join(report_lines)
        
        # Save report to file
        report_file = self.session_dir / "recovery_report.txt"
        with open(report_file, 'w') as f:
            f.write(report_text)
        
        return report_text
    
    def get_session_dir(self) -> Path:
        """Get the session directory path"""
        return self.session_dir

# Global tracker instance
_tracker = None

def get_tracker() -> OrphanedFileRecoveryTracker:
    """Get or create the global tracker instance"""
    global _tracker
    if _tracker is None:
        _tracker = OrphanedFileRecoveryTracker()
    return _tracker

def initialize_recovery_session():
    """Initialize a new recovery session"""
    tracker = get_tracker()
    tracker.log("INFO", "Recovery tracking system initialized")
    return tracker

if __name__ == "__main__":
    # Test the tracking system
    tracker = initialize_recovery_session()
    
    # Simulate some operations
    tracker.start_phase('sam_pipeline')
    tracker.add_mapping('sam_pipeline', {
        'uuid': 'test-uuid-123',
        'client_id': 502,
        'filename': 'test_file.mp4',
        'confidence': 'high',
        'method': 'pipeline_data'
    })
    tracker.complete_phase('sam_pipeline', 1)
    
    # Generate and display report
    print("\n" + tracker.generate_report())
    
    # Save session
    tracker.save_session()
    print(f"\nSession saved in: {tracker.get_session_dir()}")