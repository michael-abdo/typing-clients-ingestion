#!/usr/bin/env python3
"""Create a snapshot of the current codebase structure and metrics."""

import os
import json
import time
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

class CodebaseSnapshot:
    """Create detailed snapshot of codebase before refactoring."""
    
    def __init__(self):
        self.snapshot_time = datetime.now().isoformat()
        self.project_root = Path(__file__).parent
        
    def calculate_file_hash(self, filepath: Path) -> str:
        """Calculate SHA256 hash of a file."""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def analyze_python_file(self, filepath: Path) -> Dict[str, Any]:
        """Analyze a Python file for metrics."""
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            lines = content.splitlines()
        
        metrics = {
            'path': str(filepath.relative_to(self.project_root)),
            'size_bytes': filepath.stat().st_size,
            'line_count': len(lines),
            'hash': self.calculate_file_hash(filepath),
            'imports': [],
            'has_path_setup': False,
            'has_makedirs': False,
            'has_csv_operations': False,
            'has_try_except': False
        }
        
        # Analyze patterns
        for line in lines:
            line_stripped = line.strip()
            
            # Check for imports
            if line_stripped.startswith(('import ', 'from ')):
                metrics['imports'].append(line_stripped)
            
            # Check for patterns
            if 'sys.path.insert' in line:
                metrics['has_path_setup'] = True
            if 'os.makedirs' in line:
                metrics['has_makedirs'] = True
            if 'csv.' in line or 'pd.read_csv' in line or 'to_csv' in line:
                metrics['has_csv_operations'] = True
            if line_stripped.startswith('try:'):
                metrics['has_try_except'] = True
        
        return metrics
    
    def measure_import_performance(self, module_path: str) -> float:
        """Measure time to import a module."""
        start_time = time.time()
        try:
            # Clear any cached imports
            import sys
            if module_path in sys.modules:
                del sys.modules[module_path]
            
            # Time the import
            exec(f"import {module_path}")
            return time.time() - start_time
        except:
            return -1.0
    
    def create_snapshot(self) -> Dict[str, Any]:
        """Create complete snapshot of the codebase."""
        print("Creating codebase snapshot...")
        
        snapshot = {
            'timestamp': self.snapshot_time,
            'python_files': [],
            'statistics': {
                'total_files': 0,
                'total_lines': 0,
                'total_size_bytes': 0,
                'files_with_path_setup': 0,
                'files_with_makedirs': 0,
                'files_with_csv_ops': 0,
                'files_with_try_except': 0
            },
            'import_performance': {},
            'directory_structure': {}
        }
        
        # Find all Python files
        python_files = []
        for root, dirs, files in os.walk(self.project_root):
            # Skip certain directories
            dirs[:] = [d for d in dirs if d not in ['__pycache__', '.git', 'venv', '.venv']]
            
            for file in files:
                if file.endswith('.py'):
                    filepath = Path(root) / file
                    python_files.append(filepath)
        
        # Analyze each file
        for filepath in python_files:
            try:
                metrics = self.analyze_python_file(filepath)
                snapshot['python_files'].append(metrics)
                
                # Update statistics
                snapshot['statistics']['total_files'] += 1
                snapshot['statistics']['total_lines'] += metrics['line_count']
                snapshot['statistics']['total_size_bytes'] += metrics['size_bytes']
                
                if metrics['has_path_setup']:
                    snapshot['statistics']['files_with_path_setup'] += 1
                if metrics['has_makedirs']:
                    snapshot['statistics']['files_with_makedirs'] += 1
                if metrics['has_csv_operations']:
                    snapshot['statistics']['files_with_csv_ops'] += 1
                if metrics['has_try_except']:
                    snapshot['statistics']['files_with_try_except'] += 1
                    
            except Exception as e:
                print(f"Error analyzing {filepath}: {e}")
        
        # Measure import performance for key modules
        test_modules = [
            'utils.config',
            'utils.csv_tracker',
            'utils.error_handling',
            'utils.logger'
        ]
        
        print("Measuring import performance...")
        for module in test_modules:
            import_time = self.measure_import_performance(module)
            if import_time > 0:
                snapshot['import_performance'][module] = import_time
                print(f"  {module}: {import_time:.3f}s")
        
        # Create directory structure map
        for root, dirs, files in os.walk(self.project_root):
            rel_root = Path(root).relative_to(self.project_root)
            snapshot['directory_structure'][str(rel_root)] = {
                'dirs': dirs,
                'py_files': [f for f in files if f.endswith('.py')]
            }
        
        return snapshot
    
    def save_snapshot(self, snapshot: Dict[str, Any], filename: str = None):
        """Save snapshot to JSON file."""
        if filename is None:
            filename = f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        filepath = self.project_root / filename
        with open(filepath, 'w') as f:
            json.dump(snapshot, f, indent=2)
        
        print(f"\nSnapshot saved to: {filename}")
        return filepath


def main():
    """Create and save codebase snapshot."""
    snapshotter = CodebaseSnapshot()
    snapshot = snapshotter.create_snapshot()
    
    # Print summary
    print("\n" + "="*60)
    print("SNAPSHOT SUMMARY")
    print("="*60)
    print(f"Total Python files: {snapshot['statistics']['total_files']}")
    print(f"Total lines of code: {snapshot['statistics']['total_lines']:,}")
    print(f"Total size: {snapshot['statistics']['total_size_bytes']:,} bytes")
    print(f"\nPattern occurrences:")
    print(f"  Files with sys.path.insert: {snapshot['statistics']['files_with_path_setup']}")
    print(f"  Files with os.makedirs: {snapshot['statistics']['files_with_makedirs']}")
    print(f"  Files with CSV operations: {snapshot['statistics']['files_with_csv_ops']}")
    print(f"  Files with try/except: {snapshot['statistics']['files_with_try_except']}")
    
    # Save snapshot
    snapshotter.save_snapshot(snapshot, "pre_refactoring_snapshot.json")
    
    # Also save a summary
    summary = {
        'timestamp': snapshot['timestamp'],
        'statistics': snapshot['statistics'],
        'import_performance': snapshot['import_performance']
    }
    
    with open('pre_refactoring_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    print("Summary saved to: pre_refactoring_summary.json")


if __name__ == "__main__":
    main()