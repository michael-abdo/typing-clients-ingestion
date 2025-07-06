#!/usr/bin/env python3
"""
Apply DRY Refactoring - Automated script to apply common refactoring patterns
This script helps implement the DRY refactoring procedure step by step.
"""

import os
import re
import sys
from pathlib import Path
from typing import List, Tuple
import argparse
import shutil
from datetime import datetime


class DRYRefactorer:
    """Handles automated DRY refactoring operations."""
    
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.changes_made = []
        self.project_root = Path(__file__).parent
        
    def backup_file(self, filepath: Path) -> Path:
        """Create a backup of the file before modifying."""
        if not self.dry_run:
            backup_path = filepath.with_suffix(filepath.suffix + '.backup')
            shutil.copy2(filepath, backup_path)
            return backup_path
        return filepath
    
    def log_change(self, filepath: Path, change_type: str, details: str = ""):
        """Log a change that was made or would be made."""
        action = "Would update" if self.dry_run else "Updated"
        self.changes_made.append({
            'file': str(filepath),
            'action': action,
            'type': change_type,
            'details': details
        })
        print(f"{action} {filepath.name}: {change_type}")
        if details:
            print(f"  → {details}")
    
    def refactor_path_setup(self, filepath: Path) -> bool:
        """Replace sys.path.insert patterns with init_project_imports()."""
        with open(filepath, 'r') as f:
            content = f.read()
        
        # Pattern to match sys.path.insert
        pattern = r'(import sys\s*\n\s*import os\s*\n\s*)?sys\.path\.insert\(0,\s*os\.path\.dirname\(os\.path\.dirname\(os\.path\.abspath\(__file__\)\)\)\)'
        
        if re.search(pattern, content):
            # Check if utils.path_setup is already imported
            if 'from utils.path_setup import' not in content:
                # Replace the pattern
                replacement = 'from utils.path_setup import init_project_imports\ninit_project_imports()'
                new_content = re.sub(pattern, replacement, content)
                
                if not self.dry_run:
                    self.backup_file(filepath)
                    with open(filepath, 'w') as f:
                        f.write(new_content)
                
                self.log_change(filepath, "Path setup refactored", 
                              "Replaced sys.path.insert with init_project_imports()")
                return True
        
        return False
    
    def refactor_makedirs(self, filepath: Path) -> bool:
        """Replace os.makedirs patterns with ensure_directory_exists()."""
        with open(filepath, 'r') as f:
            content = f.read()
        
        # Pattern to match os.makedirs
        pattern = r'os\.makedirs\(([^,\)]+),\s*exist_ok=True\)'
        
        matches = list(re.finditer(pattern, content))
        if matches:
            new_content = content
            
            # Add import if not present
            if 'from utils.path_setup import' not in content:
                imports_added = False
                # Find where to add import (after other imports)
                import_match = re.search(r'((?:from|import)\s+\S+.*\n)+', content)
                if import_match:
                    insert_pos = import_match.end()
                    new_content = (content[:insert_pos] + 
                                 "from utils.path_setup import ensure_directory_exists\n" +
                                 "from pathlib import Path\n" +
                                 content[insert_pos:])
                    imports_added = True
            
            # Replace makedirs calls
            for match in reversed(matches):  # Reverse to maintain positions
                dir_var = match.group(1).strip()
                replacement = f'ensure_directory_exists(Path({dir_var}))'
                new_content = new_content[:match.start()] + replacement + new_content[match.end():]
            
            if not self.dry_run:
                self.backup_file(filepath)
                with open(filepath, 'w') as f:
                    f.write(new_content)
            
            self.log_change(filepath, "Directory creation refactored",
                          f"Replaced {len(matches)} os.makedirs calls")
            return True
        
        return False
    
    def find_python_files(self, exclude_dirs: List[str] = None) -> List[Path]:
        """Find all Python files in the project."""
        exclude_dirs = exclude_dirs or ['__pycache__', '.git', 'venv', '.venv', 
                                       'backup', 'backups', 'archive']
        
        python_files = []
        for root, dirs, files in os.walk(self.project_root):
            # Remove excluded directories from search
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            
            for file in files:
                if file.endswith('.py'):
                    python_files.append(Path(root) / file)
        
        return python_files
    
    def analyze_csv_operations(self, filepath: Path) -> List[str]:
        """Analyze file for direct CSV operations that should use CSVManager."""
        issues = []
        
        with open(filepath, 'r') as f:
            content = f.read()
        
        # Check for direct CSV usage without CSVManager
        if 'import csv' in content and 'CSVManager' not in content:
            if re.search(r'csv\.(Dict)?Reader|csv\.(Dict)?Writer', content):
                issues.append("Uses direct csv.Reader/Writer instead of CSVManager")
        
        # Check for pandas CSV operations
        if 'pd.read_csv' in content or 'to_csv(' in content:
            if 'CSVManager' not in content:
                issues.append("Uses pandas CSV operations instead of CSVManager")
        
        return issues
    
    def analyze_error_handling(self, filepath: Path) -> List[str]:
        """Analyze file for error handling that should use decorators."""
        issues = []
        
        with open(filepath, 'r') as f:
            content = f.read()
        
        # Check for generic try/except patterns
        try_except_pattern = r'try:\s*\n.*?except\s+(?:Exception|FileNotFoundError|IOError)'
        matches = re.findall(try_except_pattern, content, re.DOTALL)
        
        if matches and '@handle_' not in content:
            issues.append(f"Contains {len(matches)} try/except blocks that could use error decorators")
        
        return issues
    
    def generate_report(self) -> str:
        """Generate a report of all changes made or to be made."""
        report = ["# DRY Refactoring Report", f"Generated: {datetime.now()}", ""]
        
        if self.dry_run:
            report.append("## DRY RUN MODE - No files were modified")
        else:
            report.append("## Changes Applied")
        
        report.append("")
        
        # Group changes by type
        by_type = {}
        for change in self.changes_made:
            change_type = change['type']
            if change_type not in by_type:
                by_type[change_type] = []
            by_type[change_type].append(change)
        
        for change_type, changes in by_type.items():
            report.append(f"### {change_type} ({len(changes)} files)")
            for change in changes:
                report.append(f"- {change['file']}")
                if change['details']:
                    report.append(f"  - {change['details']}")
            report.append("")
        
        return "\n".join(report)
    
    def apply_refactorings(self, steps: List[str] = None):
        """Apply selected refactoring steps."""
        steps = steps or ['path_setup', 'makedirs']
        
        python_files = self.find_python_files()
        print(f"Found {len(python_files)} Python files to analyze")
        print("")
        
        for filepath in python_files:
            if 'path_setup' in steps:
                self.refactor_path_setup(filepath)
            
            if 'makedirs' in steps:
                self.refactor_makedirs(filepath)
            
            if 'analyze_csv' in steps:
                issues = self.analyze_csv_operations(filepath)
                if issues:
                    self.log_change(filepath, "CSV operations to refactor", 
                                  "; ".join(issues))
            
            if 'analyze_errors' in steps:
                issues = self.analyze_error_handling(filepath)
                if issues:
                    self.log_change(filepath, "Error handling to refactor",
                                  "; ".join(issues))


def main():
    parser = argparse.ArgumentParser(
        description="Apply DRY refactoring to the codebase"
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Show what would be changed without modifying files (default: True)'
    )
    parser.add_argument(
        '--apply',
        action='store_true', 
        help='Actually apply the changes (disables dry-run)'
    )
    parser.add_argument(
        '--steps',
        nargs='+',
        choices=['path_setup', 'makedirs', 'analyze_csv', 'analyze_errors', 'all'],
        default=['path_setup', 'makedirs'],
        help='Which refactoring steps to apply'
    )
    parser.add_argument(
        '--report',
        type=str,
        default='dry_refactoring_report.md',
        help='Output file for the refactoring report'
    )
    
    args = parser.parse_args()
    
    # If --apply is used, disable dry run
    dry_run = not args.apply
    
    if not dry_run:
        # Check if running in CI/automation mode
        import os
        if not os.environ.get('CI') and os.isatty(0):
            response = input("⚠️  This will modify files in place. Continue? (yes/no): ")
            if response.lower() != 'yes':
                print("Aborted.")
                return
        else:
            print("⚠️  Running in automation mode - applying changes without confirmation")
    
    refactorer = DRYRefactorer(dry_run=dry_run)
    
    # Expand 'all' to all steps
    steps = args.steps
    if 'all' in steps:
        steps = ['path_setup', 'makedirs', 'analyze_csv', 'analyze_errors']
    
    # Apply refactorings
    refactorer.apply_refactorings(steps)
    
    # Generate report
    report = refactorer.generate_report()
    
    # Save report
    with open(args.report, 'w') as f:
        f.write(report)
    
    print(f"\n{'='*60}")
    print(f"Report saved to: {args.report}")
    print(f"Total changes: {len(refactorer.changes_made)}")
    
    if dry_run:
        print("\nTo apply these changes, run with --apply flag")


if __name__ == "__main__":
    main()