#!/usr/bin/env python3
"""
Migration script to convert empty strings to NaN in CSV files.
This ensures consistent null representation and prevents false positive corruption detection.
"""

import pandas as pd
import numpy as np
import shutil
from datetime import datetime
import os
from utils.csv_tracker import validate_csv_integrity, safe_csv_write, create_csv_backup
from utils.file_lock import file_lock

def analyze_empty_strings(csv_path: str) -> dict:
    """Analyze current empty string usage in CSV"""
    df = pd.read_csv(csv_path, dtype=str)
    
    analysis = {
        'total_rows': len(df),
        'columns_with_empty_strings': {},
        'affected_rows': set()
    }
    
    # Check each column for empty strings
    for col in df.columns:
        empty_count = (df[col] == '').sum()
        if empty_count > 0:
            analysis['columns_with_empty_strings'][col] = empty_count
            # Track which rows have empty strings
            rows_with_empty = df[df[col] == ''].index.tolist()
            analysis['affected_rows'].update(rows_with_empty)
    
    analysis['total_affected_rows'] = len(analysis['affected_rows'])
    
    return analysis

def migrate_empty_strings_to_nan(csv_path: str, dry_run: bool = True) -> bool:
    """
    Convert all empty strings to NaN in the CSV file.
    
    Args:
        csv_path: Path to CSV file
        dry_run: If True, only analyze without making changes
        
    Returns:
        True if migration successful, False otherwise
    """
    print(f"{'[DRY RUN] ' if dry_run else ''}Migrating empty strings to NaN in: {csv_path}")
    
    # Analyze current state
    print("\n=== Pre-migration Analysis ===")
    pre_analysis = analyze_empty_strings(csv_path)
    
    if not pre_analysis['columns_with_empty_strings']:
        print("No empty strings found in CSV. No migration needed.")
        return True
    
    print(f"Total rows: {pre_analysis['total_rows']}")
    print(f"Rows with empty strings: {pre_analysis['total_affected_rows']}")
    print("\nColumns with empty strings:")
    for col, count in pre_analysis['columns_with_empty_strings'].items():
        print(f"  {col}: {count} empty strings")
    
    # Check current validation status
    is_valid, message = validate_csv_integrity(csv_path)
    print(f"\nCurrent validation status: {'Valid' if is_valid else f'Invalid - {message}'}")
    
    if dry_run:
        # Simulate the migration
        df = pd.read_csv(csv_path, dtype=str)
        df_simulated = df.replace('', np.nan)
        
        # Check what validation would be after migration
        temp_path = csv_path + '.temp_simulation'
        df_simulated.to_csv(temp_path, index=False)
        
        is_valid_after, message_after = validate_csv_integrity(temp_path)
        print(f"\nPost-migration validation (simulated): {'Valid' if is_valid_after else f'Invalid - {message_after}'}")
        
        # Clean up temp file
        os.remove(temp_path)
        
        print("\n[DRY RUN] No changes made. Run with dry_run=False to apply migration.")
        return True
    
    # Actual migration
    with file_lock(f'{csv_path}.lock'):
        # Create backup
        backup_path = create_csv_backup(csv_path, 'migrate_empty_strings')
        print(f"\nCreated backup: {backup_path}")
        
        # Read CSV with string dtypes to preserve data
        df = pd.read_csv(csv_path, dtype=str)
        
        # Replace all empty strings with NaN
        df_migrated = df.replace('', np.nan)
        
        # Count changes
        changes_made = 0
        for col in df.columns:
            changes_in_col = ((df[col] == '') & (pd.isna(df_migrated[col]))).sum()
            changes_made += changes_in_col
        
        print(f"\nReplacing {changes_made} empty strings with NaN...")
        
        # Save migrated CSV
        expected_cols = df.columns.tolist()
        success = safe_csv_write(df_migrated, csv_path, 'migrate_empty_strings', expected_cols)
        
        if success:
            print("✅ Migration completed successfully")
            
            # Verify post-migration state
            is_valid_after, message_after = validate_csv_integrity(csv_path)
            print(f"\nPost-migration validation: {'Valid' if is_valid_after else f'Invalid - {message_after}'}")
            
            # Final analysis
            post_analysis = analyze_empty_strings(csv_path)
            if post_analysis['columns_with_empty_strings']:
                print("\n⚠️ Warning: Some empty strings remain:")
                for col, count in post_analysis['columns_with_empty_strings'].items():
                    print(f"  {col}: {count} empty strings")
            else:
                print("\n✅ All empty strings successfully converted to NaN")
            
            return True
        else:
            print("❌ Migration failed - CSV write validation failed")
            print(f"Backup preserved at: {backup_path}")
            return False

def main():
    """Run the migration with safety checks"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate CSV empty strings to NaN')
    parser.add_argument('--csv', default='outputs/output.csv', help='Path to CSV file')
    parser.add_argument('--apply', action='store_true', help='Apply migration (default is dry run)')
    
    args = parser.parse_args()
    
    # Always do dry run first
    if args.apply:
        print("=== Running migration ===")
        success = migrate_empty_strings_to_nan(args.csv, dry_run=False)
    else:
        print("=== Dry run mode (use --apply to make changes) ===")
        success = migrate_empty_strings_to_nan(args.csv, dry_run=True)
    
    return 0 if success else 1

if __name__ == "__main__":
    import sys
    sys.exit(main())