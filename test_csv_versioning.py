#!/usr/bin/env python3
"""
Test CSV Versioning - Demonstrates automatic CSV versioning in S3
"""

import pandas as pd
import time
from datetime import datetime
from utils.csv_manager import CSVManager
from utils.csv_s3_versioning import get_csv_versioning

def test_csv_versioning():
    """Test automatic CSV versioning functionality"""
    print("=== CSV S3 Versioning Test ===\n")
    
    # Create a test CSV file
    csv_path = "test_versioning.csv"
    manager = CSVManager(csv_path=csv_path, auto_backup=False)
    
    # Create initial data
    print("1. Creating initial CSV version...")
    data1 = pd.DataFrame({
        'row_id': ['1', '2', '3'],
        'name': ['Alice', 'Bob', 'Charlie'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
        'type': ['Type1', 'Type2', 'Type1'],
        'link': ['http://example.com/1', 'http://example.com/2', 'http://example.com/3']
    })
    
    success = manager.safe_csv_write(data1, operation_name="initial_create")
    if success:
        print("✅ Initial CSV created and uploaded to S3")
    else:
        print("❌ Failed to create initial CSV")
        return
    
    # Wait a bit to ensure different timestamps
    time.sleep(2)
    
    # Update the CSV with new data
    print("\n2. Updating CSV with new rows...")
    data2 = pd.DataFrame({
        'row_id': ['1', '2', '3', '4', '5'],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 
                  'david@example.com', 'eve@example.com'],
        'type': ['Type1', 'Type2', 'Type1', 'Type2', 'Type1'],
        'link': ['http://example.com/1', 'http://example.com/2', 'http://example.com/3',
                 'http://example.com/4', 'http://example.com/5']
    })
    
    success = manager.safe_csv_write(data2, operation_name="add_new_rows")
    if success:
        print("✅ Updated CSV created and uploaded to S3")
    else:
        print("❌ Failed to update CSV")
        return
    
    # Wait a bit
    time.sleep(2)
    
    # Modify existing data
    print("\n3. Modifying existing data...")
    data3 = data2.copy()
    data3.loc[data3['name'] == 'Bob', 'email'] = 'bob.new@example.com'
    data3.loc[data3['name'] == 'David', 'type'] = 'Type3'
    
    success = manager.safe_csv_write(data3, operation_name="modify_existing")
    if success:
        print("✅ Modified CSV created and uploaded to S3")
    else:
        print("❌ Failed to modify CSV")
        return
    
    # List all versions
    print("\n4. Listing all CSV versions in S3...")
    versioning = get_csv_versioning()
    versions = versioning.list_csv_versions(prefix_filter="test_versioning")
    
    if versions:
        print(f"Found {len(versions)} versions:\n")
        for i, version in enumerate(versions):
            print(f"Version {i+1}:")
            print(f"  - Filename: {version['filename']}")
            print(f"  - Size: {version['size']} bytes")
            print(f"  - Modified: {version['last_modified']}")
            print(f"  - URL: {version['url']}")
            print()
    else:
        print("No versions found in S3")
    
    # Get latest version
    print("5. Getting latest version...")
    latest = versioning.get_latest_version(prefix_filter="test_versioning")
    if latest:
        print(f"Latest version: {latest['filename']}")
        print(f"Modified: {latest['last_modified']}")
    else:
        print("No latest version found")
    
    # Clean up
    import os
    if os.path.exists(csv_path):
        os.remove(csv_path)
        print(f"\n✅ Cleaned up local test file: {csv_path}")
    
    print("\n=== Test Complete ===")
    print("\nCSV versioning is now automatically enabled for all CSV writes when S3 storage mode is active!")
    print("Every time the pipeline updates a CSV, a timestamped version is saved to S3.")
    print(f"\nVersions are organized in S3 as: csv-versions/YYYY/MM/filename_YYYY-MM-DD_HHMMSS.csv")

if __name__ == "__main__":
    test_csv_versioning()