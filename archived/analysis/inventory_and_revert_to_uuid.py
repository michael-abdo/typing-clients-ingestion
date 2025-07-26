#!/usr/bin/env python3
"""
Step 1: Create complete inventory and revert to UUID-based files/ structure
Ensures every file has a CSV reference before and after the move.
"""

import boto3
import csv
import json
import uuid
from collections import defaultdict
from datetime import datetime

def create_complete_inventory():
    """Create complete inventory of all files and their mappings"""
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    inventory = {
        'timestamp': datetime.now().isoformat(),
        'files': [],
        'csv_mappings': {},
        'total_files': 0,
        'files_with_csv_mapping': 0,
        'files_without_csv_mapping': 0
    }
    
    print("1. Scanning all S3 files...")
    
    # Get all files from S3 (excluding metadata)
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            key = obj['Key']
            
            # Skip metadata JSON files
            if key.startswith('clients/') and key.endswith('.json'):
                continue
            if key.endswith('.csv'):  # Skip CSV backups
                continue
                
            # This is a media file
            inventory['files'].append({
                'current_s3_key': key,
                'size': obj['Size'],
                'last_modified': obj['LastModified'].isoformat(),
                'person_id': None,
                'person_name': None,
                'original_filename': None,
                'proposed_uuid': None,
                'has_csv_mapping': False
            })
            inventory['total_files'] += 1
    
    print(f"   Found {inventory['total_files']} media files")
    
    # Load CSV mappings
    print("2. Loading CSV mappings...")
    
    with open('outputs/output.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_id = row.get('row_id', '').strip()
            if row_id and row_id.isdigit():
                person_id = int(row_id)
                
                # Parse s3_paths JSON
                s3_paths_str = row.get('s3_paths', '{}')
                if s3_paths_str and s3_paths_str != '{}':
                    try:
                        s3_paths = json.loads(s3_paths_str)
                        inventory['csv_mappings'][person_id] = {
                            'name': row.get('name', ''),
                            's3_paths': s3_paths,
                            'file_uuids': json.loads(row.get('file_uuids', '{}'))
                        }
                    except:
                        pass
    
    print(f"   Found CSV mappings for {len(inventory['csv_mappings'])} people")
    
    # Match files to CSV mappings
    print("3. Matching files to CSV mappings...")
    
    for file_info in inventory['files']:
        key = file_info['current_s3_key']
        
        # Extract person info from current path
        if '/' in key and not key.startswith('files/'):
            parts = key.split('/')
            
            # Handle both formats: "502/Sam_Torode/file.mp4" and "clients/472/uuid.mp4"
            if parts[0] == 'clients' and len(parts) >= 3 and parts[1].isdigit():
                # Format: clients/472/uuid.mp4
                person_id = int(parts[1])
                filename = parts[-1]  # UUID filename
                
                file_info['person_id'] = person_id
                file_info['person_name'] = f"Person_{person_id}"  # We'll get real name from CSV
                file_info['original_filename'] = filename
                
            elif len(parts) >= 3 and parts[0].isdigit():
                # Format: 502/Sam_Torode/file.mp4
                person_id = int(parts[0])
                person_name = parts[1].replace('_', ' ')
                filename = parts[-1]
                
                file_info['person_id'] = person_id
                file_info['person_name'] = person_name
                file_info['original_filename'] = filename
                
            # Check if this person has CSV mappings
            if person_id in inventory['csv_mappings']:
                file_info['has_csv_mapping'] = True
                inventory['files_with_csv_mapping'] += 1
                
                # Find or generate UUID for this file
                csv_data = inventory['csv_mappings'][person_id]
                file_uuids = csv_data.get('file_uuids', {})
                s3_paths = csv_data.get('s3_paths', {})
                
                # Look for existing UUID mapping
                found_uuid = None
                
                # For clients/ directory files, the filename IS the UUID
                if parts[0] == 'clients':
                    # Extract UUID from filename (remove extension)
                    uuid_from_filename = filename.split('.')[0] if '.' in filename else filename
                    
                    # Check if this UUID exists in CSV mappings
                    for fname, uuid_val in file_uuids.items():
                        if uuid_val == uuid_from_filename:
                            found_uuid = uuid_val
                            break
                    
                    # Also check s3_paths to see if this file is already mapped
                    for uuid_key, path in s3_paths.items():
                        if path == key:  # This exact S3 path is already mapped
                            found_uuid = uuid_key
                            break
                
                else:
                    # For regular person/name/file.ext structure
                    for fname, uuid_val in file_uuids.items():
                        if fname == filename:
                            found_uuid = uuid_val
                            break
                
                if found_uuid:
                    file_info['proposed_uuid'] = found_uuid
                else:
                    # Generate new UUID
                    file_info['proposed_uuid'] = str(uuid.uuid4())
            else:
                inventory['files_without_csv_mapping'] += 1
        else:
            # File already in files/ directory - check if it has CSV mapping
            if key.startswith('files/'):
                filename = key.split('/')[-1]
                uuid_from_filename = filename.split('.')[0] if '.' in filename else filename
                
                # Look through all CSV mappings to find this UUID
                found_mapping = False
                for person_id, csv_data in inventory['csv_mappings'].items():
                    s3_paths = csv_data.get('s3_paths', {})
                    file_uuids = csv_data.get('file_uuids', {})
                    
                    # Check if this UUID exists in any person's mappings
                    if uuid_from_filename in s3_paths or any(uuid_val == uuid_from_filename for uuid_val in file_uuids.values()):
                        file_info['has_csv_mapping'] = True
                        file_info['person_id'] = person_id
                        file_info['person_name'] = csv_data.get('name', f'Person_{person_id}')
                        file_info['original_filename'] = filename
                        file_info['proposed_uuid'] = uuid_from_filename
                        inventory['files_with_csv_mapping'] += 1
                        found_mapping = True
                        break
                
                if not found_mapping:
                    inventory['files_without_csv_mapping'] += 1
            else:
                # File in other location without proper structure
                inventory['files_without_csv_mapping'] += 1
    
    return inventory

def verify_100_percent_mapping(inventory):
    """Verify every file will have a CSV mapping"""
    print("4. Verifying 100% CSV mapping coverage...")
    
    unmapped_files = []
    for file_info in inventory['files']:
        if not file_info['has_csv_mapping'] or not file_info['proposed_uuid']:
            unmapped_files.append(file_info)
    
    if unmapped_files:
        print(f"   ❌ Found {len(unmapped_files)} files without proper mapping:")
        for f in unmapped_files[:5]:
            print(f"      {f['current_s3_key']}")
        if len(unmapped_files) > 5:
            print(f"      ... and {len(unmapped_files) - 5} more")
        return False
    else:
        print("   ✅ All files have proper CSV mappings!")
        return True

def move_files_to_uuid_structure(inventory, dry_run=True):
    """Move all files to files/ directory with UUID names"""
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    print(f"5. {'DRY RUN - ' if dry_run else ''}Moving files to UUID structure...")
    
    moves = []
    for file_info in inventory['files']:
        if file_info['has_csv_mapping'] and file_info['proposed_uuid']:
            current_key = file_info['current_s3_key']
            
            # Skip if already in files/ directory
            if current_key.startswith('files/'):
                continue
                
            # Determine file extension
            extension = ''
            if '.' in file_info['original_filename']:
                extension = '.' + file_info['original_filename'].split('.')[-1]
            
            new_key = f"files/{file_info['proposed_uuid']}{extension}"
            
            moves.append({
                'old_key': current_key,
                'new_key': new_key,
                'person_id': file_info['person_id'],
                'filename': file_info['original_filename']
            })
    
    print(f"   {len(moves)} files to move")
    
    if not dry_run:
        moved = 0
        for move in moves:
            try:
                # Copy to new location
                copy_source = {'Bucket': bucket, 'Key': move['old_key']}
                s3.copy_object(
                    CopySource=copy_source,
                    Bucket=bucket,
                    Key=move['new_key']
                )
                
                # Delete old location
                s3.delete_object(Bucket=bucket, Key=move['old_key'])
                
                moved += 1
                print(f"   Moved: {move['old_key']} -> {move['new_key']}")
                
            except Exception as e:
                print(f"   Error moving {move['old_key']}: {e}")
        
        print(f"   Successfully moved {moved} files")
    else:
        print("   DRY RUN - showing first 10 moves:")
        for move in moves[:10]:
            print(f"      {move['old_key']} -> {move['new_key']}")
        if len(moves) > 10:
            print(f"      ... and {len(moves) - 10} more")
    
    return moves

def update_csv_with_uuid_paths(inventory, moves, dry_run=True):
    """Update CSV to reference the new UUID file paths"""
    print(f"6. {'DRY RUN - ' if dry_run else ''}Updating CSV with UUID paths...")
    
    # Create mapping from old path to new path
    path_mapping = {}
    for move in moves:
        path_mapping[move['old_key']] = move['new_key']
    
    if not dry_run:
        # Backup current CSV
        backup_file = f'outputs/output.csv.backup_uuid_revert_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        import shutil
        shutil.copy('outputs/output.csv', backup_file)
        print(f"   Created backup: {backup_file}")
        
        # Update CSV
        rows = []
        with open('outputs/output.csv', 'r') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            
            for row in reader:
                row_id = row.get('row_id', '').strip()
                if row_id and row_id.isdigit():
                    person_id = int(row_id)
                    
                    # Update s3_paths to point to UUID files
                    s3_paths_str = row.get('s3_paths', '{}')
                    if s3_paths_str and s3_paths_str != '{}':
                        try:
                            s3_paths = json.loads(s3_paths_str)
                            updated_paths = {}
                            
                            for uuid_key, old_path in s3_paths.items():
                                if old_path in path_mapping:
                                    updated_paths[uuid_key] = path_mapping[old_path]
                                else:
                                    updated_paths[uuid_key] = old_path
                            
                            row['s3_paths'] = json.dumps(updated_paths)
                        except:
                            pass
                
                rows.append(row)
        
        # Write updated CSV
        with open('outputs/output.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"   Updated CSV with {len(path_mapping)} new UUID paths")
    else:
        print(f"   Would update {len(path_mapping)} paths in CSV")

def main():
    print("REVERTING TO UUID-BASED FILE STRUCTURE")
    print("=" * 60)
    print("Goal: Every file in files/ with UUID name + CSV reference")
    print()
    
    # Step 1: Create complete inventory
    inventory = create_complete_inventory()
    
    # Step 2: Verify 100% mapping
    if not verify_100_percent_mapping(inventory):
        print("\n❌ Cannot proceed - not all files have CSV mappings!")
        print("Please fix missing mappings before running revert.")
        return
    
    # Step 3: Plan the moves (dry run)
    moves = move_files_to_uuid_structure(inventory, dry_run=True)
    
    # Step 4: Plan CSV updates (dry run)
    update_csv_with_uuid_paths(inventory, moves, dry_run=True)
    
    # Summary
    print(f"\n" + "=" * 60)
    print("REVERT PLAN SUMMARY:")
    print(f"- Total files: {inventory['total_files']}")
    print(f"- Files with CSV mapping: {inventory['files_with_csv_mapping']}")
    print(f"- Files to move to UUID structure: {len(moves)}")
    print(f"- All files will be in files/ directory with UUID names")
    print(f"- All files will have CSV references")
    print(f"\nTo execute: python3 inventory_and_revert_to_uuid.py --execute")

if __name__ == "__main__":
    import sys
    if '--execute' in sys.argv:
        print("EXECUTING REVERT TO UUID STRUCTURE...")
        inventory = create_complete_inventory()
        if verify_100_percent_mapping(inventory):
            moves = move_files_to_uuid_structure(inventory, dry_run=False)
            update_csv_with_uuid_paths(inventory, moves, dry_run=False)
            print("\n✅ Revert completed successfully!")
        else:
            print("\n❌ Cannot execute - mapping verification failed!")
    else:
        main()