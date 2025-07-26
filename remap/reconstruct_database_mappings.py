#!/usr/bin/env python3
"""
Reconstruct database mappings from all available sources.
This script searches through:
1. Git history for migration_state data
2. JSON files with UUID mappings
3. S3 file metadata
4. File size correlations
"""

import json
import boto3
import psycopg2
import subprocess
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

def search_git_for_database_data():
    """Search git history for any database exports or migration data"""
    print("\n1. Searching git history for database data...")
    
    # Search for commits that might have database exports
    result = subprocess.run([
        'git', 'log', '--all', '--grep=database.*export\|migration.*complete\|UUID.*mapping', 
        '-i', '--oneline'
    ], capture_output=True, text=True)
    
    commits = result.stdout.strip().split('\n') if result.stdout else []
    
    database_data = []
    for commit in commits[:10]:  # Check first 10 matching commits
        if not commit:
            continue
        commit_hash = commit.split()[0]
        
        # Check files in each commit
        files_result = subprocess.run([
            'git', 'show', '--name-only', '--format=', commit_hash
        ], capture_output=True, text=True)
        
        for filename in files_result.stdout.strip().split('\n'):
            if filename.endswith('.json') or filename.endswith('.sql'):
                # Try to get file content
                content_result = subprocess.run([
                    'git', 'show', f'{commit_hash}:{filename}'
                ], capture_output=True, text=True)
                
                if 'file_uuid' in content_result.stdout or 'migration_state' in content_result.stdout:
                    database_data.append({
                        'commit': commit_hash,
                        'file': filename,
                        'content_preview': content_result.stdout[:500]
                    })
    
    return database_data

def try_postgresql_connection():
    """Attempt to connect to PostgreSQL and retrieve migration_state data"""
    print("\n2. Attempting PostgreSQL connection...")
    
    try:
        # Try common connection parameters
        conn = psycopg2.connect(
            host="localhost",
            database="typing_clients",
            user="typing_user",
            password="rl5ATPkRzRKT"
        )
        
        with conn.cursor() as cur:
            # Check if migration_state table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'migration_state'
                );
            """)
            
            if cur.fetchone()[0]:
                print("   Found migration_state table!")
                
                # Get all migration data
                cur.execute("""
                    SELECT 
                        file_uuid, person_id, person_name, 
                        source_path, destination_path, file_size,
                        operation_status
                    FROM migration_state
                    WHERE file_uuid IS NOT NULL
                    ORDER BY person_id, started_at
                """)
                
                rows = cur.fetchall()
                print(f"   Retrieved {len(rows)} migration records")
                
                # Save to JSON
                migration_data = []
                for row in rows:
                    migration_data.append({
                        'file_uuid': str(row[0]),
                        'person_id': row[1],
                        'person_name': row[2],
                        'source_path': row[3],
                        'destination_path': row[4],
                        'file_size': row[5],
                        'operation_status': row[6]
                    })
                
                with open('recovered_migration_state.json', 'w') as f:
                    json.dump(migration_data, f, indent=2)
                
                return migration_data
            else:
                print("   migration_state table not found")
                
    except Exception as e:
        print(f"   PostgreSQL connection failed: {e}")
        
    return []

def analyze_s3_metadata():
    """Analyze S3 object metadata for clues about mappings"""
    print("\n3. Analyzing S3 object metadata...")
    
    s3 = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    
    metadata_clues = []
    
    try:
        # Check a sample of orphaned files for metadata
        paginator = s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix='files/'):
            for obj in page.get('Contents', [])[:10]:  # Sample first 10
                key = obj['Key']
                
                # Get object metadata
                try:
                    response = s3.head_object(Bucket=bucket_name, Key=key)
                    metadata = response.get('Metadata', {})
                    
                    if metadata:
                        metadata_clues.append({
                            'key': key,
                            'metadata': metadata,
                            'last_modified': obj['LastModified'],
                            'size': obj['Size']
                        })
                        
                except Exception as e:
                    continue
                    
    except Exception as e:
        print(f"   S3 metadata analysis failed: {e}")
        
    return metadata_clues

def correlate_upload_times():
    """Correlate S3 upload times with known events"""
    print("\n4. Correlating S3 upload times with migration events...")
    
    s3 = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    
    # Get all orphaned files with timestamps
    time_correlations = defaultdict(list)
    
    try:
        paginator = s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix='files/'):
            for obj in page.get('Contents', []):
                key = obj['Key']
                timestamp = obj['LastModified']
                
                # Group by upload hour
                hour_key = timestamp.strftime('%Y-%m-%d %H:00')
                time_correlations[hour_key].append({
                    'key': key,
                    'size': obj['Size'],
                    'exact_time': timestamp
                })
                
    except Exception as e:
        print(f"   Time correlation failed: {e}")
        
    # Find batch uploads
    batch_uploads = []
    for hour, files in time_correlations.items():
        if len(files) > 5:  # Likely a batch
            batch_uploads.append({
                'time_window': hour,
                'file_count': len(files),
                'total_size': sum(f['size'] for f in files),
                'files': files[:5]  # Sample
            })
    
    return batch_uploads

def create_reconstruction_report(git_data, pg_data, s3_metadata, time_correlations):
    """Create a comprehensive report of all findings"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f'database_reconstruction_report_{timestamp}.json'
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'findings': {
            'git_history': {
                'commits_searched': len(git_data),
                'database_references': git_data[:5]  # Top 5
            },
            'postgresql': {
                'connection_successful': len(pg_data) > 0,
                'records_recovered': len(pg_data),
                'sample_records': pg_data[:10] if pg_data else []
            },
            's3_metadata': {
                'files_with_metadata': len(s3_metadata),
                'metadata_samples': s3_metadata[:5]
            },
            'time_correlations': {
                'batch_uploads_found': len(time_correlations),
                'largest_batches': sorted(time_correlations, 
                                        key=lambda x: x['file_count'], 
                                        reverse=True)[:5]
            }
        },
        'recommendations': []
    }
    
    # Add recommendations based on findings
    if pg_data:
        report['recommendations'].append(
            "PostgreSQL data recovered! Use recovered_migration_state.json for mappings."
        )
    elif time_correlations:
        report['recommendations'].append(
            "Batch uploads detected. Cross-reference with known migration times."
        )
    else:
        report['recommendations'].append(
            "Consider content-based matching or manual review for remaining files."
        )
    
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
        
    print(f"\n5. Reconstruction report saved to: {report_file}")
    
    return report

def main():
    """Main reconstruction process"""
    print("Starting database mapping reconstruction...")
    print("="*60)
    
    # Search git history
    git_data = search_git_for_database_data()
    print(f"   Found {len(git_data)} potential database references in git")
    
    # Try PostgreSQL connection
    pg_data = try_postgresql_connection()
    
    # Analyze S3 metadata
    s3_metadata = analyze_s3_metadata()
    print(f"   Found {len(s3_metadata)} files with metadata")
    
    # Correlate upload times
    time_correlations = correlate_upload_times()
    print(f"   Found {len(time_correlations)} batch upload windows")
    
    # Create comprehensive report
    report = create_reconstruction_report(git_data, pg_data, s3_metadata, time_correlations)
    
    print("\n" + "="*60)
    print("Reconstruction complete!")
    
    if pg_data:
        print("✅ DATABASE DATA RECOVERED - Check recovered_migration_state.json")
    else:
        print("❌ No direct database access - Review report for alternative approaches")

if __name__ == "__main__":
    main()