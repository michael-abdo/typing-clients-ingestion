#!/usr/bin/env python3
"""Comprehensive git history analysis for orphaned file recovery"""

import boto3
import pandas as pd
import json
import subprocess
import re
from datetime import datetime
from orphaned_file_recovery_tracker import get_tracker

def get_remaining_orphaned_files():
    """Get current list of remaining orphaned files"""
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
    remaining_files = []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if not obj['Key'].endswith('/'):
                filename = obj['Key'].split('/')[-1]
                uuid = filename.split('.')[0]
                extension = filename.split('.')[-1] if '.' in filename else 'no_ext'
                
                remaining_files.append({
                    'uuid': uuid,
                    'filename': filename,
                    'extension': extension,
                    'size_mb': obj['Size'] / (1024 * 1024),
                    'last_modified': obj['LastModified'],
                    's3_key': obj['Key']
                })
    
    return remaining_files

def analyze_git_commit_history():
    """Analyze git commit history for file-client associations"""
    print("🔍 ANALYZING GIT COMMIT HISTORY")
    print("=" * 50)
    
    try:
        # Get comprehensive commit log with file changes
        cmd = [
            'git', 'log', '--all', '--name-only', '--pretty=format:%H|%ai|%s|%an', 
            '--since=2025-07-01', '--grep=upload', '--grep=download', '--grep=client',
            '--grep=file', '--grep=uuid', '-i'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')
        
        if result.returncode != 0:
            print(f"   ❌ Git log failed: {result.stderr}")
            return []
        
        commits = []
        current_commit = None
        
        for line in result.stdout.split('\n'):
            if '|' in line and len(line.split('|')) >= 4:
                # New commit header
                parts = line.split('|')
                current_commit = {
                    'hash': parts[0],
                    'date': parts[1],
                    'message': parts[2],
                    'author': parts[3],
                    'files': []
                }
                commits.append(current_commit)
            elif line.strip() and current_commit:
                # File in this commit
                current_commit['files'].append(line.strip())
        
        print(f"   Found {len(commits)} relevant commits")
        
        # Extract potential UUID and client mentions
        uuid_pattern = re.compile(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}')
        client_mappings = []
        
        for commit in commits:
            message = commit['message'].lower()
            
            # Look for UUIDs in commit message
            uuids_in_message = uuid_pattern.findall(commit['message'])
            
            # Look for client names or patterns
            client_indicators = []
            if any(keyword in message for keyword in ['client', 'user', 'upload', 'download', 'file']):
                client_indicators.append(f"commit_msg: {commit['message'][:50]}...")
            
            if uuids_in_message:
                print(f"   📍 Commit {commit['hash'][:8]}: Found UUIDs in message")
                print(f"      UUIDs: {uuids_in_message[:3]}...")
                print(f"      Message: {commit['message'][:60]}...")
                
                client_mappings.append({
                    'commit_hash': commit['hash'],
                    'uuids': uuids_in_message,
                    'message': commit['message'],
                    'date': commit['date'],
                    'author': commit['author'],
                    'files_changed': commit['files']
                })
        
        return client_mappings
        
    except Exception as e:
        print(f"   ❌ Error analyzing git commits: {str(e)}")
        return []

def search_git_logs_for_orphaned_uuids(orphaned_files):
    """Search git logs for specific orphaned UUIDs"""
    print(f"\n🎯 SEARCHING GIT LOGS FOR ORPHANED UUIDs")
    print("=" * 50)
    
    uuid_mappings = []
    orphaned_uuids = [f['uuid'] for f in orphaned_files]
    
    print(f"   Searching for {len(orphaned_uuids)} orphaned UUIDs in git history...")
    
    # Search in batches to avoid command line length limits
    batch_size = 10
    
    for i in range(0, len(orphaned_uuids), batch_size):
        batch = orphaned_uuids[i:i+batch_size]
        
        try:
            # Search all git history for these UUIDs
            cmd = ['git', 'log', '--all', '--grep=' + '|'.join(batch), '-E', '--pretty=format:%H|%ai|%s|%an']
            
            result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')
            
            if result.returncode == 0 and result.stdout.strip():
                for line in result.stdout.split('\n'):
                    if '|' in line:
                        parts = line.split('|')
                        if len(parts) >= 4:
                            commit_hash, date, message, author = parts[0], parts[1], parts[2], parts[3]
                            
                            # Find which UUIDs match
                            matching_uuids = [uuid for uuid in batch if uuid in message]
                            
                            if matching_uuids:
                                print(f"   ✅ Found UUIDs {matching_uuids[:2]}... in commit {commit_hash[:8]}")
                                print(f"      Message: {message[:60]}...")
                                
                                uuid_mappings.append({
                                    'commit_hash': commit_hash,
                                    'matching_uuids': matching_uuids,
                                    'message': message,
                                    'date': date,
                                    'author': author
                                })
        
        except Exception as e:
            print(f"   ⚠️  Error searching batch {i//batch_size + 1}: {str(e)}")
            continue
    
    return uuid_mappings

def analyze_git_branches_for_context():
    """Analyze git branches for additional context"""
    print(f"\n🌿 ANALYZING GIT BRANCHES")
    print("=" * 40)
    
    try:
        # Get all branches
        cmd = ['git', 'branch', '-a']
        result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')
        
        branches = []
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                branch = line.strip().replace('* ', '').replace('remotes/', '')
                if branch and branch != 'HEAD':
                    branches.append(branch)
        
        print(f"   Found {len(branches)} branches")
        
        # Analyze relevant branches for file operations
        relevant_branches = []
        for branch in branches:
            if any(keyword in branch.lower() for keyword in ['file', 'upload', 'download', 'client', 's3', 'migration']):
                relevant_branches.append(branch)
        
        print(f"   Relevant branches: {relevant_branches}")
        
        branch_analysis = []
        for branch in relevant_branches[:5]:  # Limit to top 5
            try:
                # Get recent commits in this branch
                cmd = ['git', 'log', branch, '--oneline', '-10']
                result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')
                
                if result.returncode == 0:
                    commits = result.stdout.strip().split('\n')
                    branch_analysis.append({
                        'branch': branch,
                        'recent_commits': commits,
                        'commit_count': len(commits)
                    })
                    
                    print(f"   📍 {branch}: {len(commits)} recent commits")
            
            except Exception as e:
                continue
        
        return branch_analysis
        
    except Exception as e:
        print(f"   ❌ Error analyzing branches: {str(e)}")
        return []

def search_git_reflog():
    """Search git reflog for additional history"""
    print(f"\n📜 SEARCHING GIT REFLOG")
    print("=" * 30)
    
    try:
        cmd = ['git', 'reflog', '--grep=upload', '--grep=file', '--grep=client', '-i']
        result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')
        
        reflog_entries = []
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if line.strip():
                    reflog_entries.append(line.strip())
        
        print(f"   Found {len(reflog_entries)} relevant reflog entries")
        
        # Look for UUIDs in reflog
        uuid_pattern = re.compile(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}')
        
        for entry in reflog_entries[:10]:  # Show first 10
            uuids = uuid_pattern.findall(entry)
            if uuids:
                print(f"   📍 {entry[:60]}...")
                print(f"      UUIDs: {uuids[:2]}...")
        
        return reflog_entries
        
    except Exception as e:
        print(f"   ❌ Error searching reflog: {str(e)}")
        return []

def analyze_file_history_in_logs_directory():
    """Analyze files in logs directory for additional UUID mappings"""
    print(f"\n📂 ANALYZING LOGS DIRECTORY")
    print("=" * 40)
    
    try:
        # Get all log files
        cmd = ['find', 'logs/', '-name', '*.log', '-o', '-name', '*.json', '-o', '-name', '*.txt']
        result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')
        
        log_files = []
        if result.returncode == 0:
            log_files = [f.strip() for f in result.stdout.split('\n') if f.strip()]
        
        print(f"   Found {len(log_files)} log files to analyze")
        
        # Analyze each log file for UUID mappings
        mappings_found = []
        
        for log_file in log_files[:20]:  # Limit to prevent timeout
            try:
                with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                # Look for UUID patterns and client names
                uuid_pattern = re.compile(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}')
                uuids = uuid_pattern.findall(content)
                
                if uuids:
                    # Look for client indicators in the same content
                    client_patterns = [
                        r'client[:\s]+([A-Za-z\s]+)',
                        r'name[:\s]+["\']([A-Za-z\s]+)["\']',
                        r'user[:\s]+([A-Za-z\s]+)',
                        r'row_id[:\s]+(\d+)'
                    ]
                    
                    client_info = []
                    for pattern in client_patterns:
                        matches = re.findall(pattern, content, re.IGNORECASE)
                        client_info.extend(matches)
                    
                    if client_info:
                        print(f"   📍 {log_file}: {len(uuids)} UUIDs, potential clients: {client_info[:3]}...")
                        
                        mappings_found.append({
                            'log_file': log_file,
                            'uuids': uuids,
                            'client_info': client_info,
                            'uuid_count': len(uuids)
                        })
            
            except Exception as e:
                continue
        
        return mappings_found
        
    except Exception as e:
        print(f"   ❌ Error analyzing logs directory: {str(e)}")
        return []

def correlate_git_findings_with_orphaned_files(git_findings, orphaned_files):
    """Correlate all git findings with remaining orphaned files"""
    print(f"\n🎯 CORRELATING GIT FINDINGS WITH ORPHANED FILES")
    print("=" * 60)
    
    orphaned_uuids = set(f['uuid'] for f in orphaned_files)
    confirmed_mappings = []
    
    # Process all types of git findings
    all_findings = []
    
    # Add commit mappings
    if 'commit_mappings' in git_findings:
        for mapping in git_findings['commit_mappings']:
            for uuid in mapping.get('uuids', []):
                if uuid in orphaned_uuids:
                    all_findings.append({
                        'uuid': uuid,
                        'source': 'git_commit',
                        'evidence': f"Commit {mapping['commit_hash'][:8]}: {mapping['message'][:50]}",
                        'author': mapping.get('author'),
                        'date': mapping.get('date'),
                        'confidence': 0.7
                    })
    
    # Add log mappings
    if 'log_mappings' in git_findings:
        for mapping in git_findings['log_mappings']:
            for uuid in mapping.get('uuids', []):
                if uuid in orphaned_uuids:
                    all_findings.append({
                        'uuid': uuid,
                        'source': 'log_file',
                        'evidence': f"Log file {mapping['log_file']}: {mapping['client_info'][:50]}",
                        'client_info': mapping.get('client_info', []),
                        'confidence': 0.6
                    })
    
    # Group findings by UUID
    uuid_findings = {}
    for finding in all_findings:
        uuid = finding['uuid']
        if uuid not in uuid_findings:
            uuid_findings[uuid] = []
        uuid_findings[uuid].append(finding)
    
    print(f"   Found evidence for {len(uuid_findings)} orphaned UUIDs")
    
    # Try to map to specific clients
    df = pd.read_csv('outputs/output.csv')
    
    for uuid, findings in uuid_findings.items():
        print(f"\n   🔍 Analyzing UUID {uuid}:")
        
        # Combine evidence from all sources
        combined_confidence = sum(f['confidence'] for f in findings) / len(findings)
        combined_evidence = "; ".join(f['evidence'] for f in findings)
        
        # Look for client matches
        potential_clients = []
        
        for finding in findings:
            if 'client_info' in finding and finding['client_info']:
                for client_hint in finding['client_info']:
                    if isinstance(client_hint, str) and len(client_hint.strip()) > 2:
                        # Try to match with CSV clients
                        for _, row in df.iterrows():
                            client_name = str(row['name']).lower()
                            hint_lower = client_hint.lower().strip()
                            
                            if hint_lower in client_name or client_name in hint_lower:
                                potential_clients.append({
                                    'row_id': row['row_id'],
                                    'name': row['name'],
                                    'match_hint': client_hint,
                                    'confidence': 0.8
                                })
            
            # Check author names as potential clients
            if 'author' in finding and finding['author']:
                author = finding['author'].lower()
                for _, row in df.iterrows():
                    client_name = str(row['name']).lower()
                    name_parts = client_name.split()
                    
                    matches = sum(1 for part in name_parts if len(part) > 2 and part in author)
                    if matches >= 1:
                        potential_clients.append({
                            'row_id': row['row_id'],
                            'name': row['name'],
                            'match_hint': f"author: {finding['author']}",
                            'confidence': 0.6
                        })
        
        # Remove duplicates and rank by confidence
        unique_clients = {}
        for client in potential_clients:
            row_id = client['row_id']
            if row_id not in unique_clients or client['confidence'] > unique_clients[row_id]['confidence']:
                unique_clients[row_id] = client
        
        if unique_clients:
            best_client = max(unique_clients.values(), key=lambda x: x['confidence'])
            
            print(f"      Best match: {best_client['name']} (Row {best_client['row_id']}) - {best_client['confidence']:.1%}")
            print(f"      Evidence: {combined_evidence[:80]}...")
            
            if best_client['confidence'] >= 0.6:
                confirmed_mappings.append({
                    'uuid': uuid,
                    'client_row_id': best_client['row_id'],
                    'client_name': best_client['name'],
                    'confidence': best_client['confidence'],
                    'method': 'git_analysis',
                    'evidence': combined_evidence,
                    'match_hint': best_client['match_hint']
                })
        else:
            print(f"      No client matches found")
    
    print(f"\n📊 GIT ANALYSIS RESULTS:")
    print(f"   UUIDs with evidence: {len(uuid_findings)}")
    print(f"   Confirmed mappings: {len(confirmed_mappings)}")
    
    if confirmed_mappings:
        print(f"\n🎯 HIGH-CONFIDENCE GIT MAPPINGS:")
        for mapping in sorted(confirmed_mappings, key=lambda x: x['confidence'], reverse=True):
            print(f"   {mapping['uuid']} → {mapping['client_name']} (Row {mapping['client_row_id']}) - {mapping['confidence']:.1%}")
    
    return confirmed_mappings

def main():
    """Main comprehensive git analysis function"""
    tracker = get_tracker()
    
    print("🚀 COMPREHENSIVE GIT HISTORY ANALYSIS")
    print("=" * 60)
    
    # Get remaining orphaned files
    orphaned_files = get_remaining_orphaned_files()
    print(f"📊 Analyzing {len(orphaned_files)} remaining orphaned files")
    
    git_findings = {}
    
    # Step 1: Analyze git commit history
    git_findings['commit_mappings'] = analyze_git_commit_history()
    
    # Step 2: Search git logs for specific UUIDs
    git_findings['uuid_mappings'] = search_git_logs_for_orphaned_uuids(orphaned_files)
    
    # Step 3: Analyze git branches
    git_findings['branch_analysis'] = analyze_git_branches_for_context()
    
    # Step 4: Search git reflog
    git_findings['reflog_entries'] = search_git_reflog()
    
    # Step 5: Analyze logs directory
    git_findings['log_mappings'] = analyze_file_history_in_logs_directory()
    
    # Step 6: Correlate all findings
    confirmed_mappings = correlate_git_findings_with_orphaned_files(git_findings, orphaned_files)
    
    # Save results
    tracker.session_data['git_analysis'] = {
        'orphaned_files_analyzed': len(orphaned_files),
        'git_findings': git_findings,
        'confirmed_mappings': confirmed_mappings,
        'total_mappings_found': len(confirmed_mappings)
    }
    
    return confirmed_mappings

if __name__ == "__main__":
    mappings = main()
    
    if mappings:
        print(f"\n✅ SUCCESS: Found {len(mappings)} git-based mappings ready for execution!")
    else:
        print(f"\n📊 Git analysis complete - no high-confidence mappings found")