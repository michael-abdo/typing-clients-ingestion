#!/usr/bin/env python3
"""Targeted git mining for specific migration and upload patterns"""

import boto3
import pandas as pd
import json
import subprocess
import re
from datetime import datetime
from orphaned_file_recovery_tracker import get_tracker

def deep_analyze_migration_commits():
    """Deep analysis of migration-related commits around July 13, 2025"""
    print("🔍 DEEP ANALYSIS OF MIGRATION COMMITS")
    print("=" * 50)
    
    try:
        # Focus on July 13, 2025 when orphaned files were uploaded
        migration_commands = [
            ['git', 'log', '--all', '--since=2025-07-12', '--until=2025-07-14', '--pretty=format:%H|%ai|%s|%an|%ae', '--name-only'],
            ['git', 'log', '--all', '--grep=s3', '--grep=upload', '--grep=migration', '-i', '--pretty=format:%H|%ai|%s|%an|%ae', '--name-only'],
            ['git', 'log', '--all', '--grep=uuid', '--grep=file', '-i', '--since=2025-07-01', '--pretty=format:%H|%ai|%s|%an|%ae', '--name-only']
        ]
        
        all_commits = []
        
        for cmd in migration_commands:
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')
                
                if result.returncode == 0 and result.stdout.strip():
                    lines = result.stdout.split('\n')
                    print(f"   📍 Command found {len(lines)} lines")
                    
                    current_commit = None
                    for line in lines:
                        if '|' in line and len(line.split('|')) >= 4:
                            # New commit header
                            parts = line.split('|')
                            current_commit = {
                                'hash': parts[0],
                                'date': parts[1],
                                'message': parts[2],
                                'author': parts[3],
                                'email': parts[4] if len(parts) > 4 else '',
                                'files': []
                            }
                            all_commits.append(current_commit)
                        elif line.strip() and current_commit:
                            # File in this commit
                            current_commit['files'].append(line.strip())
            
            except Exception as e:
                print(f"   ⚠️  Error with command: {str(e)}")
                continue
        
        # Remove duplicates by hash
        unique_commits = {}
        for commit in all_commits:
            unique_commits[commit['hash']] = commit
        
        commits = list(unique_commits.values())
        print(f"   Found {len(commits)} unique migration-related commits")
        
        # Analyze each commit for client patterns
        client_patterns = []
        
        for commit in commits:
            print(f"\n   📍 Commit {commit['hash'][:8]} ({commit['date'][:10]})")
            print(f"      Author: {commit['author']} <{commit['email']}>")
            print(f"      Message: {commit['message'][:80]}...")
            print(f"      Files: {len(commit['files'])} files changed")
            
            # Look for client indicators in files
            client_files = []
            for file_path in commit['files']:
                if any(indicator in file_path.lower() for indicator in ['client', 'user', 'name', '.csv', '.json']):
                    client_files.append(file_path)
            
            if client_files:
                print(f"      Client-related files: {client_files}")
                
                # Try to get the actual diff to see content
                try:
                    diff_cmd = ['git', 'show', '--name-only', commit['hash']]
                    diff_result = subprocess.run(diff_cmd, capture_output=True, text=True, cwd='.')
                    
                    if diff_result.returncode == 0:
                        # Look for UUID patterns in the diff
                        uuid_pattern = re.compile(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}')
                        uuids_in_diff = uuid_pattern.findall(diff_result.stdout)
                        
                        if uuids_in_diff:
                            print(f"      UUIDs in diff: {len(uuids_in_diff)} found")
                            
                            client_patterns.append({
                                'commit': commit,
                                'uuids': uuids_in_diff,
                                'client_files': client_files,
                                'confidence': 0.7
                            })
                
                except Exception as e:
                    continue
        
        return client_patterns
        
    except Exception as e:
        print(f"   ❌ Error analyzing migration commits: {str(e)}")
        return []

def analyze_s3_upload_scripts():
    """Analyze S3 upload scripts and their git history"""
    print(f"\n📤 ANALYZING S3 UPLOAD SCRIPTS")
    print("=" * 40)
    
    try:
        # Find S3-related files
        s3_files = []
        
        # Search for S3-related files in current directory
        find_cmd = ['find', '.', '-name', '*s3*', '-o', '-name', '*upload*', '-type', 'f']
        result = subprocess.run(find_cmd, capture_output=True, text=True, cwd='.')
        
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if line.strip() and not line.startswith('./.git'):
                    s3_files.append(line.strip())
        
        print(f"   Found {len(s3_files)} S3-related files")
        
        # Analyze git history of these files
        file_histories = []
        
        for s3_file in s3_files[:10]:  # Limit to prevent timeout
            try:
                # Get file history
                log_cmd = ['git', 'log', '--follow', '--pretty=format:%H|%ai|%s|%an', s3_file]
                result = subprocess.run(log_cmd, capture_output=True, text=True, cwd='.')
                
                if result.returncode == 0 and result.stdout.strip():
                    commits = []
                    for line in result.stdout.split('\n'):
                        if '|' in line:
                            parts = line.split('|')
                            if len(parts) >= 4:
                                commits.append({
                                    'hash': parts[0],
                                    'date': parts[1],
                                    'message': parts[2],
                                    'author': parts[3]
                                })
                    
                    if commits:
                        print(f"   📍 {s3_file}: {len(commits)} commits")
                        file_histories.append({
                            'file': s3_file,
                            'commits': commits
                        })
                        
                        # Check recent content for UUID patterns
                        try:
                            with open(s3_file, 'r', encoding='utf-8', errors='ignore') as f:
                                content = f.read()
                            
                            uuid_pattern = re.compile(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}')
                            uuids = uuid_pattern.findall(content)
                            
                            if uuids:
                                print(f"      Contains {len(uuids)} UUIDs")
                        
                        except Exception as e:
                            continue
            
            except Exception as e:
                continue
        
        return file_histories
        
    except Exception as e:
        print(f"   ❌ Error analyzing S3 scripts: {str(e)}")
        return []

def search_for_batch_upload_evidence():
    """Search for evidence of batch uploads around July 13, 2025"""
    print(f"\n📦 SEARCHING FOR BATCH UPLOAD EVIDENCE")
    print("=" * 50)
    
    try:
        # Search for files created or modified around July 13
        batch_evidence = []
        
        # Check if any files in git were modified on July 13
        date_cmd = ['git', 'log', '--all', '--since=2025-07-13 00:00', '--until=2025-07-13 23:59', '--pretty=format:%H|%ai|%s|%an', '--name-only']
        result = subprocess.run(date_cmd, capture_output=True, text=True, cwd='.')
        
        if result.returncode == 0 and result.stdout.strip():
            print(f"   Found activity on July 13, 2025")
            
            current_commit = None
            for line in result.stdout.split('\n'):
                if '|' in line and len(line.split('|')) >= 4:
                    # New commit
                    parts = line.split('|')
                    current_commit = {
                        'hash': parts[0],
                        'date': parts[1],
                        'message': parts[2],
                        'author': parts[3],
                        'files': []
                    }
                    batch_evidence.append(current_commit)
                elif line.strip() and current_commit:
                    current_commit['files'].append(line.strip())
            
            print(f"   July 13 commits: {len(batch_evidence)}")
            
            for commit in batch_evidence:
                print(f"   📍 {commit['hash'][:8]}: {commit['message'][:60]}...")
                print(f"      Files: {len(commit['files'])} changed")
                
                # Look for CSV or data files that might contain client mappings
                data_files = [f for f in commit['files'] if any(ext in f.lower() for ext in ['.csv', '.json', '.txt', '.log'])]
                if data_files:
                    print(f"      Data files: {data_files}")
        
        # Also check for any reports or logs from that date
        log_patterns = [
            'logs/runs/2025-07-13*',
            '*2025-07-13*',
            'reports/*2025-07-13*'
        ]
        
        for pattern in log_patterns:
            try:
                find_cmd = ['find', '.', '-path', pattern, '-type', 'f']
                result = subprocess.run(find_cmd, capture_output=True, text=True, cwd='.')
                
                if result.returncode == 0:
                    files = [f.strip() for f in result.stdout.split('\n') if f.strip()]
                    if files:
                        print(f"   Found files matching {pattern}: {len(files)}")
                        for f in files[:5]:  # Show first 5
                            print(f"      - {f}")
            
            except Exception as e:
                continue
        
        return batch_evidence
        
    except Exception as e:
        print(f"   ❌ Error searching batch evidence: {str(e)}")
        return []

def analyze_author_patterns():
    """Analyze git authors who might correlate with client names"""
    print(f"\n👤 ANALYZING GIT AUTHOR PATTERNS")
    print("=" * 40)
    
    try:
        # Get all authors from git history
        authors_cmd = ['git', 'log', '--all', '--pretty=format:%an|%ae', '--since=2025-07-01']
        result = subprocess.run(authors_cmd, capture_output=True, text=True, cwd='.')
        
        authors = {}
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if '|' in line:
                    name, email = line.split('|', 1)
                    if name not in authors:
                        authors[name] = set()
                    authors[name].add(email)
        
        print(f"   Found {len(authors)} unique authors")
        
        # Load client data for comparison
        df = pd.read_csv('outputs/output.csv')
        
        author_client_matches = []
        
        for author_name, emails in authors.items():
            author_lower = author_name.lower()
            
            # Try to match with client names
            for _, client_row in df.iterrows():
                client_name = str(client_row['name']).lower()
                client_email = str(client_row.get('email', '')).lower()
                
                # Name matching
                client_parts = client_name.split()
                author_parts = author_lower.split()
                
                name_matches = 0
                for client_part in client_parts:
                    if len(client_part) > 2:
                        for author_part in author_parts:
                            if client_part in author_part or author_part in client_part:
                                name_matches += 1
                                break
                
                # Email matching
                email_match = False
                for email in emails:
                    if client_email and (client_email in email.lower() or email.lower() in client_email):
                        email_match = True
                        break
                
                # Score the match
                confidence = 0
                if name_matches >= 2:
                    confidence = 0.8
                elif name_matches == 1:
                    confidence = 0.5
                
                if email_match:
                    confidence += 0.3
                
                if confidence >= 0.5:
                    print(f"   📍 {author_name} → {client_row['name']} (Row {client_row['row_id']}) - {confidence:.1%}")
                    
                    author_client_matches.append({
                        'author_name': author_name,
                        'author_emails': list(emails),
                        'client_row_id': client_row['row_id'],
                        'client_name': client_row['name'],
                        'confidence': confidence,
                        'name_matches': name_matches,
                        'email_match': email_match
                    })
        
        # Now find commits by these authors that might contain UUIDs
        uuid_commits = []
        
        for match in author_client_matches:
            author_name = match['author_name']
            
            # Get commits by this author
            author_cmd = ['git', 'log', '--all', '--author=' + author_name, '--pretty=format:%H|%ai|%s', '--since=2025-07-01']
            result = subprocess.run(author_cmd, capture_output=True, text=True, cwd='.')
            
            if result.returncode == 0:
                commits = []
                for line in result.stdout.split('\n'):
                    if '|' in line:
                        parts = line.split('|')
                        if len(parts) >= 3:
                            commits.append({
                                'hash': parts[0],
                                'date': parts[1],
                                'message': parts[2]
                            })
                
                if commits:
                    print(f"   📍 {author_name}: {len(commits)} commits")
                    
                    # Check these commits for UUIDs
                    for commit in commits[:5]:  # Limit to recent commits
                        try:
                            show_cmd = ['git', 'show', commit['hash']]
                            show_result = subprocess.run(show_cmd, capture_output=True, text=True, cwd='.')
                            
                            if show_result.returncode == 0:
                                uuid_pattern = re.compile(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}')
                                uuids = uuid_pattern.findall(show_result.stdout)
                                
                                if uuids:
                                    uuid_commits.append({
                                        'author_match': match,
                                        'commit': commit,
                                        'uuids': uuids
                                    })
                        
                        except Exception as e:
                            continue
        
        print(f"\n   Author-UUID commits found: {len(uuid_commits)}")
        return author_client_matches, uuid_commits
        
    except Exception as e:
        print(f"   ❌ Error analyzing authors: {str(e)}")
        return [], []

def main():
    """Main targeted git mining function"""
    tracker = get_tracker()
    
    print("🎯 TARGETED GIT MINING FOR ORPHANED FILE RECOVERY")
    print("=" * 70)
    
    results = {}
    
    # Step 1: Deep migration commit analysis
    results['migration_patterns'] = deep_analyze_migration_commits()
    
    # Step 2: S3 upload script analysis
    results['s3_script_history'] = analyze_s3_upload_scripts()
    
    # Step 3: Batch upload evidence
    results['batch_evidence'] = search_for_batch_upload_evidence()
    
    # Step 4: Author pattern analysis
    author_matches, uuid_commits = analyze_author_patterns()
    results['author_matches'] = author_matches
    results['uuid_commits'] = uuid_commits
    
    # Consolidate findings
    total_evidence = 0
    if results['migration_patterns']:
        total_evidence += len(results['migration_patterns'])
    if results['uuid_commits']:
        total_evidence += len(results['uuid_commits'])
    
    print(f"\n📊 TARGETED GIT MINING RESULTS:")
    print(f"   Migration patterns found: {len(results.get('migration_patterns', []))}")
    print(f"   S3 script histories: {len(results.get('s3_script_history', []))}")
    print(f"   Batch evidence pieces: {len(results.get('batch_evidence', []))}")
    print(f"   Author-client matches: {len(results.get('author_matches', []))}")
    print(f"   UUID-containing commits: {len(results.get('uuid_commits', []))}")
    print(f"   Total evidence pieces: {total_evidence}")
    
    # Save results
    tracker.session_data['targeted_git_mining'] = results
    
    return results

if __name__ == "__main__":
    results = main()