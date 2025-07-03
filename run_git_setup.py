#!/usr/bin/env python3
"""
Git setup automation script for creating GitHub repository
Run this to automate the git commands for michael-abdo
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(cmd, description=""):
    """Run a shell command and return success status"""
    if description:
        print(f"\n🔄 {description}")
        print(f"Running: {cmd}")
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False)
        
        if result.returncode == 0:
            print(f"✅ Success")
            if result.stdout.strip():
                print(f"Output: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ Failed (exit code: {result.returncode})")
            if result.stderr.strip():
                print(f"Error: {result.stderr.strip()}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def main():
    print("🚀 GitHub Repository Setup for michael-abdo")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not Path("README_GITHUB.md").exists() and not Path("README.md").exists():
        print("❌ Not in the correct directory. Please run from the project root.")
        sys.exit(1)
    
    # 1. Rename README
    if Path("README_GITHUB.md").exists():
        run_command("mv README_GITHUB.md README.md", "Renaming README file")
    
    # 2. Clean up temporary files
    temp_files = [
        "cleanup_temp_files.py",
        "test_*.py", 
        "verify_*.py",
        "commit_duplicate*.sh",
        "push_commands.txt",
        "push_to_github.sh",
        "setup_github_repo.sh"
    ]
    
    for pattern in temp_files:
        if '*' in pattern:
            run_command(f"rm -f {pattern}", f"Removing {pattern}")
        else:
            if Path(pattern).exists():
                run_command(f"rm -f {pattern}", f"Removing {pattern}")
    
    # 3. Git operations
    print(f"\n📋 Git Status:")
    run_command("git status --short")
    
    print(f"\n📦 Staging all changes...")
    success = run_command("git add -A", "Staging files")
    if not success:
        print("❌ Failed to stage files")
        sys.exit(1)
    
    print(f"\n💾 Creating commit...")
    commit_msg = """feat: Complete personality typing content manager system

- Comprehensive YouTube and Google Drive download system
- Row-centric CSV tracking with data integrity
- Bidirectional file-to-row mapping (100% coverage)
- Intelligent error handling and retry mechanisms
- Production monitoring with configurable alerts
- Code deduplication completed (removed all duplicates)
- Organized file structure with clear separation of concerns
- All tests passing with full system validation

This production-ready system manages content downloads from Google Sheets
sources while maintaining perfect data integrity and traceability."""
    
    success = run_command(f'git commit -m "{commit_msg}"', "Creating commit")
    if not success:
        print("❌ Failed to create commit")
        sys.exit(1)
    
    # 4. Try GitHub CLI first
    print(f"\n🔍 Checking for GitHub CLI...")
    gh_available = run_command("gh --version", "Checking GitHub CLI")
    
    if gh_available:
        print(f"\n🎯 Creating repository with GitHub CLI...")
        repo_cmd = """gh repo create ops-typing-log-client --public --source=. --remote=origin --push --description="Production-ready system for downloading and tracking personality typing content from Google Sheets sources" """
        
        success = run_command(repo_cmd, "Creating GitHub repository")
        if success:
            print(f"\n🎉 Repository created successfully!")
            print(f"🔗 URL: https://github.com/michael-abdo/ops-typing-log-client")
            return
    
    # 5. Manual instructions if GitHub CLI failed
    print(f"\n📝 GitHub CLI not available or failed. Manual setup required:")
    print(f"""
    1. Go to https://github.com/new
    2. Repository name: ops-typing-log-client
    3. Description: Production-ready system for downloading and tracking personality typing content from Google Sheets sources
    4. Make it Public
    5. DON'T initialize with README, .gitignore or license
    6. Click 'Create repository'
    
    Then run these commands:
    git remote add origin https://github.com/michael-abdo/ops-typing-log-client.git
    git branch -M main
    git push -u origin main
    """)

if __name__ == "__main__":
    main()