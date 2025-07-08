#!/usr/bin/env python3
"""
Download actual Google Drive files from info files.
"""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Any


def find_drive_info_files() -> List[str]:
    """Find all Drive info JSON files in downloads directory."""
    downloads_dir = Path("downloads")
    info_files = []
    
    for info_file in downloads_dir.rglob("*drive*info.json"):
        info_files.append(str(info_file))
    
    return sorted(info_files)


def load_drive_info(info_file: str) -> Dict[str, Any]:
    """Load Drive info from JSON file."""
    with open(info_file, 'r') as f:
        return json.load(f)


def get_output_file_path(info_file: str, drive_info: Dict[str, Any]) -> str:
    """Get output file path for downloaded Drive file."""
    info_path = Path(info_file)
    output_dir = info_path.parent
    
    # Remove _info.json from filename
    base_name = info_path.stem.replace('_info', '')
    output_file = output_dir / base_name
    
    return str(output_file)


def is_file_downloaded(output_file: str) -> bool:
    """Check if file is already downloaded and complete."""
    if not os.path.exists(output_file):
        return False
    
    # Check if file is not empty
    file_size = os.path.getsize(output_file)
    return file_size > 0


def download_drive_file(drive_info: Dict[str, Any], output_file: str) -> bool:
    """Download a Google Drive file using gdown."""
    drive_id = drive_info.get("id") or drive_info.get("file_id")
    person = drive_info.get("person", "Unknown")
    
    if not drive_id:
        print(f"❌ No drive ID found for {person}")
        return False
    
    # Skip folders for now
    if drive_info.get("type") == "drive_folder":
        print(f"⏭️  Skipping folder for {person} (folders not supported yet)")
        return True
    
    print(f"📥 Downloading Drive file for {person}...")
    print(f"   ID: {drive_id}")
    print(f"   Output: {output_file}")
    
    try:
        # Use gdown to download the file
        cmd = [
            "gdown",
            f"https://drive.google.com/uc?id={drive_id}",
            "-O", output_file,
            "--fuzzy"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)  # 30 minute timeout
        
        if result.returncode == 0:
            print(f"✅ Successfully downloaded Drive file for {person}")
            return True
        else:
            print(f"❌ Failed to download Drive file for {person}")
            print(f"   Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ Download timeout for {person} (30 minutes)")
        return False
    except Exception as e:
        print(f"❌ Error downloading Drive file for {person}: {str(e)}")
        return False


def main():
    """Main function to download all Drive files."""
    print("🔍 Finding Drive info files...")
    
    info_files = find_drive_info_files()
    
    if not info_files:
        print("❌ No Drive info files found")
        return
    
    print(f"📋 Found {len(info_files)} Drive info files")
    
    downloaded_count = 0
    failed_count = 0
    skipped_count = 0
    
    for info_file in info_files:
        print(f"\n{'='*50}")
        print(f"Processing: {info_file}")
        
        try:
            drive_info = load_drive_info(info_file)
            output_file = get_output_file_path(info_file, drive_info)
            
            # Check if already downloaded
            if is_file_downloaded(output_file):
                print(f"✅ Already downloaded: {drive_info.get('person', 'Unknown')}")
                skipped_count += 1
                continue
            
            # Download the file
            if download_drive_file(drive_info, output_file):
                downloaded_count += 1
            else:
                failed_count += 1
                
        except Exception as e:
            print(f"❌ Error processing {info_file}: {str(e)}")
            failed_count += 1
    
    print(f"\n{'='*50}")
    print("📊 SUMMARY")
    print(f"✅ Downloaded: {downloaded_count}")
    print(f"⏭️  Skipped (already downloaded): {skipped_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"📋 Total processed: {len(info_files)}")


if __name__ == "__main__":
    main()