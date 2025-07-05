#!/usr/bin/env python3
"""Download Emilie's folder with new files"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.download_drive import download_from_drive_url

# Emilie's folder URL
folder_url = "https://drive.google.com/drive/folders/1nrNku9G5dnWxGmfawSi6gLNb9Jaij_2r"

print("Downloading Emilie's folder...")
print(f"URL: {folder_url}")
print("-" * 80)

try:
    result = download_from_drive_url(folder_url)
    print(f"\nDownload completed!")
    print(f"Result: {result}")
except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()