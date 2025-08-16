#!/usr/bin/env python3
"""
Quick test to verify auto-update functionality works in the workflow
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.s3_manager import UnifiedS3Manager, S3Config, UploadMode
from utils.config import get_config

def test_auto_update():
    """Test that auto-update is working"""
    print("🧪 Testing yt-dlp auto-update in S3 manager...")
    
    # Get config
    config = get_config()
    
    # Create S3 manager
    s3_config = S3Config(
        bucket_name=config.get("downloads.s3.default_bucket", "typing-clients-uuid-system"),
        upload_mode=UploadMode.DIRECT_STREAMING,
        organize_by_person=False,
        add_metadata=True
    )
    s3_manager = UnifiedS3Manager(s3_config)
    
    # Test a simple YouTube URL (this is a very short video for testing)
    test_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"  # Rick Roll - short video
    test_s3_key = "test/auto_update_test.mp4"
    
    print(f"📹 Testing with URL: {test_url}")
    print(f"🪣 S3 key: {test_s3_key}")
    
    try:
        # This should trigger the auto-update
        result = s3_manager.stream_youtube_to_s3(test_url, test_s3_key, "TestUser")
        
        if result.success:
            print("✅ Auto-update test successful!")
            print(f"   S3 URL: {result.s3_url}")
            print(f"   Upload time: {result.upload_time}")
        else:
            print(f"❌ Test failed: {result.error}")
            
    except Exception as e:
        print(f"💥 Exception during test: {e}")
    
    print("\n🎯 Key points about the auto-update:")
    print("1. yt-dlp will be automatically updated before each download")
    print("2. You can disable this by setting downloads.youtube.auto_update_yt_dlp: false in config.yaml")
    print("3. You can also use --no-yt-dlp-update flag when running simple_workflow.py")

if __name__ == "__main__":
    test_auto_update()