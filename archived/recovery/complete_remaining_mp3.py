#!/usr/bin/env python3
"""
Complete remaining MP3-only clients
"""

import json
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from batch_mp4_reprocessor import BatchMP4Reprocessor

def main():
    print("🔄 COMPLETING REMAINING MP3-ONLY CLIENTS")
    print("=" * 60)
    
    # Already completed clients
    completed = {'487_Olivia_Tomlinson', '502_Sam_Torode', '494_John_Williams', '493_Maddie_Boyle'}
    
    # Initialize processor
    processor = BatchMP4Reprocessor()
    
    # Filter to remaining MP3-only clients
    remaining = [c for c in processor.candidates 
                 if c['reprocess_type'] == 'full' and c['client_id'] not in completed]
    
    print(f"📊 Found {len(remaining)} remaining clients with {sum(c['video_count'] for c in remaining)} videos")
    for client in remaining:
        print(f"  • {client['client_id']}: {client['video_count']} videos")
    
    if not remaining:
        print("✅ All MP3-only clients already completed!")
        return True
    
    print("\n🚀 Starting completion...")
    
    # Process remaining clients
    processor.progress['total_clients'] = len(remaining)
    processor.progress['total_videos'] = sum(c['video_count'] for c in remaining)
    
    results = []
    for client in remaining:
        result = processor.process_client(client)
        results.append(result)
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 COMPLETION SUMMARY")
    print("=" * 60)
    
    total_success = sum(r['successful_videos'] for r in results)
    total_videos = sum(r['total_videos'] for r in results)
    
    print(f"✅ Processed: {total_success}/{total_videos} videos")
    
    for result in results:
        status = "✅" if result['failed_videos'] == 0 else "⚠️"
        print(f"{status} {result['client_id']}: {result['successful_videos']}/{result['total_videos']} videos")
    
    # Save completion report
    completion_data = {
        "completion_run": True,
        "remaining_clients": len(remaining),
        "results": results,
        "summary": {
            "total_videos": total_videos,
            "successful_videos": total_success,
            "failed_videos": total_videos - total_success
        }
    }
    
    with open("mp3_completion_results.json", "w") as f:
        json.dump(completion_data, f, indent=2)
    
    print(f"\n📄 Results saved to: mp3_completion_results.json")
    
    return total_success == total_videos

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)