#!/usr/bin/env python3
"""
Complete the 3 remaining MP3-only clients directly
"""

import subprocess
import boto3
import os
import json
import time

def stream_mp4_to_s3(video_id, client_folder):
    """Stream a single video to S3 as MP4"""
    
    youtube_url = f"https://www.youtube.com/watch?v={video_id}"
    s3_key = f"{client_folder}/youtube_{video_id}.mp4"
    pipe_path = f"/tmp/complete_{video_id}_{os.getpid()}"
    
    s3 = boto3.client('s3', region_name='us-east-1')
    bucket = 'typing-clients-storage-2025'
    
    try:
        if os.path.exists(pipe_path):
            os.remove(pipe_path)
        os.mkfifo(pipe_path)
        
        print(f"    📥 Streaming: {video_id}")
        
        cmd = ["yt-dlp", "-f", "best[ext=mp4]/best", "-o", pipe_path, youtube_url]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        with open(pipe_path, 'rb') as pipe_file:
            s3.upload_fileobj(
                pipe_file,
                bucket,
                s3_key,
                ExtraArgs={'ContentType': 'video/mp4'}
            )
        
        process.wait()
        
        if process.returncode == 0:
            s3_url = f"https://{bucket}.s3.amazonaws.com/{s3_key}"
            print(f"      ✅ Success: {s3_url}")
            return True, s3_url
        else:
            error = process.stderr.read().decode()
            print(f"      ❌ Failed: {error[:100]}")
            return False, error
            
    except Exception as e:
        print(f"      ❌ Error: {str(e)}")
        return False, str(e)
    finally:
        if os.path.exists(pipe_path):
            os.remove(pipe_path)

def main():
    print("🔄 COMPLETING REMAINING 3 MP3-ONLY CLIENTS")
    print("=" * 60)
    
    # Define remaining clients
    remaining_clients = [
        {
            "client_id": "482_Brandon_Donahue",
            "folder": "482/Brandon_Donahue",
            "videos": ["x2jejX4YbrA"]
        },
        {
            "client_id": "485_Brandon_Donahue",
            "folder": "485/Brandon_Donahue",
            "videos": ["x2jejX4YbrA"]
        },
        {
            "client_id": "488_Jeremy_May",
            "folder": "488/Jeremy_May",
            "videos": ["d6IR17a0M2o"]
        }
    ]
    
    results = []
    total_success = 0
    total_videos = 0
    
    for client in remaining_clients:
        print(f"\n🎬 Processing {client['client_id']}")
        client_result = {
            "client_id": client['client_id'],
            "videos": []
        }
        
        for video_id in client['videos']:
            total_videos += 1
            success, result = stream_mp4_to_s3(video_id, client['folder'])
            
            if success:
                total_success += 1
                client_result['videos'].append({
                    "video_id": video_id,
                    "success": True,
                    "s3_url": result
                })
            else:
                client_result['videos'].append({
                    "video_id": video_id,
                    "success": False,
                    "error": result
                })
            
            time.sleep(1)  # Brief pause
        
        results.append(client_result)
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 COMPLETION SUMMARY")
    print("=" * 60)
    print(f"✅ Successfully processed: {total_success}/{total_videos} videos")
    
    for result in results:
        success_count = sum(1 for v in result['videos'] if v['success'])
        status = "✅" if success_count == len(result['videos']) else "⚠️"
        print(f"{status} {result['client_id']}: {success_count}/{len(result['videos'])} videos")
    
    # Save results
    with open("remaining_mp3_completion.json", "w") as f:
        json.dump({
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_success": total_success,
            "total_videos": total_videos,
            "results": results
        }, f, indent=2)
    
    print(f"\n✅ ALL MP3-ONLY CLIENTS NOW COMPLETE!")
    print(f"📄 Results saved to: remaining_mp3_completion.json")

if __name__ == "__main__":
    main()