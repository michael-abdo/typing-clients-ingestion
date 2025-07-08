#!/usr/bin/env python3
"""
Create a summary of what we can download
"""
import sqlite3
import re

def get_download_summary():
    """Get summary of all downloadable content"""
    conn = sqlite3.connect("minimal/xenodex.db")
    cursor = conn.cursor()
    
    # Get YouTube links
    youtube_query = """
    SELECT COUNT(DISTINCT el.url), COUNT(DISTINCT p.id)
    FROM people p
    JOIN documents d ON p.id = d.person_id
    JOIN extracted_links el ON d.id = el.document_id
    WHERE (el.url LIKE '%youtube.com/watch%' OR el.url LIKE '%youtu.be%')
      AND el.url NOT LIKE '%playlist%'
    """
    
    youtube_count, youtube_people = cursor.execute(youtube_query).fetchone()
    
    # Get Drive links
    drive_query = """
    SELECT COUNT(DISTINCT el.url), COUNT(DISTINCT p.id)
    FROM people p
    JOIN documents d ON p.id = d.person_id
    JOIN extracted_links el ON d.id = el.document_id
    WHERE el.url LIKE '%drive.google.com%'
    """
    
    drive_count, drive_people = cursor.execute(drive_query).fetchone()
    
    # Get sample of people with most videos
    top_people_query = """
    SELECT p.name, p.row_id, COUNT(DISTINCT el.url) as video_count
    FROM people p
    JOIN documents d ON p.id = d.person_id
    JOIN extracted_links el ON d.id = el.document_id
    WHERE (el.url LIKE '%youtube.com/watch%' OR el.url LIKE '%youtu.be%')
      AND el.url NOT LIKE '%playlist%'
    GROUP BY p.id
    ORDER BY video_count DESC
    LIMIT 10
    """
    
    top_people = cursor.execute(top_people_query).fetchall()
    
    print("=== DOWNLOAD SUMMARY ===\n")
    
    print(f"YouTube Videos:")
    print(f"  Total videos: {youtube_count}")
    print(f"  People with videos: {youtube_people}")
    print(f"  Average per person: {youtube_count/youtube_people:.1f}")
    
    print(f"\nGoogle Drive Files:")
    print(f"  Total files: {drive_count}")
    print(f"  People with files: {drive_people}")
    
    print(f"\nTop 10 People by Video Count:")
    for name, row_id, count in top_people:
        print(f"  - {name} (Row {row_id}): {count} videos")
    
    # Create download commands
    print("\n=== DOWNLOAD COMMANDS ===\n")
    
    print("To download specific people's videos:")
    for name, row_id, count in top_people[:3]:
        safe_name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        print(f"\n# Download {name}'s videos ({count} videos):")
        
        # Get their video IDs
        video_query = """
        SELECT DISTINCT el.url
        FROM people p
        JOIN documents d ON p.id = d.person_id
        JOIN extracted_links el ON d.id = el.document_id
        WHERE p.row_id = ? 
          AND (el.url LIKE '%youtube.com/watch%' OR el.url LIKE '%youtu.be%')
          AND el.url NOT LIKE '%playlist%'
        """
        
        videos = cursor.execute(video_query, (row_id,)).fetchall()
        
        for url in videos[:2]:  # Show first 2
            url = url[0]
            match = re.search(r'v=([a-zA-Z0-9_-]{11})', url)
            if match:
                video_id = match.group(1)
                print(f"yt-dlp -f best[ext=mp4] -o '{safe_name}_{video_id}.mp4' 'https://www.youtube.com/watch?v={video_id}'")
    
    conn.close()

if __name__ == "__main__":
    get_download_summary()