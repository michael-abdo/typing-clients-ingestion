#!/usr/bin/env python3
import csv
import os
import re

def is_drive_link(url):
    """Check if a URL is a Google Drive link"""
    return ('drive.google.com/file/d/' in url or 
            'drive.google.com/open?id=' in url or
            'drive.google.com/drive/folders/' in url)

input_file = '/Users/Mike/ops_typing_log/ongoing_clients/output.csv'
temp_file = '/Users/Mike/ops_typing_log/ongoing_clients/output_temp.csv'

drive_links_found = 0

with open(input_file, 'r', newline='', encoding='utf-8') as csvfile, \
     open(temp_file, 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(csvfile)
    fieldnames = reader.fieldnames
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in reader:
        # Check extracted_links for Google Drive links
        links = row['extracted_links'].split('|') if row['extracted_links'] else []
        
        # Also check the main link column
        if row['link'] and is_drive_link(row['link']):
            links.append(row['link'])
        
        # Extract Google Drive links
        drive_links = []
        for link in links:
            if is_drive_link(link):
                drive_links.append(link)
                drive_links_found += 1
        
        # Update the google_drive column
        row['google_drive'] = '|'.join(drive_links)
        
        # Also check if there are YouTube links that should go in youtube_playlist
        if not row['youtube_playlist'] and any('youtube.com' in link or 'youtu.be' in link for link in links):
            youtube_links = [link for link in links if 'youtube.com' in link or 'youtu.be' in link]
            row['youtube_playlist'] = youtube_links[0] if youtube_links else ''
        
        writer.writerow(row)

# Replace original file with the temp file
os.replace(temp_file, input_file)
print(f'Successfully populated Google Drive links. Found {drive_links_found} drive links.')