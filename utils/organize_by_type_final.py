#!/usr/bin/env python3
"""
Final organization script - organizes all files by personality type
using the complete 100% mapping achieved.
"""

import os
import shutil
import pandas as pd
from pathlib import Path
from collections import defaultdict
import argparse


def organize_files_by_type(mapping_csv: str = 'complete_file_mapping_100_percent.csv',
                          output_dir: str = 'organized_by_type',
                          copy_files: bool = True) -> None:
    """Organize all files by personality type using complete mapping"""
    
    # Load complete mapping
    df = pd.read_csv(mapping_csv)
    
    # Filter out system files
    df_persons = df[df['mapping_status'] != 'system_file']
    
    print(f"=== ORGANIZING {len(df_persons)} FILES BY PERSONALITY TYPE ===")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Track statistics
    type_counts = defaultdict(int)
    person_counts = defaultdict(int)
    organized_count = 0
    
    # Process each file
    for idx, row in df_persons.iterrows():
        try:
            # Skip if file doesn't exist
            if not os.path.exists(row['file_path']):
                print(f"  ⚠️  File not found: {row['file_path']}")
                continue
            
            # Clean personality type for directory name
            personality_type = str(row['personality_type'])
            if personality_type in ['see_action', 'nan']:
                # Try to extract from fix_action if available
                if 'fix_action' in row and pd.notna(row['fix_action']):
                    import re
                    match = re.search(r'\((.*?)\)', str(row['fix_action']))
                    if match:
                        personality_type = match.group(1)
                    else:
                        personality_type = 'Unknown_Type'
                else:
                    personality_type = 'Unknown_Type'
            
            # Clean type for filesystem
            type_clean = personality_type.replace('/', '-').replace(' ', '_').replace('#', 'num')
            
            # Clean person name
            person_name = str(row['person_name'])
            if person_name in ['see_action', 'nan']:
                person_name = 'Unknown_Person'
            person_clean = person_name.replace(' ', '_').replace('/', '-')
            
            # Create directory structure
            type_dir = os.path.join(output_dir, type_clean)
            person_dir = os.path.join(type_dir, f"{row['row_id']}_{person_clean}")
            
            os.makedirs(person_dir, exist_ok=True)
            
            # Determine file category
            filename = row['filename']
            if filename.endswith('.mp4') or filename.endswith('.mov') or filename.endswith('.avi'):
                subdir = 'videos'
            elif filename.endswith('.vtt') or filename.endswith('.srt'):
                subdir = 'transcripts'
            elif filename.endswith('.json'):
                subdir = 'metadata'
            elif filename.endswith('.pdf') or filename.endswith('.doc') or filename.endswith('.docx'):
                subdir = 'documents'
            else:
                subdir = 'other'
            
            # Create category subdirectory
            dest_dir = os.path.join(person_dir, subdir)
            os.makedirs(dest_dir, exist_ok=True)
            
            # Copy or move file
            dest_path = os.path.join(dest_dir, filename)
            
            if not os.path.exists(dest_path):
                if copy_files:
                    shutil.copy2(row['file_path'], dest_path)
                else:
                    shutil.move(row['file_path'], dest_path)
                
                organized_count += 1
                type_counts[type_clean] += 1
                person_counts[person_clean] += 1
                
                if organized_count % 50 == 0:
                    print(f"  Organized {organized_count} files...")
                    
        except Exception as e:
            print(f"  ❌ Error organizing {row['filename']}: {e}")
    
    # Generate organization report
    print(f"\n=== ORGANIZATION COMPLETE ===")
    print(f"Total files organized: {organized_count}")
    print(f"Personality types: {len(type_counts)}")
    print(f"Unique persons: {len(person_counts)}")
    
    # Show top personality types
    print(f"\nTop 10 personality types by file count:")
    for ptype, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {ptype}: {count} files")
    
    # Create index file
    create_index_file(output_dir, type_counts, organized_count)
    
    print(f"\n✅ All files organized in: {output_dir}/")
    print(f"   Index created at: {output_dir}/INDEX.md")


def create_index_file(output_dir: str, type_counts: dict, total_files: int) -> None:
    """Create an index file for easy navigation"""
    
    index_path = os.path.join(output_dir, 'INDEX.md')
    
    with open(index_path, 'w') as f:
        f.write("# Personality Type Organized Files\n\n")
        f.write(f"Total files: {total_files}\n")
        f.write(f"Total personality types: {len(type_counts)}\n\n")
        
        f.write("## Directory Structure\n\n")
        f.write("```\n")
        f.write("organized_by_type/\n")
        f.write("├── [Personality_Type]/\n")
        f.write("│   ├── [RowID_PersonName]/\n")
        f.write("│   │   ├── videos/\n")
        f.write("│   │   ├── transcripts/\n")
        f.write("│   │   ├── documents/\n")
        f.write("│   │   ├── metadata/\n")
        f.write("│   │   └── other/\n")
        f.write("```\n\n")
        
        f.write("## Personality Types\n\n")
        for ptype, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
            f.write(f"- **{ptype}**: {count} files\n")
        
        f.write("\n## Navigation\n\n")
        f.write("Each personality type folder contains individual person folders.\n")
        f.write("Within each person folder, files are organized by type:\n")
        f.write("- `videos/` - Video files (.mp4, .mov, .avi)\n")
        f.write("- `transcripts/` - Transcript files (.vtt, .srt)\n")
        f.write("- `documents/` - Document files (.pdf, .doc, .docx)\n")
        f.write("- `metadata/` - Metadata files (.json)\n")
        f.write("- `other/` - Other file types\n")


def main():
    parser = argparse.ArgumentParser(description='Organize files by personality type')
    parser.add_argument('--mapping', default='complete_file_mapping_100_percent.csv',
                       help='Path to complete mapping CSV')
    parser.add_argument('--output', default='organized_by_type',
                       help='Output directory for organized files')
    parser.add_argument('--move', action='store_true',
                       help='Move files instead of copying')
    
    args = parser.parse_args()
    
    organize_files_by_type(
        mapping_csv=args.mapping,
        output_dir=args.output,
        copy_files=not args.move
    )


if __name__ == "__main__":
    main()