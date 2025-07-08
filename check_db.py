#!/usr/bin/env python3
"""Simple database check script"""
import sqlite3
import os

print("Current directory:", os.getcwd())
print("Files in directory:", os.listdir('.'))

# Check minimal directory 
minimal_dir = "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal"
print(f"Files in minimal dir: {os.listdir(minimal_dir)}")

# Check the database
db_path = os.path.join(minimal_dir, "xenodx.db")
if os.path.exists(db_path):
    print(f"Database exists at: {db_path}")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print("Tables:", [table[0] for table in tables])
        
        # Try to get rows 472-502
        try:
            cursor.execute("SELECT row_id, name, email, type FROM sheet_data WHERE CAST(row_id AS INTEGER) BETWEEN 472 AND 502 ORDER BY CAST(row_id AS INTEGER)")
            rows = cursor.fetchall()
            print(f"Found {len(rows)} rows between 472-502:")
            for row in rows:
                print(f"  {row}")
        except Exception as e:
            print(f"Error querying sheet_data: {e}")
            
        conn.close()
    except Exception as e:
        print(f"Error accessing database: {e}")
else:
    print(f"Database not found at: {db_path}")