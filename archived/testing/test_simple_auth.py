#!/usr/bin/env python3
import psycopg2

try:
    conn = psycopg2.connect(
        host='127.0.0.1',
        port=5432,
        database='typing_clients_uuid',
        user='migration_user', 
        password='simple123'
    )
    print("✅ Database connection successful!")
    
    with conn.cursor() as cur:
        cur.execute("SELECT current_user, current_database();")
        result = cur.fetchone()
        print(f"Connected as: {result[0]} to database: {result[1]}")
    
    conn.close()
    
except Exception as e:
    print(f"❌ Database connection failed: {e}")