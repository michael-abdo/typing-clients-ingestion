#!/usr/bin/env python3
"""
Person Model - Represents a person from the CSV/Google Sheets
Maps directly to RowContext for compatibility with existing system
Following CLAUDE.md principle: Smallest Possible Feature
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from database.db_connection import get_database_engine
from sqlalchemy import text


@dataclass
class Person:
    """Person entity matching the CSV structure and RowContext"""
    row_id: str                    # Primary key from CSV
    name: str                      # Person name - CRITICAL for mapping
    email: Optional[str] = None    # Email address
    personality_type: Optional[str] = None  # Type - CRITICAL for preservation
    source_link: Optional[str] = None       # Google Doc URL
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations"""
        return {
            'row_id': self.row_id,
            'name': self.name,
            'email': self.email,
            'personality_type': self.personality_type,
            'source_link': self.source_link
        }
    
    @classmethod
    def from_row(cls, row: tuple) -> 'Person':
        """Create Person from database row"""
        return cls(
            row_id=row[0],
            name=row[1],
            email=row[2],
            personality_type=row[3],
            source_link=row[4],
            created_at=row[5] if len(row) > 5 else None,
            updated_at=row[6] if len(row) > 6 else None
        )


class PersonOperations:
    """Simple database operations for Person entities"""
    
    def __init__(self):
        self.engine = get_database_engine()
    
    def create_table(self) -> bool:
        """Create the people table if it doesn't exist"""
        try:
            with self.engine.connect() as conn:
                # Read and execute the schema
                schema_path = os.path.join(os.path.dirname(__file__), '..', 'schema', 'init.sql')
                with open(schema_path, 'r') as f:
                    schema_sql = f.read()
                
                # Execute the entire schema as one transaction
                # Remove comments and empty lines for cleaner execution
                clean_sql = '\n'.join(line for line in schema_sql.split('\n') 
                                    if line.strip() and not line.strip().startswith('--'))
                
                # Execute all statements
                conn.exec_driver_sql(clean_sql)
                conn.commit()
                
            print("✅ People table created successfully")
            return True
            
        except Exception as e:
            print(f"❌ Error creating table: {e}")
            return False
    
    def insert_person(self, person: Person) -> bool:
        """Insert a single person record"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO people (row_id, name, email, personality_type, source_link)
                    VALUES (:row_id, :name, :email, :personality_type, :source_link)
                    ON CONFLICT (row_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        email = EXCLUDED.email,
                        personality_type = EXCLUDED.personality_type,
                        source_link = EXCLUDED.source_link;
                """), person.to_dict())
                conn.commit()
            
            print(f"✅ Inserted person: {person.name} (row_id: {person.row_id})")
            return True
            
        except Exception as e:
            print(f"❌ Error inserting person: {e}")
            return False
    
    def get_person(self, row_id: str) -> Optional[Person]:
        """Retrieve a person by row_id"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT row_id, name, email, personality_type, source_link, 
                           created_at, updated_at
                    FROM people 
                    WHERE row_id = :row_id
                """), {'row_id': row_id})
                
                row = result.fetchone()
                if row:
                    return Person.from_row(row)
                return None
                
        except Exception as e:
            print(f"❌ Error retrieving person: {e}")
            return None
    
    def __del__(self):
        """Clean up database connection"""
        if hasattr(self, 'engine'):
            self.engine.dispose()


def test_atomic_feature():
    """Test the atomic feature: create table, insert one record, query it back"""
    print("🧪 Testing atomic database feature...")
    
    # Initialize operations
    ops = PersonOperations()
    
    # Step 1: Create table
    if not ops.create_table():
        print("❌ Failed to create table")
        return False
    
    # Step 2: Insert ONE test person (real data structure)
    test_person = Person(
        row_id="001",
        name="Test Person",
        email="test@example.com",
        personality_type="FF-Ne/Fi-CP/B(S) #1",
        source_link="https://docs.google.com/document/d/test"
    )
    
    if not ops.insert_person(test_person):
        print("❌ Failed to insert person")
        return False
    
    # Step 3: Query the person back
    retrieved = ops.get_person("001")
    if not retrieved:
        print("❌ Failed to retrieve person")
        return False
    
    # Verify data matches
    if (retrieved.row_id == test_person.row_id and 
        retrieved.name == test_person.name and
        retrieved.personality_type == test_person.personality_type):
        print(f"✅ Successfully retrieved: {retrieved.name}")
        print(f"   Type: {retrieved.personality_type}")
        print(f"   Email: {retrieved.email}")
        return True
    else:
        print("❌ Retrieved data doesn't match")
        return False


if __name__ == "__main__":
    # Run atomic test when executed directly
    import sys
    success = test_atomic_feature()
    sys.exit(0 if success else 1)