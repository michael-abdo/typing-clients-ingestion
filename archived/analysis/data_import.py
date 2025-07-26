#!/usr/bin/env python3
"""
Data Import and Validation for UUID System
Created: 2025-07-13

Imports data from migration tracking into production schema:
1. Creates person records from CSV and migration data
2. Creates file records with UUID mappings
3. Validates data integrity and relationships
4. Generates verification reports
"""

import csv
import json
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from typing import Dict, List, Tuple, Optional
import logging
from pathlib import Path
from decimal import Decimal
from migration_utilities import DEFAULT_DB_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def convert_decimal(obj):
    """Convert Decimal objects to float for JSON serialization"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal(item) for item in obj]
    return obj

class DataImporter:
    """Imports and validates data for the UUID-based system"""
    
    def __init__(self, csv_file: str, db_config: Dict = None):
        self.csv_file = csv_file
        self.db_config = db_config or DEFAULT_DB_CONFIG
        self.logger = logging.getLogger(__name__)
        
        # Statistics
        self.total_people = 0
        self.imported_people = 0
        self.total_files = 0
        self.imported_files = 0
        self.errors = []
    
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)
    
    def load_csv_data(self) -> pd.DataFrame:
        """Load CSV data with person information"""
        self.logger.info(f"📋 Loading CSV data from: {self.csv_file}")
        
        df = pd.read_csv(self.csv_file)
        self.total_people = len(df)
        
        self.logger.info(f"✅ Loaded {self.total_people} people from CSV")
        return df
    
    def get_migration_data(self) -> List[Dict]:
        """Get successful migration data"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM migration_state 
                    WHERE operation_status = 'completed'
                    ORDER BY person_id, source_path
                """)
                migration_data = [dict(row) for row in cur.fetchall()]
        
        self.total_files = len(migration_data)
        self.logger.info(f"📁 Found {self.total_files} successfully migrated files")
        
        return migration_data
    
    def determine_file_type(self, file_extension: str) -> str:
        """Determine file type from extension"""
        ext = file_extension.lower()
        
        if ext in ['.mp3', '.m4a', '.wav', '.flac']:
            return 'audio'
        elif ext in ['.mp4', '.webm', '.avi', '.mov']:
            return 'video'
        elif ext in ['.pdf', '.doc', '.docx', '.txt']:
            return 'document'
        elif ext in ['.srt', '.vtt']:
            return 'transcript'
        elif ext in ['.json']:
            return 'metadata'
        elif ext in ['.jpg', '.png', '.gif']:
            return 'image'
        elif ext in ['.bin', '.dat']:
            return 'binary'
        else:
            return 'other'
    
    def get_mime_type(self, file_extension: str) -> str:
        """Get MIME type from extension"""
        ext = file_extension.lower()
        
        mime_types = {
            '.mp3': 'audio/mpeg',
            '.m4a': 'audio/mp4',
            '.mp4': 'video/mp4',
            '.webm': 'video/webm',
            '.pdf': 'application/pdf',
            '.json': 'application/json',
            '.srt': 'text/srt',
            '.txt': 'text/plain',
            '.jpg': 'image/jpeg',
            '.png': 'image/png',
            '.bin': 'application/octet-stream',
            '.dat': 'application/octet-stream'
        }
        
        return mime_types.get(ext, 'application/octet-stream')
    
    def import_people(self, df: pd.DataFrame) -> Dict[int, int]:
        """Import people from CSV into person table"""
        self.logger.info("👥 Importing people...")
        
        # Remove duplicates based on row_id, keeping the first occurrence
        df_deduplicated = df.drop_duplicates(subset='row_id', keep='first')
        if len(df_deduplicated) < len(df):
            removed_duplicates = len(df) - len(df_deduplicated)
            self.logger.warning(f"⚠️  Removed {removed_duplicates} duplicate row_id entries")
        
        person_mapping = {}  # row_id -> person_id
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Prepare person data
                person_data = []
                for _, row in df_deduplicated.iterrows():
                    person_data.append((
                        int(row['row_id']),
                        str(row['name']).strip(),
                        str(row.get('email', '')).strip() if pd.notna(row.get('email')) else None,
                        str(row.get('type', '')).strip() if pd.notna(row.get('type')) else None
                    ))
                
                # Insert people (handle duplicates)
                insert_query = """
                    INSERT INTO person (row_id, name, email, type)
                    VALUES %s
                    ON CONFLICT (row_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        email = EXCLUDED.email,
                        type = EXCLUDED.type,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING row_id, person_id
                """
                
                results = execute_values(
                    cur, insert_query, person_data,
                    template=None, page_size=100, fetch=True
                )
                
                # Build mapping
                for row_id, person_id in results:
                    person_mapping[row_id] = person_id
                    self.imported_people += 1
                
                conn.commit()
        
        self.logger.info(f"✅ Imported {self.imported_people} people")
        return person_mapping
    
    def import_files(self, migration_data: List[Dict], person_mapping: Dict[int, int]):
        """Import files from migration data into file table"""
        self.logger.info("📁 Importing files...")
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Prepare file data
                file_data = []
                for migration_record in migration_data:
                    person_id = person_mapping.get(migration_record['person_id'])
                    if not person_id:
                        self.logger.warning(f"⚠️  No person mapping for row_id {migration_record['person_id']}")
                        continue
                    
                    # Extract file extension from destination path
                    dest_path = migration_record['destination_path']
                    file_extension = Path(dest_path).suffix
                    
                    # Handle files without extensions
                    if not file_extension:
                        file_extension = '.bin'  # Default extension for binary files
                        # Fix the storage path to include the new extension
                        if dest_path.endswith('.'):
                            dest_path = dest_path[:-1] + file_extension
                        else:
                            dest_path = dest_path + file_extension
                        self.logger.warning(f"⚠️  File has no extension, using .bin and updating path: {dest_path}")
                    
                    # Determine file type and MIME type
                    file_type = self.determine_file_type(file_extension)
                    mime_type = self.get_mime_type(file_extension)
                    
                    # Extract original filename from source path
                    source_path = migration_record['source_path']
                    original_filename = Path(source_path).name
                    
                    file_data.append((
                        migration_record['file_uuid'],  # file_id (UUID)
                        person_id,
                        dest_path,  # storage_path
                        original_filename,
                        file_type,
                        file_extension,
                        migration_record['file_size'],
                        migration_record['source_checksum'],
                        mime_type,
                        source_path,  # source_s3_path
                        json.dumps({  # metadata
                            'migration_timestamp': migration_record['started_at'].isoformat() if migration_record['started_at'] else None,
                            'original_person_name': migration_record['person_name'],
                            'migration_batch': 'uuid_migration_2025'
                        })
                    ))
                
                # Insert files
                insert_query = """
                    INSERT INTO file (
                        file_id, person_id, storage_path, original_filename,
                        file_type, file_extension, file_size, source_checksum,
                        mime_type, source_s3_path, metadata
                    ) VALUES %s
                    ON CONFLICT (file_id) DO UPDATE SET
                        person_id = EXCLUDED.person_id,
                        storage_path = EXCLUDED.storage_path,
                        original_filename = EXCLUDED.original_filename,
                        file_type = EXCLUDED.file_type,
                        file_extension = EXCLUDED.file_extension,
                        file_size = EXCLUDED.file_size,
                        source_checksum = EXCLUDED.source_checksum,
                        mime_type = EXCLUDED.mime_type,
                        source_s3_path = EXCLUDED.source_s3_path,
                        metadata = EXCLUDED.metadata
                """
                
                execute_values(
                    cur, insert_query, file_data,
                    template=None, page_size=100
                )
                
                self.imported_files = len(file_data)
                conn.commit()
        
        self.logger.info(f"✅ Imported {self.imported_files} files")
    
    def validate_data_integrity(self) -> Dict:
        """Validate imported data integrity"""
        self.logger.info("🔍 Validating data integrity...")
        
        validation_results = {
            'person_count': 0,
            'file_count': 0,
            'orphaned_files': 0,
            'missing_files': 0,
            'file_type_breakdown': {},
            'size_statistics': {},
            'validation_errors': []
        }
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Count people
                cur.execute("SELECT COUNT(*) as count FROM person")
                validation_results['person_count'] = cur.fetchone()['count']
                
                # Count files
                cur.execute("SELECT COUNT(*) as count FROM file")
                validation_results['file_count'] = cur.fetchone()['count']
                
                # Check for orphaned files (shouldn't happen with foreign keys)
                cur.execute("""
                    SELECT COUNT(*) as count FROM file f
                    LEFT JOIN person p ON f.person_id = p.person_id
                    WHERE p.person_id IS NULL
                """)
                validation_results['orphaned_files'] = cur.fetchone()['count']
                
                # File type breakdown
                cur.execute("""
                    SELECT file_type, COUNT(*) as count, 
                           COALESCE(SUM(file_size), 0) as total_size
                    FROM file 
                    GROUP BY file_type 
                    ORDER BY count DESC
                """)
                for row in cur.fetchall():
                    validation_results['file_type_breakdown'][row['file_type']] = {
                        'count': row['count'],
                        'total_size': row['total_size']
                    }
                
                # Size statistics
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_files,
                        COALESCE(SUM(file_size), 0) as total_size,
                        COALESCE(AVG(file_size), 0) as avg_size,
                        COALESCE(MIN(file_size), 0) as min_size,
                        COALESCE(MAX(file_size), 0) as max_size
                    FROM file
                    WHERE file_size IS NOT NULL
                """)
                size_stats = dict(cur.fetchone())
                validation_results['size_statistics'] = size_stats
                
                # Check for people without files
                cur.execute("""
                    SELECT p.row_id, p.name FROM person p
                    LEFT JOIN file f ON p.person_id = f.person_id
                    WHERE f.file_id IS NULL
                """)
                people_without_files = [dict(row) for row in cur.fetchall()]
                if people_without_files:
                    validation_results['validation_errors'].append({
                        'type': 'people_without_files',
                        'count': len(people_without_files),
                        'details': people_without_files[:5]  # First 5 for reporting
                    })
        
        return validation_results
    
    def generate_import_report(self, validation_results: Dict) -> str:
        """Generate comprehensive import report"""
        report_file = f"data_import_report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        report = {
            'import_metadata': {
                'timestamp': pd.Timestamp.now().isoformat(),
                'csv_file': self.csv_file,
                'total_people_in_csv': self.total_people,
                'imported_people': self.imported_people,
                'total_files_migrated': self.total_files,
                'imported_files': self.imported_files
            },
            'validation_results': validation_results,
            'summary': {
                'people_import_success_rate': (self.imported_people / self.total_people * 100) if self.total_people > 0 else 0,
                'files_import_success_rate': (self.imported_files / self.total_files * 100) if self.total_files > 0 else 0,
                'data_integrity_status': 'PASS' if len(validation_results['validation_errors']) == 0 else 'WARNINGS'
            }
        }
        
        # Convert any Decimal objects before JSON serialization
        report_converted = convert_decimal(report)
        
        with open(report_file, 'w') as f:
            json.dump(report_converted, f, indent=2)
        
        return report_file
    
    def print_summary(self, validation_results: Dict, report_file: str):
        """Print import summary"""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("📊 DATA IMPORT SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"👥 People: {validation_results['person_count']} imported")
        self.logger.info(f"📁 Files: {validation_results['file_count']} imported")
        self.logger.info(f"🔗 Orphaned Files: {validation_results['orphaned_files']}")
        
        self.logger.info("\nFile Type Breakdown:")
        for file_type, stats in validation_results['file_type_breakdown'].items():
            size_mb = stats['total_size'] / (1024 * 1024)
            self.logger.info(f"  {file_type}: {stats['count']} files ({size_mb:.1f} MB)")
        
        size_stats = validation_results['size_statistics']
        total_gb = size_stats['total_size'] / (1024 ** 3)
        self.logger.info(f"\n💾 Total Size: {total_gb:.2f} GB")
        
        if validation_results['validation_errors']:
            self.logger.warning(f"\n⚠️  {len(validation_results['validation_errors'])} validation warnings")
            for error in validation_results['validation_errors']:
                self.logger.warning(f"   - {error['type']}: {error['count']} items")
        else:
            self.logger.info("\n✅ All validation checks passed!")
        
        self.logger.info(f"\n📄 Report saved: {report_file}")
    
    def execute_import(self) -> Dict:
        """Execute the complete data import process"""
        self.logger.info("🚀 Starting Data Import Process")
        
        try:
            # Load CSV data
            df = self.load_csv_data()
            
            # Get migration data
            migration_data = self.get_migration_data()
            
            # Import people
            person_mapping = self.import_people(df)
            
            # Import files
            self.import_files(migration_data, person_mapping)
            
            # Validate data integrity
            validation_results = self.validate_data_integrity()
            
            # Generate report
            report_file = self.generate_import_report(validation_results)
            
            # Print summary
            self.print_summary(validation_results, report_file)
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"❌ Import failed: {e}")
            raise

def main():
    """Main execution function"""
    CSV_FILE = 'outputs/output.csv'
    
    # Check if CSV file exists
    if not Path(CSV_FILE).exists():
        print(f"❌ CSV file not found: {CSV_FILE}")
        return
    
    # Create importer and execute
    importer = DataImporter(CSV_FILE)
    
    try:
        validation_results = importer.execute_import()
        
        if len(validation_results['validation_errors']) == 0:
            print("\n🎉 Data import completed successfully!")
            print("Ready to proceed to next phase")
        else:
            print(f"\n⚠️  Import completed with {len(validation_results['validation_errors'])} warnings")
            print("Review validation report before proceeding")
            
    except Exception as e:
        print(f"\n❌ Import failed: {e}")

if __name__ == "__main__":
    main()