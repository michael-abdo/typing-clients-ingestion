#!/usr/bin/env python3
"""
Test Helper Utilities (DRY)
Shared test data factories and utilities for tests and benchmarks
"""

from utils.csv_manager import CSVManager


class TestDataFactory:
    """Factory for creating test data (DRY)"""
    
    @staticmethod
    def create_test_person(row_id: str = "100", name: str = "Test User", 
                          email: str = "test@example.com", 
                          personality_type: str = "FF-Ne/Ti-CP/B(S) #3",
                          doc_link: str = "") -> dict:
        """Create a test person record"""
        return {
            'row_id': row_id,
            'name': name,
            'email': email,
            'type': personality_type,
            'doc_link': doc_link
        }
    
    @staticmethod
    def create_test_links(youtube_count: int = 2, drive_count: int = 1) -> dict:
        """Create test link data"""
        youtube_links = [f"https://www.youtube.com/watch?v=test{i:011d}" for i in range(youtube_count)]
        drive_links = [f"https://drive.google.com/file/d/test{i:025d}/view" for i in range(drive_count)]
        
        return {
            'youtube': youtube_links,
            'drive_files': drive_links,
            'drive_folders': [],
            'all_links': youtube_links + drive_links
        }
    
    @staticmethod
    def create_test_document(content_length: int = 1000) -> tuple:
        """Create test document content and text"""
        doc_content = f"<html><body>{'Test content ' * (content_length // 12)}</body></html>"
        doc_text = "Test document text content " * (content_length // 27)
        return doc_content, doc_text
    
    @staticmethod
    def create_test_record(person: dict, links: dict = None, doc_text: str = "") -> dict:
        """Create a test CSV record using the centralized factory"""
        return CSVManager.create_record(person, mode='full', doc_text=doc_text, links=links)
    
    @staticmethod
    def generate_test_batch(num_records: int = 100, with_links: bool = False) -> list:
        """Generate a batch of test records for benchmarking (DRY)"""
        test_data = []
        for i in range(num_records):
            person = TestDataFactory.create_test_person(
                row_id=f"test_{i:04d}",
                name=f"Test Person {i}",
                email=f"test{i}@example.com",
                personality_type=f"Test-Type-{i % 5}",
                doc_link=f"https://example.com/doc_{i}" if i % 3 == 0 else ""
            )
            
            if with_links and i % 3 == 0:
                # Add some records with links for more realistic testing
                links = TestDataFactory.create_test_links(
                    youtube_count=i % 3 + 1,
                    drive_count=i % 2 + 1
                )
                record = TestDataFactory.create_test_record(person, links=links)
            else:
                record = person
                # For basic records, ensure 'link' field exists for compatibility
                record['link'] = record.get('doc_link', '')
            
            test_data.append(record)
        
        return test_data


# ============================================================================
# CONSOLIDATED TEST UTILITIES (DRY Phase 2)
# ============================================================================

import os
import time
import shutil
import tempfile
import subprocess
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Callable, Any
from datetime import datetime
import boto3
import requests
from io import BytesIO

# Import configuration
try:
    from .config import get_s3_bucket_name, get_s3_region, get_default_csv_file
except ImportError:
    from config import get_s3_bucket_name, get_s3_region, get_default_csv_file


class TestEnvironment:
    """Manage test environment setup and cleanup"""
    
    def __init__(self, test_name: str, base_dir: str = "/tmp"):
        self.test_name = test_name
        self.base_dir = Path(base_dir)
        self.test_dir = self.base_dir / f"test_{test_name}_{int(time.time())}"
        self.created_dirs = []
        self.created_files = []
        
    def __enter__(self):
        """Setup test environment"""
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.created_dirs.append(self.test_dir)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup test environment"""
        for dir_path in self.created_dirs:
            if dir_path.exists():
                shutil.rmtree(dir_path)
        
        for file_path in self.created_files:
            if Path(file_path).exists():
                os.remove(file_path)
    
    def create_subdir(self, name: str) -> Path:
        """Create subdirectory in test environment"""
        subdir = self.test_dir / name
        subdir.mkdir(parents=True, exist_ok=True)
        self.created_dirs.append(subdir)
        return subdir
    
    def create_temp_file(self, content: str, suffix: str = ".tmp") -> Path:
        """Create temporary file with content"""
        temp_file = self.test_dir / f"temp_{len(self.created_files)}{suffix}"
        with open(temp_file, 'w') as f:
            f.write(content)
        self.created_files.append(temp_file)
        return temp_file


class FilesystemMonitor:
    """Monitor filesystem for file creation during tests"""
    
    def __init__(self, watch_dir: Path):
        self.watch_dir = Path(watch_dir)
        self.before_files = set()
        self.start_monitoring()
    
    def start_monitoring(self):
        """Start monitoring the directory"""
        if self.watch_dir.exists():
            self.before_files = set(os.listdir(self.watch_dir))
        else:
            self.before_files = set()
    
    def get_new_files(self) -> List[str]:
        """Get list of new files created since monitoring started"""
        if not self.watch_dir.exists():
            return []
        
        current_files = set(os.listdir(self.watch_dir))
        new_files = current_files - self.before_files
        return list(new_files)
    
    def has_new_files(self) -> bool:
        """Check if any new files were created"""
        return len(self.get_new_files()) > 0


class TestCSVHandler:
    """Handle CSV operations for tests"""
    
    @staticmethod
    def read_test_csv(csv_file: Optional[str] = None) -> pd.DataFrame:
        """Read CSV file for testing"""
        if not csv_file:
            csv_file = get_default_csv_file()
        
        if not Path(csv_file).exists():
            raise FileNotFoundError(f"Test CSV file not found: {csv_file}")
        
        return pd.read_csv(csv_file)
    
    @staticmethod
    def find_person_with_youtube(df: pd.DataFrame, limit: int = 1) -> List[Dict]:
        """Find people with YouTube links for testing"""
        results = []
        for _, row in df.iterrows():
            youtube_links = TestCSVHandler.extract_links(row, 'youtube_playlist')
            if youtube_links and any('watch?v=' in link for link in youtube_links):
                results.append({
                    'row': row,
                    'youtube_links': youtube_links,
                    'person_name': row['name']
                })
                if len(results) >= limit:
                    break
        return results
    
    @staticmethod
    def find_person_with_drive(df: pd.DataFrame, limit: int = 1) -> List[Dict]:
        """Find people with Drive links for testing"""
        results = []
        for _, row in df.iterrows():
            drive_links = TestCSVHandler.extract_links(row, 'google_drive')
            if drive_links and any('/file/d/' in link for link in drive_links):
                results.append({
                    'row': row,
                    'drive_links': drive_links,
                    'person_name': row['name']
                })
                if len(results) >= limit:
                    break
        return results
    
    @staticmethod
    def extract_links(row: pd.Series, column: str) -> List[str]:
        """Extract links from CSV row"""
        links = str(row.get(column, '')).split('|') if pd.notna(row.get(column)) else []
        return [l.strip() for l in links if l and l != 'nan' and l.strip()]


class DownloadTester:
    """Test download functionality"""
    
    def __init__(self, test_env: TestEnvironment):
        self.test_env = test_env
        self.results = []
    
    def test_youtube_download(self, url: str, person_name: str, row_id: int, 
                            max_size: str = "5M") -> Dict:
        """Test YouTube download functionality"""
        person_dir = self.test_env.create_subdir(f"{row_id}_{person_name.replace(' ', '_')}")
        
        result = {
            'type': 'youtube',
            'url': url,
            'person': person_name,
            'success': False,
            'error': None,
            'files': [],
            'size_kb': 0
        }
        
        # Create download command
        output_file = person_dir / "youtube_test.%(ext)s"
        cmd = [
            "yt-dlp",
            "-f", "worstaudio",
            "-o", str(output_file),
            "--max-filesize", max_size,
            "--no-playlist",
            url
        ]
        
        try:
            process_result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if process_result.returncode == 0:
                # Check for downloaded files
                downloaded_files = list(person_dir.glob("youtube_test.*"))
                if downloaded_files:
                    result['success'] = True
                    result['files'] = [f.name for f in downloaded_files]
                    result['size_kb'] = sum(f.stat().st_size for f in downloaded_files) / 1024
                else:
                    result['error'] = "No files found after download"
            else:
                result['error'] = process_result.stderr[:200]
                
        except subprocess.TimeoutExpired:
            result['error'] = "Download timed out"
        except Exception as e:
            result['error'] = str(e)
        
        self.results.append(result)
        return result
    
    def test_drive_info_save(self, url: str, person_name: str, row_id: int) -> Dict:
        """Test Drive info saving functionality"""
        person_dir = self.test_env.create_subdir(f"{row_id}_{person_name.replace(' ', '_')}")
        
        result = {
            'type': 'drive_info',
            'url': url,
            'person': person_name,
            'success': False,
            'error': None,
            'files': [],
            'file_id': None
        }
        
        try:
            # Extract file ID
            import re
            file_id_match = re.search(r'file/d/([a-zA-Z0-9_-]+)', url)
            if file_id_match:
                file_id = file_id_match.group(1)
                result['file_id'] = file_id
                
                # Save info file
                info_file = person_dir / f"drive_{file_id}_info.json"
                info_data = {
                    "file_id": file_id,
                    "url": url,
                    "person": person_name,
                    "row_id": row_id,
                    "test_timestamp": datetime.now().isoformat(),
                    "note": "Test drive file info"
                }
                
                with open(info_file, 'w') as f:
                    json.dump(info_data, f, indent=2)
                
                result['success'] = True
                result['files'] = [info_file.name]
            else:
                result['error'] = "Could not extract file ID from URL"
                
        except Exception as e:
            result['error'] = str(e)
        
        self.results.append(result)
        return result
    
    def test_gdown_download(self, url: str, person_name: str, row_id: int, 
                           timeout: int = 10) -> Dict:
        """Test gdown download functionality"""
        person_dir = self.test_env.create_subdir(f"{row_id}_{person_name.replace(' ', '_')}")
        
        result = {
            'type': 'gdown',
            'url': url,
            'person': person_name,
            'success': False,
            'error': None,
            'files': [],
            'size_kb': 0
        }
        
        try:
            # Extract file ID
            import re
            file_id_match = re.search(r'file/d/([a-zA-Z0-9_-]+)', url)
            if file_id_match:
                file_id = file_id_match.group(1)
                output_file = person_dir / f"drive_{file_id}"
                
                cmd = ["gdown", f"https://drive.google.com/uc?id={file_id}", "-O", str(output_file)]
                
                process_result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
                
                if process_result.returncode == 0 and output_file.exists():
                    result['success'] = True
                    result['files'] = [output_file.name]
                    result['size_kb'] = output_file.stat().st_size / 1024
                else:
                    result['error'] = "Download failed or file not found"
            else:
                result['error'] = "Could not extract file ID from URL"
                
        except subprocess.TimeoutExpired:
            result['error'] = "Download timed out"
        except Exception as e:
            result['error'] = str(e)
        
        self.results.append(result)
        return result
    
    def get_summary(self) -> Dict:
        """Get summary of all test results"""
        return {
            'total_tests': len(self.results),
            'successful': len([r for r in self.results if r['success']]),
            'failed': len([r for r in self.results if not r['success']]),
            'results': self.results
        }


class S3Tester:
    """Test S3 operations"""
    
    def __init__(self, bucket_name: Optional[str] = None, region: Optional[str] = None):
        self.bucket_name = bucket_name or get_s3_bucket_name()
        self.region = region or get_s3_region()
        self.s3_client = boto3.client('s3', region_name=self.region)
        self.uploaded_keys = []
    
    def test_file_upload(self, file_path: Path, s3_key: str) -> Dict:
        """Test uploading a file to S3"""
        result = {
            'type': 's3_upload',
            'file_path': str(file_path),
            's3_key': s3_key,
            'success': False,
            'error': None,
            'size_bytes': 0,
            'upload_time': 0
        }
        
        try:
            if not file_path.exists():
                result['error'] = "File does not exist"
                return result
            
            start_time = time.time()
            
            self.s3_client.upload_file(
                str(file_path),
                self.bucket_name,
                s3_key,
                ExtraArgs={'ContentType': 'application/octet-stream'}
            )
            
            end_time = time.time()
            
            # Verify upload
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            
            result['success'] = True
            result['size_bytes'] = response['ContentLength']
            result['upload_time'] = end_time - start_time
            self.uploaded_keys.append(s3_key)
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def test_stream_upload(self, data: bytes, s3_key: str, content_type: str = 'application/octet-stream') -> Dict:
        """Test streaming upload to S3"""
        result = {
            'type': 's3_stream_upload',
            's3_key': s3_key,
            'success': False,
            'error': None,
            'size_bytes': len(data),
            'upload_time': 0
        }
        
        try:
            start_time = time.time()
            
            self.s3_client.upload_fileobj(
                BytesIO(data),
                self.bucket_name,
                s3_key,
                ExtraArgs={'ContentType': content_type}
            )
            
            end_time = time.time()
            
            # Verify upload
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            
            result['success'] = True
            result['upload_time'] = end_time - start_time
            self.uploaded_keys.append(s3_key)
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def test_direct_youtube_stream(self, url: str, s3_key: str) -> Dict:
        """Test direct YouTube to S3 streaming"""
        result = {
            'type': 'youtube_s3_stream',
            'url': url,
            's3_key': s3_key,
            'success': False,
            'error': None,
            'size_bytes': 0,
            'upload_time': 0
        }
        
        try:
            start_time = time.time()
            
            # Use yt-dlp to stream to stdout
            cmd = [
                "yt-dlp",
                "-f", "bestaudio[ext=webm]",
                "-o", "-",
                url
            ]
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # Stream to S3
            audio_data = BytesIO()
            while True:
                chunk = process.stdout.read(1024 * 1024)
                if not chunk:
                    break
                audio_data.write(chunk)
            
            process.wait()
            
            if process.returncode != 0:
                result['error'] = f"yt-dlp failed: {process.stderr.read().decode()[:200]}"
                return result
            
            # Upload to S3
            audio_data.seek(0)
            self.s3_client.upload_fileobj(
                audio_data,
                self.bucket_name,
                s3_key,
                ExtraArgs={'ContentType': 'video/webm'}
            )
            
            end_time = time.time()
            
            # Verify upload
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            
            result['success'] = True
            result['size_bytes'] = response['ContentLength']
            result['upload_time'] = end_time - start_time
            self.uploaded_keys.append(s3_key)
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def cleanup(self):
        """Clean up uploaded test files"""
        for s3_key in self.uploaded_keys:
            try:
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            except:
                pass
        self.uploaded_keys.clear()


class TestReporter:
    """Generate test reports"""
    
    @staticmethod
    def print_test_header(test_name: str, description: str = ""):
        """Print formatted test header"""
        print(f"\n🧪 {test_name.upper()}")
        if description:
            print(f"📝 {description}")
        print("=" * 70)
    
    @staticmethod
    def print_test_result(test_name: str, success: bool, details: str = ""):
        """Print formatted test result"""
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{status} {test_name}")
        if details:
            print(f"   {details}")
    
    @staticmethod
    def print_summary(results: List[Dict]):
        """Print test summary"""
        total = len(results)
        passed = len([r for r in results if r.get('success', False)])
        failed = total - passed
        
        print("\n" + "=" * 70)
        print("📊 TEST SUMMARY")
        print(f"Total tests: {total}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"📈 Success rate: {(passed/total)*100:.1f}%" if total > 0 else "No tests run")
        
        if failed > 0:
            print("\n❌ FAILED TESTS:")
            for result in results:
                if not result.get('success', False):
                    print(f"   - {result.get('type', 'unknown')}: {result.get('error', 'Unknown error')}")
    
    @staticmethod
    def save_report(results: List[Dict], report_file: str):
        """Save test report to JSON file"""
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'total_tests': len(results),
            'passed': len([r for r in results if r.get('success', False)]),
            'failed': len([r for r in results if not r.get('success', False)]),
            'results': results
        }
        
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2)


# Utility functions for common test operations
def run_quick_download_test(max_tests: int = 2) -> List[Dict]:
    """Run quick download tests with real data"""
    results = []
    
    with TestEnvironment("quick_download") as test_env:
        # Test CSV reading
        try:
            df = TestCSVHandler.read_test_csv()
            TestReporter.print_test_result("CSV Reading", True, f"Loaded {len(df)} rows")
        except Exception as e:
            TestReporter.print_test_result("CSV Reading", False, str(e))
            return results
        
        # Test YouTube downloads
        youtube_people = TestCSVHandler.find_person_with_youtube(df, max_tests)
        downloader = DownloadTester(test_env)
        
        for person_data in youtube_people:
            row = person_data['row']
            url = person_data['youtube_links'][0]
            result = downloader.test_youtube_download(url, row['name'], row['row_id'])
            results.append(result)
            
            TestReporter.print_test_result(
                f"YouTube Download - {row['name']}", 
                result['success'], 
                result.get('error', f"Downloaded {result.get('size_kb', 0):.1f} KB")
            )
        
        # Test Drive info saving
        drive_people = TestCSVHandler.find_person_with_drive(df, max_tests)
        
        for person_data in drive_people:
            row = person_data['row']
            url = person_data['drive_links'][0]
            result = downloader.test_drive_info_save(url, row['name'], row['row_id'])
            results.append(result)
            
            TestReporter.print_test_result(
                f"Drive Info - {row['name']}", 
                result['success'], 
                result.get('error', f"Saved info for {result.get('file_id', 'unknown')}")
            )
    
    return results


def run_direct_s3_test(test_youtube: bool = True, test_drive: bool = True) -> List[Dict]:
    """Run direct-to-S3 streaming tests"""
    results = []
    
    with TestEnvironment("direct_s3") as test_env:
        monitor = FilesystemMonitor(test_env.test_dir)
        s3_tester = S3Tester()
        
        if test_youtube:
            # Test YouTube direct streaming
            TestReporter.print_test_header("YouTube Direct-to-S3 Test")
            
            # Use a short test video
            test_url = "https://youtu.be/2iwahDWerSQ"
            s3_key = f"test_direct/youtube_test_{int(time.time())}.webm"
            
            result = s3_tester.test_direct_youtube_stream(test_url, s3_key)
            results.append(result)
            
            # Check for local files
            new_files = monitor.get_new_files()
            
            TestReporter.print_test_result(
                "YouTube Direct Stream", 
                result['success'] and len(new_files) == 0,
                f"Size: {result.get('size_bytes', 0)/1024:.1f} KB, Local files: {len(new_files)}"
            )
        
        if test_drive:
            # Test Drive direct streaming
            TestReporter.print_test_header("Drive Direct-to-S3 Test")
            
            # Test with small file
            drive_id = "1fKip6e5UFmQPaWY0EW2PJ7MC2eowQF2t"
            s3_key = f"test_direct/drive_test_{int(time.time())}.bin"
            
            # Stream Drive file
            download_url = f"https://drive.google.com/uc?id={drive_id}&export=download"
            try:
                response = requests.get(download_url, stream=True)
                file_obj = BytesIO()
                
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file_obj.write(chunk)
                
                file_obj.seek(0)
                
                result = s3_tester.test_stream_upload(file_obj.read(), s3_key)
                results.append(result)
                
                # Check for local files
                new_files = monitor.get_new_files()
                
                TestReporter.print_test_result(
                    "Drive Direct Stream", 
                    result['success'] and len(new_files) == 0,
                    f"Size: {result.get('size_bytes', 0)/1024:.1f} KB, Local files: {len(new_files)}"
                )
                
            except Exception as e:
                result = {
                    'type': 'drive_direct_stream',
                    'success': False,
                    'error': str(e)
                }
                results.append(result)
                TestReporter.print_test_result("Drive Direct Stream", False, str(e))
        
        # Cleanup
        s3_tester.cleanup()
    
    return results


# === DRY PHASE 2: CONSOLIDATED EXTRACTION TESTING PATTERNS ===

def run_extraction_test(row_id: str, test_case: dict, extraction_func: Callable) -> dict:
    """
    Standard extraction test runner with consistent reporting.
    
    Consolidates the repeated pattern found in 12+ test files:
        def test_extraction(row_id, test_case):
            print(f"Testing Row {row_id}: {test_case['name']}")
            try:
                result = extract_data(row_id)
                print("✓ Success")
                return result
            except Exception as e:
                print(f"❌ Error: {e}")
                return None
    
    Args:
        row_id: Row ID to test
        test_case: Dictionary with test case information
        extraction_func: Function that performs the extraction
        
    Returns:
        Dictionary with test results
        
    Example:
        result = run_extraction_test("123", {"name": "Test Person"}, extract_person_data)
        if result['success']:
            print(f"Extraction took {result['timing']:.2f}s")
    """
    TestReporter.print_test_header(f"Testing Row {row_id}: {test_case.get('name', 'Unknown')}")
    
    result = {
        'row_id': row_id,
        'test_case': test_case.get('name', 'Unknown'),
        'success': False,
        'extracted_data': None,
        'error': None,
        'timing': 0,
        'timestamp': datetime.now().isoformat()
    }
    
    try:
        start_time = time.time()
        extracted_data = extraction_func(row_id, test_case)
        end_time = time.time()
        
        result.update({
            'success': True,
            'extracted_data': extracted_data,
            'timing': end_time - start_time
        })
        
        TestReporter.print_test_result(
            f"Row {row_id} Extraction", 
            True, 
            f"Completed in {result['timing']:.2f}s"
        )
        
    except Exception as e:
        result['error'] = str(e)
        TestReporter.print_test_result(f"Row {row_id} Extraction", False, str(e))
    
    return result


def compare_extraction_results(expected: dict, actual: dict) -> dict:
    """
    Compare extraction results with expected values.
    
    Consolidates result comparison patterns used in extraction tests.
    
    Args:
        expected: Expected extraction results
        actual: Actual extraction results
        
    Returns:
        Dictionary with comparison results
        
    Example:
        comparison = compare_extraction_results(expected_data, actual_data)
        if comparison['match']:
            print(f"Perfect match! Similarity: {comparison['similarity_score']:.2f}")
    """
    comparison = {
        'match': True,
        'differences': [],
        'similarity_score': 0.0,
        'details': {
            'expected_keys': set(expected.keys()) if expected else set(),
            'actual_keys': set(actual.keys()) if actual else set(),
            'common_keys': set(),
            'missing_keys': set(),
            'extra_keys': set()
        }
    }
    
    # Handle None/empty cases
    if not expected and not actual:
        comparison['similarity_score'] = 1.0
        return comparison
    
    if not expected or not actual:
        comparison['match'] = False
        comparison['differences'].append("One of the results is None/empty")
        return comparison
    
    # Analyze key differences
    expected_keys = set(expected.keys())
    actual_keys = set(actual.keys())
    
    comparison['details']['common_keys'] = expected_keys & actual_keys
    comparison['details']['missing_keys'] = expected_keys - actual_keys
    comparison['details']['extra_keys'] = actual_keys - expected_keys
    
    # Check for missing or extra keys
    if comparison['details']['missing_keys']:
        comparison['match'] = False
        comparison['differences'].append(f"Missing keys: {list(comparison['details']['missing_keys'])}")
    
    if comparison['details']['extra_keys']:
        comparison['differences'].append(f"Extra keys: {list(comparison['details']['extra_keys'])}")
    
    # Compare values for common keys
    matches = 0
    total_comparisons = 0
    
    for key in comparison['details']['common_keys']:
        total_comparisons += 1
        expected_val = expected[key]
        actual_val = actual[key]
        
        if expected_val == actual_val:
            matches += 1
        else:
            comparison['match'] = False
            
            # Special handling for different data types
            if isinstance(expected_val, list) and isinstance(actual_val, list):
                list_similarity = _compare_lists(expected_val, actual_val)
                comparison['differences'].append(f"Key '{key}': list similarity {list_similarity:.2f}")
            elif isinstance(expected_val, str) and isinstance(actual_val, str):
                string_similarity = _compare_strings(expected_val, actual_val)
                comparison['differences'].append(f"Key '{key}': string similarity {string_similarity:.2f}")
            else:
                comparison['differences'].append(f"Key '{key}': expected {expected_val}, got {actual_val}")
    
    # Calculate similarity score
    if total_comparisons > 0:
        key_score = len(comparison['details']['common_keys']) / len(expected_keys)
        value_score = matches / total_comparisons
        comparison['similarity_score'] = (key_score + value_score) / 2
    
    return comparison


def _compare_lists(list1: list, list2: list) -> float:
    """Compare two lists and return similarity score."""
    if not list1 and not list2:
        return 1.0
    
    if not list1 or not list2:
        return 0.0
    
    # Convert to sets for comparison
    set1 = set(str(item) for item in list1)
    set2 = set(str(item) for item in list2)
    
    intersection = set1 & set2
    union = set1 | set2
    
    return len(intersection) / len(union) if union else 0.0


def _compare_strings(str1: str, str2: str) -> float:
    """Compare two strings and return similarity score."""
    if str1 == str2:
        return 1.0
    
    if not str1 or not str2:
        return 0.0
    
    # Simple character-based similarity
    str1 = str1.lower().strip()
    str2 = str2.lower().strip()
    
    if str1 == str2:
        return 1.0
    
    # Check if one is contained in the other
    if str1 in str2 or str2 in str1:
        return 0.8
    
    # Basic word overlap
    words1 = set(str1.split())
    words2 = set(str2.split())
    
    if words1 and words2:
        intersection = words1 & words2
        union = words1 | words2
        return len(intersection) / len(union)
    
    return 0.0


def run_batch_extraction_tests(test_cases: List[dict], extraction_func: Callable, 
                             max_concurrent: int = 1) -> List[dict]:
    """
    Run multiple extraction tests in batch.
    
    Consolidates batch testing patterns used across test scripts.
    
    Args:
        test_cases: List of test case dictionaries
        extraction_func: Function that performs extraction
        max_concurrent: Maximum number of concurrent tests (default: 1)
        
    Returns:
        List of test results
        
    Example:
        test_cases = [
            {"row_id": "123", "name": "Test Person 1", "expected": {...}},
            {"row_id": "456", "name": "Test Person 2", "expected": {...}}
        ]
        results = run_batch_extraction_tests(test_cases, extract_person_data)
    """
    TestReporter.print_test_header("Batch Extraction Tests", f"Running {len(test_cases)} tests")
    
    results = []
    
    for i, test_case in enumerate(test_cases):
        print(f"\nTest {i+1}/{len(test_cases)}")
        
        row_id = test_case.get('row_id', f'test_{i}')
        result = run_extraction_test(row_id, test_case, extraction_func)
        
        # If expected results are provided, compare them
        if 'expected' in test_case and result['success']:
            comparison = compare_extraction_results(test_case['expected'], result['extracted_data'])
            result['comparison'] = comparison
            
            if not comparison['match']:
                print(f"⚠️  Comparison failed: {comparison['differences']}")
        
        results.append(result)
    
    # Print summary
    TestReporter.print_summary(results)
    
    return results


def create_extraction_test_suite(csv_file: str = None, max_tests: int = 5) -> List[dict]:
    """
    Create a test suite from CSV data for extraction testing.
    
    Consolidates test suite creation patterns.
    
    Args:
        csv_file: Path to CSV file (default: from config)
        max_tests: Maximum number of tests to create
        
    Returns:
        List of test case dictionaries
        
    Example:
        test_suite = create_extraction_test_suite("outputs/output.csv", max_tests=3)
        results = run_batch_extraction_tests(test_suite, my_extraction_func)
    """
    try:
        df = TestCSVHandler.read_test_csv(csv_file)
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        return []
    
    test_cases = []
    
    # Sample rows for testing
    sample_rows = df.sample(min(max_tests, len(df)))
    
    for _, row in sample_rows.iterrows():
        test_case = {
            'row_id': str(row.get('row_id', 'unknown')),
            'name': row.get('name', 'Unknown Person'),
            'email': row.get('email', ''),
            'type': row.get('type', ''),
            'doc_link': row.get('link', ''),
            'expected': {
                'person_name': row.get('name', ''),
                'youtube_links': TestCSVHandler.extract_links(row, 'youtube_playlist'),
                'drive_links': TestCSVHandler.extract_links(row, 'google_drive'),
                'document_text': row.get('document_text', ''),
                'processed': row.get('processed', '')
            }
        }
        
        test_cases.append(test_case)
    
    return test_cases


def validate_extraction_function(extraction_func: Callable, test_sample_size: int = 3) -> dict:
    """
    Validate an extraction function with a small test sample.
    
    Consolidates extraction function validation patterns.
    
    Args:
        extraction_func: Function to validate
        test_sample_size: Number of tests to run for validation
        
    Returns:
        Dictionary with validation results
        
    Example:
        validation = validate_extraction_function(my_extract_func, test_sample_size=2)
        if validation['valid']:
            print("Function is ready for production use")
    """
    TestReporter.print_test_header("Extraction Function Validation")
    
    validation = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'test_results': [],
        'performance': {
            'avg_time': 0.0,
            'max_time': 0.0,
            'min_time': float('inf')
        }
    }
    
    # Create test suite
    test_cases = create_extraction_test_suite(max_tests=test_sample_size)
    
    if not test_cases:
        validation['valid'] = False
        validation['errors'].append("Could not create test cases")
        return validation
    
    # Run tests
    results = run_batch_extraction_tests(test_cases, extraction_func)
    validation['test_results'] = results
    
    # Analyze results
    successful_tests = [r for r in results if r['success']]
    failed_tests = [r for r in results if not r['success']]
    
    if not successful_tests:
        validation['valid'] = False
        validation['errors'].append("No successful tests")
    
    if len(failed_tests) > len(successful_tests):
        validation['valid'] = False
        validation['errors'].append(f"More failures ({len(failed_tests)}) than successes ({len(successful_tests)})")
    
    # Calculate performance metrics
    if successful_tests:
        timings = [r['timing'] for r in successful_tests]
        validation['performance']['avg_time'] = sum(timings) / len(timings)
        validation['performance']['max_time'] = max(timings)
        validation['performance']['min_time'] = min(timings)
        
        # Performance warnings
        if validation['performance']['avg_time'] > 10.0:
            validation['warnings'].append(f"Average extraction time is high: {validation['performance']['avg_time']:.2f}s")
        
        if validation['performance']['max_time'] > 30.0:
            validation['warnings'].append(f"Maximum extraction time is very high: {validation['performance']['max_time']:.2f}s")
    
    # Check for common issues
    common_errors = {}
    for result in failed_tests:
        error = result.get('error', 'Unknown error')
        common_errors[error] = common_errors.get(error, 0) + 1
    
    if common_errors:
        most_common_error = max(common_errors.items(), key=lambda x: x[1])
        validation['warnings'].append(f"Most common error: {most_common_error[0]} ({most_common_error[1]} times)")
    
    # Print validation summary
    print(f"\n📊 Validation Summary:")
    print(f"✅ Valid: {validation['valid']}")
    print(f"🧪 Tests run: {len(results)}")
    print(f"✅ Successful: {len(successful_tests)}")
    print(f"❌ Failed: {len(failed_tests)}")
    print(f"⏱️  Average time: {validation['performance']['avg_time']:.2f}s")
    
    if validation['errors']:
        print(f"❌ Errors: {validation['errors']}")
    
    if validation['warnings']:
        print(f"⚠️  Warnings: {validation['warnings']}")
    
    return validation


if __name__ == "__main__":
    # Test the extraction testing utilities
    print("=== Testing Extraction Test Utilities ===")
    
    # Example extraction function for testing
    def example_extraction_func(row_id: str, test_case: dict) -> dict:
        """Example extraction function for testing"""
        time.sleep(0.1)  # Simulate processing time
        return {
            'person_name': test_case.get('name', 'Unknown'),
            'extracted_text': f"Extracted content for {row_id}",
            'links_found': 2,
            'success': True
        }
    
    # Test single extraction
    print("\n1. Testing single extraction:")
    test_case = {'name': 'Test Person', 'row_id': '123'}
    result = run_extraction_test('123', test_case, example_extraction_func)
    print(f"Result: {result['success']}, Time: {result['timing']:.2f}s")
    
    # Test result comparison
    print("\n2. Testing result comparison:")
    expected = {'person_name': 'Test Person', 'links_found': 2}
    actual = {'person_name': 'Test Person', 'links_found': 2}
    comparison = compare_extraction_results(expected, actual)
    print(f"Match: {comparison['match']}, Similarity: {comparison['similarity_score']:.2f}")
    
    # Test with differences
    actual_different = {'person_name': 'Different Person', 'links_found': 1}
    comparison_diff = compare_extraction_results(expected, actual_different)
    print(f"Match: {comparison_diff['match']}, Differences: {comparison_diff['differences']}")
    
    print("\n=== Extraction test utilities ready! ===")