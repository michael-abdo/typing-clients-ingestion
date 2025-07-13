"""Input validation and sanitization utilities to prevent security vulnerabilities"""
import re
import os
import time
import subprocess
import importlib
from datetime import datetime
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse, parse_qs
from pathlib import Path


class ValidationError(Exception):
    """Raised when input validation fails"""
    pass


def validate_url(url, allowed_domains=None):
    """
    Validate and sanitize a URL to prevent injection attacks
    
    Args:
        url: The URL to validate
        allowed_domains: Optional list of allowed domains (e.g., ['youtube.com', 'docs.google.com'])
    
    Returns:
        Sanitized URL string
        
    Raises:
        ValidationError: If URL is invalid or potentially malicious
    """
    if not url or not isinstance(url, str):
        raise ValidationError("URL must be a non-empty string")
    
    # Remove any null bytes or control characters
    url = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', url)
    
    # Basic URL validation
    try:
        parsed = urlparse(url)
    except Exception:
        raise ValidationError(f"Invalid URL format: {url}")
    
    # Ensure URL has a scheme and netloc
    if not parsed.scheme or not parsed.netloc:
        raise ValidationError(f"URL must include protocol and domain: {url}")
    
    # Only allow http/https
    if parsed.scheme not in ('http', 'https'):
        raise ValidationError(f"Only HTTP/HTTPS URLs are allowed: {url}")
    
    # Check for suspicious characters that might indicate injection
    suspicious_patterns = [
        r'[;|`$]',  # Command separators (removed & which is valid in URLs)
        r'\.\./',     # Path traversal
        r'%00',       # Null byte
        r'\$\(',      # Command substitution
        r'\{.*\}',    # Variable expansion
        r"'.*&&.*'",  # Single quotes with command separator
        r'".*&&.*"',  # Double quotes with command separator
    ]
    
    for pattern in suspicious_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            raise ValidationError(f"URL contains suspicious characters: {url}")
    
    # Validate against allowed domains if specified
    if allowed_domains:
        domain = parsed.netloc.lower()
        # Remove www. prefix for comparison
        domain = domain.replace('www.', '')
        
        if not any(domain.endswith(allowed_domain.lower()) for allowed_domain in allowed_domains):
            raise ValidationError(f"URL domain not in allowed list: {domain}")
    
    return url


def validate_youtube_url(url):
    """
    Validate a YouTube URL and extract video ID
    
    Returns:
        tuple: (sanitized_url, video_id)
    """
    url = validate_url(url, allowed_domains=['youtube.com', 'youtu.be'])
    
    # Extract video ID
    video_id = None
    
    if 'youtu.be' in url:
        # Match video ID followed by end of string, /, ? or #
        match = re.search(r'youtu\.be/([a-zA-Z0-9_-]{11})(?:[/?#]|$)', url)
        if match:
            video_id = match.group(1)
    else:
        # Match video ID followed by &, #, or end of string
        match = re.search(r'[?&]v=([a-zA-Z0-9_-]{11})(?:[&#]|$)', url)
        if match:
            video_id = match.group(1)
    
    if not video_id:
        raise ValidationError(f"Could not extract valid YouTube video ID from URL: {url}")
    
    # Validate video ID format
    if not re.match(r'^[a-zA-Z0-9_-]{11}$', video_id):
        raise ValidationError(f"Invalid YouTube video ID format: {video_id}")
    
    return url, video_id


def validate_google_drive_url(url):
    """
    Validate a Google Drive URL and extract file ID
    
    Returns:
        tuple: (sanitized_url, file_id)
    """
    url = validate_url(url, allowed_domains=['drive.google.com', 'docs.google.com', 'drive.usercontent.google.com'])
    
    # Extract file ID from various Google Drive URL formats
    file_id = None
    
    # Format 1: /file/d/{id}/view
    match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', url)
    if match:
        file_id = match.group(1)
    
    # Format 2: ?id={id}
    if not file_id:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        if 'id' in params:
            file_id = params['id'][0]
    
    # Format 3: /document/d/{id}/
    if not file_id:
        match = re.search(r'/document/d/([a-zA-Z0-9_-]+)', url)
        if match:
            file_id = match.group(1)
    
    if not file_id:
        raise ValidationError(f"Could not extract file ID from Google Drive URL: {url}")
    
    # Validate file ID format (alphanumeric, underscore, hyphen)
    if not re.match(r'^[a-zA-Z0-9_-]+$', file_id):
        raise ValidationError(f"Invalid Google Drive file ID format: {file_id}")
    
    return url, file_id


def validate_youtube_playlist_url(url):
    """
    Validate a YouTube playlist URL (watch_videos format)
    
    Returns:
        tuple: (sanitized_url, list of video_ids)
    """
    url = validate_url(url, allowed_domains=['youtube.com'])
    
    video_ids = []
    
    # Check for watch_videos format
    if 'watch_videos?video_ids=' in url:
        match = re.search(r'watch_videos\?video_ids=([a-zA-Z0-9_,-]+)', url)
        if match:
            ids_string = match.group(1)
            # Split by comma and validate each ID
            potential_ids = ids_string.split(',')
            for pid in potential_ids:
                # Validate each video ID using the dedicated function
                try:
                    validate_youtube_video_id(pid)
                    video_ids.append(pid)
                except ValidationError:
                    # Skip invalid IDs instead of failing
                    continue
    
    # Check for playlist format
    elif 'playlist?list=' in url:
        match = re.search(r'playlist\?list=([a-zA-Z0-9_-]+)', url)
        if match:
            # For regular playlists, we can't extract individual video IDs
            # Just validate the playlist ID format
            playlist_id = match.group(1)
            if not re.match(r'^[a-zA-Z0-9_-]+$', playlist_id):
                raise ValidationError(f"Invalid YouTube playlist ID format: {playlist_id}")
            return url, []
    
    if not video_ids and 'playlist?list=' not in url:
        raise ValidationError(f"No valid YouTube video IDs found in playlist URL: {url}")
    
    # Reconstruct clean URL with only valid video IDs
    if video_ids:
        clean_url = f"https://www.youtube.com/watch_videos?video_ids={','.join(video_ids)}"
        return clean_url, video_ids
    
    return url, video_ids


def validate_youtube_video_id(video_id):
    """
    Validate a YouTube video ID format
    
    Returns:
        bool: True if valid, raises ValidationError if not
    """
    if not video_id:
        raise ValidationError("Video ID cannot be empty")
    
    if not isinstance(video_id, str):
        raise ValidationError("Video ID must be a string")
    
    # YouTube video IDs are exactly 11 characters: alphanumeric, underscore, or hyphen
    if not re.match(r'^[a-zA-Z0-9_-]{11}$', video_id):
        raise ValidationError(f"Invalid YouTube video ID format: {video_id}. Must be exactly 11 characters.")
    
    # Additional check for common patterns that indicate corrupted IDs
    # These patterns suggest the ID contains control characters or encoding issues
    if any(pattern in video_id.lower() for pattern in ['u00', '\\u', 'origin', 'null']):
        raise ValidationError(f"Video ID appears to be corrupted: {video_id}")
    
    return True


def validate_file_path(path, base_dir=None, must_exist=False):
    """
    Validate a file path to prevent path traversal attacks
    
    Args:
        path: The file path to validate
        base_dir: Optional base directory to ensure path stays within
        must_exist: Whether the path must already exist
    
    Returns:
        Sanitized absolute Path object
    """
    if not path or not isinstance(path, (str, Path)):
        raise ValidationError("Path must be a non-empty string or Path object")
    
    # Convert to Path object
    path = Path(path)
    
    # Resolve to absolute path (this also normalizes .. and .)
    try:
        abs_path = path.resolve()
    except Exception:
        raise ValidationError(f"Invalid path: {path}")
    
    # Check for null bytes
    if '\x00' in str(abs_path):
        raise ValidationError("Path contains null bytes")
    
    # If base_dir is specified, ensure path is within it
    if base_dir:
        base_dir = Path(base_dir).resolve()
        try:
            abs_path.relative_to(base_dir)
        except ValueError:
            raise ValidationError(f"Path escapes base directory: {abs_path}")
    
    # Check if path exists if required
    if must_exist and not abs_path.exists():
        raise ValidationError(f"Path does not exist: {abs_path}")
    
    return abs_path


def validate_filename(filename):
    """
    Validate a filename to ensure it's safe
    
    Returns:
        Sanitized filename
    """
    if not filename or not isinstance(filename, str):
        raise ValidationError("Filename must be a non-empty string")
    
    # Remove any path components
    filename = os.path.basename(filename)
    
    # Remove dangerous characters
    # Allow alphanumeric, spaces, dots, underscores, hyphens
    filename = re.sub(r'[^a-zA-Z0-9._\- ]', '', filename)
    
    # Ensure filename is not empty after sanitization
    if not filename or filename in ('.', '..'):
        raise ValidationError("Invalid filename after sanitization")
    
    # Limit length
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        if len(ext) > 20:  # Unreasonably long extension
            ext = ext[:20]
        name = name[:255 - len(ext) - 1]
        filename = name + ext
    
    return filename


# CSV sanitization moved to utils/sanitization.py for comprehensive handling


# Test the validation functions
if __name__ == "__main__":
    # Test URL validation
    test_urls = [
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtu.be/dQw4w9WgXcQ",
        "https://drive.google.com/file/d/1abc123/view",
        "https://docs.google.com/document/d/1abc123/edit",
        "javascript:alert('xss')",  # Should fail
        "https://evil.com/malware",  # Should fail with domain check
        "https://youtube.com/watch?v=test'; rm -rf /",  # Should fail
    ]
    
    print("Testing URL validation:")
    for url in test_urls:
        try:
            clean_url = validate_url(url, allowed_domains=['youtube.com', 'youtu.be', 'drive.google.com', 'docs.google.com'])
            print(f"✓ {url} -> Valid")
        except ValidationError as e:
            print(f"✗ {url} -> {e}")
    
    print("\nTesting path validation:")
    test_paths = [
        "downloads/video.mp4",
        "../../../etc/passwd",  # Should fail
        "downloads/../../../etc/passwd",  # Should fail
        "/tmp/file\x00.txt",  # Should fail
    ]
    
    for path in test_paths:
        try:
            clean_path = validate_file_path(path, base_dir=os.getcwd())
            print(f"✓ {path} -> {clean_path}")
        except ValidationError as e:
            print(f"✗ {path} -> {e}")


# ============================================================================
# SYSTEM VALIDATION (DRY Refactoring Compliance)
# ============================================================================

class SystemValidationError(Exception):
    """Raised when system validation fails"""
    pass


def run_validation(validation_type: str, **kwargs) -> Dict[str, Any]:
    """
    Run parameterized validation based on type.
    
    Consolidates validation logic from multiple root-level scripts:
    - validate_dry_refactoring.py
    - validate_imports.py
    - validate_consolidation.py
    - inline_validation.py
    - execute_validation.py
    
    Args:
        validation_type: Type of validation to run
        **kwargs: Additional parameters for specific validation types
        
    Returns:
        Dictionary with validation results
        
    Example:
        result = run_validation('dry_refactoring')
        result = run_validation('imports', modules=['utils.config', 'utils.patterns'])
    """
    validation_types = {
        'dry_refactoring': validate_dry_refactoring,
        'imports': validate_imports,
        'consolidation': validate_consolidation,
        'inline': validate_inline,
        'execution': validate_execution,
        'system_health': validate_system_health
    }
    
    if validation_type not in validation_types:
        raise SystemValidationError(f"Unknown validation type: {validation_type}")
    
    validator_func = validation_types[validation_type]
    return validator_func(**kwargs)


def validate_dry_refactoring(**kwargs) -> Dict[str, Any]:
    """
    Validate DRY refactoring compliance.
    
    Consolidates logic from validate_dry_refactoring.py
    """
    print("🔍 Validating DRY Refactoring Compliance")
    print("=" * 50)
    
    results = {
        'validation_type': 'dry_refactoring',
        'started_at': datetime.now().isoformat(),
        'tests': [],
        'passed': 0,
        'failed': 0,
        'total_time': 0.0
    }
    
    # Test cases from original validate_dry_refactoring.py
    test_cases = [
        ("python3 -c 'from utils.config import get_config; print(\"✅ Config import\")'", "Config Import"),
        ("python3 -c 'from utils.patterns import extract_youtube_id; print(\"✅ Patterns import\")'", "Patterns Import"),
        ("python3 -c 'from utils.csv_manager import CSVManager; print(\"✅ CSV Manager import\")'", "CSV Manager Import"),
        ("python3 -c 'from utils.s3_manager import UnifiedS3Manager; print(\"✅ S3 Manager import\")'", "S3 Manager Import"),
        ("python3 -c 'from utils.downloader import MinimalDownloader; print(\"✅ Downloader import\")'", "Downloader Import"),
    ]
    
    for cmd, description in test_cases:
        test_result = _run_test_command(cmd, description)
        results['tests'].append(test_result)
        
        if test_result['passed']:
            results['passed'] += 1
        else:
            results['failed'] += 1
        
        results['total_time'] += test_result['elapsed']
    
    results['completed_at'] = datetime.now().isoformat()
    results['success_rate'] = results['passed'] / len(test_cases) if test_cases else 0
    
    print(f"\n📊 DRY Refactoring Validation Summary:")
    print(f"✅ Passed: {results['passed']}/{len(test_cases)}")
    print(f"❌ Failed: {results['failed']}/{len(test_cases)}")
    print(f"📈 Success Rate: {results['success_rate']:.1%}")
    
    return results


def validate_imports(modules: Optional[List[str]] = None, **kwargs) -> Dict[str, Any]:
    """
    Validate import functionality.
    
    Consolidates logic from validate_imports.py
    """
    print("🔍 Validating Import Functionality")
    print("=" * 50)
    
    if modules is None:
        modules = [
            'utils.config',
            'utils.patterns', 
            'utils.error_handling',
            'utils.logging_config',
            'utils.csv_manager',
            'utils.s3_manager',
            'utils.downloader'
        ]
    
    results = {
        'validation_type': 'imports',
        'started_at': datetime.now().isoformat(),
        'modules_tested': modules,
        'passed': 0,
        'failed': 0,
        'import_results': []
    }
    
    for module_name in modules:
        try:
            start_time = time.time()
            module = importlib.import_module(module_name)
            elapsed = time.time() - start_time
            
            import_result = {
                'module': module_name,
                'passed': True,
                'error': None,
                'elapsed': elapsed
            }
            
            print(f"✅ {module_name}: SUCCESS ({elapsed:.3f}s)")
            results['passed'] += 1
            
        except Exception as e:
            import_result = {
                'module': module_name,
                'passed': False,
                'error': str(e),
                'elapsed': 0.0
            }
            
            print(f"❌ {module_name}: FAILED - {e}")
            results['failed'] += 1
        
        results['import_results'].append(import_result)
    
    results['completed_at'] = datetime.now().isoformat()
    results['success_rate'] = results['passed'] / len(modules) if modules else 0
    
    print(f"\n📊 Import Validation Summary:")
    print(f"✅ Passed: {results['passed']}/{len(modules)}")
    print(f"❌ Failed: {results['failed']}/{len(modules)}")
    print(f"📈 Success Rate: {results['success_rate']:.1%}")
    
    return results


def validate_consolidation(**kwargs) -> Dict[str, Any]:
    """
    Validate consolidation effectiveness.
    
    Consolidates logic from validate_consolidation.py
    """
    print("🔍 Validating Consolidation Effectiveness")
    print("=" * 50)
    
    results = {
        'validation_type': 'consolidation',
        'started_at': datetime.now().isoformat(),
        'checks': [],
        'passed': 0,
        'failed': 0
    }
    
    # Check that consolidated modules have expected functionality
    consolidation_checks = [
        ('utils.s3_manager', 'UnifiedS3Manager', 'S3 Manager Consolidation'),
        ('utils.downloader', 'MinimalDownloader', 'Downloader Consolidation'),
        ('utils.csv_manager', 'CSVManager', 'CSV Manager Consolidation'),
        ('utils.error_handling', 'with_standard_error_handling', 'Error Handling Consolidation'),
        ('utils.patterns', 'PatternRegistry', 'Patterns Consolidation')
    ]
    
    for module_name, expected_attr, description in consolidation_checks:
        try:
            module = importlib.import_module(module_name)
            
            if hasattr(module, expected_attr):
                check_result = {
                    'check': description,
                    'passed': True,
                    'details': f"Module {module_name} has {expected_attr}"
                }
                print(f"✅ {description}: SUCCESS")
                results['passed'] += 1
            else:
                check_result = {
                    'check': description,
                    'passed': False,
                    'details': f"Module {module_name} missing {expected_attr}"
                }
                print(f"❌ {description}: MISSING ATTRIBUTE")
                results['failed'] += 1
                
        except ImportError as e:
            check_result = {
                'check': description,
                'passed': False,
                'details': f"Import failed: {e}"
            }
            print(f"❌ {description}: IMPORT FAILED")
            results['failed'] += 1
        
        results['checks'].append(check_result)
    
    results['completed_at'] = datetime.now().isoformat()
    results['success_rate'] = results['passed'] / len(consolidation_checks) if consolidation_checks else 0
    
    print(f"\n📊 Consolidation Validation Summary:")
    print(f"✅ Passed: {results['passed']}/{len(consolidation_checks)}")
    print(f"❌ Failed: {results['failed']}/{len(consolidation_checks)}")
    print(f"📈 Success Rate: {results['success_rate']:.1%}")
    
    return results


def validate_inline(**kwargs) -> Dict[str, Any]:
    """
    Validate inline functionality.
    
    Consolidates logic from inline_validation.py
    """
    print("🔍 Validating Inline Functionality")
    print("=" * 50)
    
    results = {
        'validation_type': 'inline',
        'started_at': datetime.now().isoformat(),
        'inline_tests': [],
        'passed': 0,
        'failed': 0
    }
    
    # Basic inline tests
    inline_tests = [
        ("Basic config access", lambda: _test_config_access()),
        ("Pattern matching", lambda: _test_pattern_matching()),
        ("Error handling", lambda: _test_error_handling())
    ]
    
    for test_name, test_func in inline_tests:
        try:
            test_func()
            test_result = {
                'test': test_name,
                'passed': True,
                'error': None
            }
            print(f"✅ {test_name}: SUCCESS")
            results['passed'] += 1
        except Exception as e:
            test_result = {
                'test': test_name,
                'passed': False,
                'error': str(e)
            }
            print(f"❌ {test_name}: FAILED - {e}")
            results['failed'] += 1
        
        results['inline_tests'].append(test_result)
    
    results['completed_at'] = datetime.now().isoformat()
    results['success_rate'] = results['passed'] / len(inline_tests) if inline_tests else 0
    
    print(f"\n📊 Inline Validation Summary:")
    print(f"✅ Passed: {results['passed']}/{len(inline_tests)}")
    print(f"❌ Failed: {results['failed']}/{len(inline_tests)}")
    
    return results


def validate_execution(**kwargs) -> Dict[str, Any]:
    """
    Validate execution capability.
    
    Consolidates logic from execute_validation.py
    """
    print("🔍 Validating Execution Capability")
    print("=" * 50)
    
    results = {
        'validation_type': 'execution',
        'started_at': datetime.now().isoformat(),
        'execution_tests': [],
        'passed': 0,
        'failed': 0
    }
    
    # Check that main executables work
    execution_tests = [
        ("python3 simple_workflow.py --help", "Simple Workflow Help"),
        ("python3 utils/s3_manager.py --help", "S3 Manager Help"),
        ("python3 tests/run_all_tests.py --help", "Test Runner Help")
    ]
    
    for cmd, description in execution_tests:
        test_result = _run_test_command(cmd, description, timeout=10)
        results['execution_tests'].append(test_result)
        
        if test_result['passed']:
            results['passed'] += 1
        else:
            results['failed'] += 1
    
    results['completed_at'] = datetime.now().isoformat()
    results['success_rate'] = results['passed'] / len(execution_tests) if execution_tests else 0
    
    print(f"\n📊 Execution Validation Summary:")
    print(f"✅ Passed: {results['passed']}/{len(execution_tests)}")
    print(f"❌ Failed: {results['failed']}/{len(execution_tests)}")
    
    return results


def validate_system_health(**kwargs) -> Dict[str, Any]:
    """
    Comprehensive system health check.
    
    Runs all validation types and provides overall health assessment.
    """
    print("🏥 Running Comprehensive System Health Check")
    print("=" * 60)
    
    health_results = {
        'validation_type': 'system_health',
        'started_at': datetime.now().isoformat(),
        'component_results': {},
        'overall_health': 'unknown'
    }
    
    # Run all validation types
    validation_types = ['dry_refactoring', 'imports', 'consolidation', 'inline', 'execution']
    
    total_passed = 0
    total_tests = 0
    
    for val_type in validation_types:
        try:
            print(f"\n📋 Running {val_type} validation...")
            result = run_validation(val_type)
            health_results['component_results'][val_type] = result
            
            if 'passed' in result and 'failed' in result:
                total_passed += result['passed']
                total_tests += result['passed'] + result['failed']
            
        except Exception as e:
            print(f"❌ {val_type} validation failed: {e}")
            health_results['component_results'][val_type] = {
                'error': str(e),
                'passed': 0,
                'failed': 1
            }
            total_tests += 1
    
    # Calculate overall health
    if total_tests > 0:
        health_percentage = (total_passed / total_tests) * 100
        
        if health_percentage >= 90:
            health_results['overall_health'] = 'excellent'
        elif health_percentage >= 75:
            health_results['overall_health'] = 'good'
        elif health_percentage >= 50:
            health_results['overall_health'] = 'fair'
        else:
            health_results['overall_health'] = 'poor'
    
    health_results['completed_at'] = datetime.now().isoformat()
    health_results['total_passed'] = total_passed
    health_results['total_tests'] = total_tests
    health_results['health_percentage'] = (total_passed / total_tests * 100) if total_tests > 0 else 0
    
    print(f"\n🏥 System Health Assessment:")
    print(f"📊 Overall Score: {health_results['health_percentage']:.1f}% ({total_passed}/{total_tests})")
    print(f"🎯 Health Status: {health_results['overall_health'].upper()}")
    
    return health_results


def _run_test_command(cmd: str, description: str, timeout: int = 30) -> Dict[str, Any]:
    """Helper function to run a test command and return results"""
    start_time = time.time()
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        elapsed = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✅ {description}: SUCCESS ({elapsed:.2f}s)")
            return {
                'description': description,
                'command': cmd,
                'passed': True,
                'elapsed': elapsed,
                'output': result.stdout,
                'error': None
            }
        else:
            print(f"❌ {description}: FAILED")
            return {
                'description': description,
                'command': cmd,
                'passed': False,
                'elapsed': elapsed,
                'output': result.stdout,
                'error': result.stderr
            }
    except subprocess.TimeoutExpired:
        elapsed = timeout
        print(f"❌ {description}: TIMEOUT (>{timeout}s)")
        return {
            'description': description,
            'command': cmd,
            'passed': False,
            'elapsed': elapsed,
            'output': None,
            'error': f"Command timed out after {timeout} seconds"
        }
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ {description}: ERROR - {str(e)}")
        return {
            'description': description,
            'command': cmd,
            'passed': False,
            'elapsed': elapsed,
            'output': None,
            'error': str(e)
        }


def _test_config_access():
    """Test basic config access"""
    from utils.config import get_config
    config = get_config()
    if not config:
        raise Exception("Config is None or empty")


def _test_pattern_matching():
    """Test pattern matching functionality"""
    from utils.patterns import extract_youtube_id
    test_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    video_id = extract_youtube_id(test_url)
    if video_id != "dQw4w9WgXcQ":
        raise Exception(f"Pattern matching failed: expected 'dQw4w9WgXcQ', got '{video_id}'")


def _test_error_handling():
    """Test error handling functionality"""
    from utils.error_handling import with_standard_error_handling
    if not callable(with_standard_error_handling):
        raise Exception("Error handling decorator is not callable")