"""Parallel processing utilities for concurrent downloads"""
import concurrent.futures
import threading
import time
from typing import List, Callable, Any, Dict, Optional, Tuple
from queue import Queue
import multiprocessing
from functools import partial

# Import utilities using centralized import handling
from .import_utils import safe_import

# Setup imports using safe_import to eliminate try/except blocks
get_logger = safe_import('logging_config', ['get_logger'])
RateLimiter = safe_import('rate_limiter', ['RateLimiter'])

# Setup module logger
logger = get_logger(__name__)


class ProgressTracker:
    """Thread-safe progress tracker"""
    
    def __init__(self, total: int, logger=None):
        self.total = total
        self.completed = 0
        self.failed = 0
        self.lock = threading.Lock()
        self.logger = logger
    
    def update(self, success: bool = True):
        """Update progress counter"""
        with self.lock:
            if success:
                self.completed += 1
            else:
                self.failed += 1
            
            current = self.completed + self.failed
            if self.logger and current % 10 == 0:
                self.logger.info(
                    f"Progress: {current}/{self.total} "
                    f"({self.completed} successful, {self.failed} failed)"
                )
    
    def get_stats(self) -> Dict[str, int]:
        """Get current statistics"""
        with self.lock:
            return {
                'total': self.total,
                'completed': self.completed,
                'failed': self.failed,
                'pending': self.total - self.completed - self.failed
            }


def parallel_process(
    items: List[Any],
    process_func: Callable,
    max_workers: Optional[int] = None,
    rate_limit: Optional[float] = None,
    logger=None,
    thread_safe: bool = True
) -> List[Tuple[Any, Any]]:
    """
    Process items in parallel using ThreadPoolExecutor or ProcessPoolExecutor
    
    Args:
        items: List of items to process
        process_func: Function to process each item
        max_workers: Maximum number of concurrent workers (default: CPU count)
        rate_limit: Maximum operations per second (optional)
        logger: Logger instance
        thread_safe: Use threads (True) or processes (False)
    
    Returns:
        List of tuples (item, result) where result is either the return value or exception
    """
    if not items:
        return []
    
    if not logger:
        # Use module-level logger
        logger = globals()['logger']
        logger = globals()['logger']
    
    # Default workers based on CPU count
    if max_workers is None:
        max_workers = min(multiprocessing.cpu_count() * 2, len(items))
    
    # Ensure we don't create more workers than items
    max_workers = min(max_workers, len(items))
    
    logger.info(f"Processing {len(items)} items with {max_workers} workers")
    
    # Create rate limiter if specified
    rate_limiter = RateLimiter(rate_limit) if rate_limit else None
    
    # Create progress tracker
    progress = ProgressTracker(len(items), logger)
    
    # Wrapper function to add rate limiting and progress tracking
    def process_with_tracking(item):
        try:
            # Apply rate limiting
            if rate_limiter:
                rate_limiter.wait_if_needed()
            
            # Process the item
            result = process_func(item)
            progress.update(success=True)
            return (item, result)
        except Exception as e:
            progress.update(success=False)
            logger.error(f"Error processing {item}: {e}")
            return (item, e)
    
    # Choose executor based on thread safety
    executor_class = concurrent.futures.ThreadPoolExecutor if thread_safe else concurrent.futures.ProcessPoolExecutor
    
    results = []
    with executor_class(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_item = {
            executor.submit(process_with_tracking, item): item 
            for item in items
        }
        
        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_item):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                item = future_to_item[future]
                logger.error(f"Unexpected error processing {item}: {e}")
                results.append((item, e))
    
    # Final statistics
    stats = progress.get_stats()
    logger.info(
        f"Processing complete: {stats['completed']} successful, "
        f"{stats['failed']} failed out of {stats['total']} total"
    )
    
    return results


def batch_parallel_process(
    items: List[Any],
    process_func: Callable,
    batch_size: int = 10,
    max_workers: Optional[int] = None,
    rate_limit: Optional[float] = None,
    logger=None
) -> List[Tuple[Any, Any]]:
    """
    Process items in parallel with batching to avoid overwhelming the system
    
    Args:
        items: List of items to process
        process_func: Function to process each item
        batch_size: Number of items per batch
        max_workers: Maximum number of concurrent workers
        rate_limit: Maximum operations per second
        logger: Logger instance
    
    Returns:
        List of tuples (item, result)
    """
    if not logger:
        # Use module-level logger
        logger = globals()['logger']
        logger = globals()['logger']
    
    all_results = []
    
    # Process in batches
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        logger.info(f"Processing batch {i // batch_size + 1} ({len(batch)} items)")
        
        batch_results = parallel_process(
            batch,
            process_func,
            max_workers=max_workers,
            rate_limit=rate_limit,
            logger=logger
        )
        
        all_results.extend(batch_results)
        
        # Small delay between batches
        if i + batch_size < len(items):
            time.sleep(1)
    
    return all_results


# YouTube-specific parallel download function
def parallel_download_youtube_videos(
    urls: List[str],
    max_workers: int = 4,
    transcript_only: bool = False,
    resolution: str = "720",
    logger=None
) -> List[Tuple[str, Tuple[Any, Any]]]:
    """
    Download multiple YouTube videos in parallel
    
    Args:
        urls: List of YouTube URLs
        max_workers: Maximum concurrent downloads
        transcript_only: Only download transcripts
        resolution: Video resolution
        logger: Logger instance
    
    Returns:
        List of tuples (url, (video_file, transcript_file))
    """
    try:
        from download_youtube import download_video
    except ImportError:
        from .download_youtube import download_video
    
    if not logger:
        # Use module-level logger
        logger = globals()['logger']
    
    # Create partial function with fixed parameters
    download_func = partial(
        download_video,
        transcript_only=transcript_only,
        resolution=resolution,
        logger=logger
    )
    
    # Process with rate limiting to avoid YouTube rate limits
    results = parallel_process(
        urls,
        download_func,
        max_workers=max_workers,
        rate_limit=1.0,  # Max 1 download per second
        logger=logger
    )
    
    return results


# Example usage and tests
if __name__ == "__main__":
    import random
    
    # Use module-level logger
    logger.info("Testing parallel processing utilities...")
    
    # Test function that simulates work
    def simulate_work(item):
        """Simulate some work with random duration"""
        duration = random.uniform(0.1, 0.5)
        time.sleep(duration)
        
        # Simulate occasional failures
        if random.random() < 0.1:  # 10% failure rate
            raise ValueError(f"Simulated failure for item {item}")
        
        return f"Processed {item} in {duration:.2f}s"
    
    # Test with small batch
    test_items = [f"item_{i}" for i in range(20)]
    
    logger.info(f"Processing {len(test_items)} items in parallel...")
    start_time = time.time()
    
    results = parallel_process(
        test_items,
        simulate_work,
        max_workers=4,
        rate_limit=5.0  # Max 5 per second
    )
    
    elapsed = time.time() - start_time
    
    # Count successes and failures
    successes = sum(1 for _, result in results if not isinstance(result, Exception))
    failures = sum(1 for _, result in results if isinstance(result, Exception))
    
    logger.info("Results:")
    logger.info(f"  Total time: {elapsed:.2f}s")
    logger.info(f"  Successful: {successes}")
    logger.info(f"  Failed: {failures}")
    logger.info(f"  Average time per item: {elapsed / len(test_items):.2f}s")
    
    # Show some results
    logger.info("Sample results:")
    for item, result in results[:5]:
        if isinstance(result, Exception):
            logger.error(f"  {item}: ERROR - {result}")
        else:
            logger.info(f"  {item}: {result}")
    
    logger.info("Parallel processing utilities ready!")