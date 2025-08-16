#!/usr/bin/env python3
"""
Channel Discovery Module for Mass Download Feature

Implements fail-fast, fail-loud, fail-safely principles for YouTube channel discovery:
- Fail Fast: Immediate validation of channel URLs and yt-dlp availability
- Fail Loud: Clear, actionable error messages with context
- Fail Safely: Safe enumeration with rollback on errors

Functionality:
- YouTube channel URL validation and normalization
- Channel video enumeration using yt-dlp
- Video metadata extraction with comprehensive validation
- Rate limiting and error recovery
"""

import os
import sys
import re
import json
import subprocess
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union, Tuple, NamedTuple
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import logging

# Fail-fast import validation
try:
    from utils.logging_config import get_logger
    from utils.rate_limiter import rate_limit
    from utils.retry_utils import retry_with_backoff
    from utils.error_handling import with_standard_error_handling
    from utils.validation import validate_youtube_url
    from utils.patterns import extract_youtube_id
    from utils.config import get_config
except ImportError as e:
    raise ImportError(
        f"CRITICAL: Required utilities not available for channel discovery. "
        f"Ensure you're running from the correct directory. Error: {e}"
    ) from e

# Initialize logger
logger = get_logger(__name__)

# Global validation state
_YT_DLP_VALIDATED = False
_MODULE_VALIDATED = False


@dataclass
class ChannelInfo:
    """
    Channel information with fail-fast validation.
    
    Contains basic channel metadata extracted from YouTube.
    """
    channel_id: str
    channel_url: str
    title: str
    description: Optional[str] = None
    subscriber_count: Optional[int] = None
    video_count: Optional[int] = None
    playlist_id: Optional[str] = None
    
    def __post_init__(self):
        """Fail-fast validation on creation."""
        self.validate()
    
    def validate(self) -> None:
        """
        Fail-fast validation of channel info.
        
        Raises:
            ValueError: If validation fails (fail-loud)
        """
        if not self.channel_id or not isinstance(self.channel_id, str):
            raise ValueError(
                f"VALIDATION ERROR: channel_id is required and must be non-empty string. "
                f"Got: {self.channel_id}"
            )
        
        if len(self.channel_id) < 5:  # YouTube channel IDs are longer
            raise ValueError(
                f"VALIDATION ERROR: channel_id appears invalid (too short). "
                f"Got: '{self.channel_id}' (length: {len(self.channel_id)})"
            )
        
        if not self.channel_url or not isinstance(self.channel_url, str):
            raise ValueError(
                f"VALIDATION ERROR: channel_url is required and must be non-empty string. "
                f"Got: {self.channel_url}"
            )
        
        if not self.channel_url.startswith(("https://youtube.com/", "https://www.youtube.com/")):
            raise ValueError(
                f"VALIDATION ERROR: Invalid channel_url format. "
                f"Must start with https://youtube.com/ or https://www.youtube.com/. "
                f"Got: {self.channel_url}"
            )
        
        if not self.title or not isinstance(self.title, str):
            raise ValueError(
                f"VALIDATION ERROR: title is required and must be non-empty string. "
                f"Got: {self.title}"
            )
        
        if self.subscriber_count is not None and (not isinstance(self.subscriber_count, int) or self.subscriber_count < 0):
            raise ValueError(
                f"VALIDATION ERROR: subscriber_count must be non-negative integer. "
                f"Got: {self.subscriber_count} (type: {type(self.subscriber_count)})"
            )
        
        if self.video_count is not None and (not isinstance(self.video_count, int) or self.video_count < 0):
            raise ValueError(
                f"VALIDATION ERROR: video_count must be non-negative integer. "
                f"Got: {self.video_count} (type: {type(self.video_count)})"
            )


@dataclass
class VideoMetadata:
    """
    Video metadata with fail-fast validation.
    
    Contains comprehensive video information extracted from YouTube.
    """
    video_id: str
    title: str
    description: Optional[str] = None
    duration: Optional[int] = None  # seconds
    upload_date: Optional[datetime] = None
    view_count: Optional[int] = None
    like_count: Optional[int] = None
    comment_count: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    categories: List[str] = field(default_factory=list)
    thumbnail_url: Optional[str] = None
    video_url: Optional[str] = None
    channel_id: Optional[str] = None
    uploader: Optional[str] = None
    is_live: bool = False
    age_restricted: bool = False
    
    def __post_init__(self):
        """Fail-fast validation on creation."""
        self.validate()
    
    def validate(self) -> None:
        """
        Fail-fast validation of video metadata.
        
        Raises:
            ValueError: If validation fails (fail-loud)
        """
        if not self.video_id or not isinstance(self.video_id, str):
            raise ValueError(
                f"VALIDATION ERROR: video_id is required and must be non-empty string. "
                f"Got: {self.video_id}"
            )
        
        if len(self.video_id) != 11:  # YouTube video IDs are exactly 11 chars
            raise ValueError(
                f"VALIDATION ERROR: YouTube video_id must be exactly 11 characters. "
                f"Got: '{self.video_id}' (length: {len(self.video_id)})"
            )
        
        if not self.title or not isinstance(self.title, str):
            raise ValueError(
                f"VALIDATION ERROR: title is required and must be non-empty string. "
                f"Got: {self.title}"
            )
        
        if self.duration is not None and (not isinstance(self.duration, int) or self.duration < 0):
            raise ValueError(
                f"VALIDATION ERROR: duration must be non-negative integer (seconds). "
                f"Got: {self.duration} (type: {type(self.duration)})"
            )
        
        if self.view_count is not None and (not isinstance(self.view_count, int) or self.view_count < 0):
            raise ValueError(
                f"VALIDATION ERROR: view_count must be non-negative integer. "
                f"Got: {self.view_count} (type: {type(self.view_count)})"
            )
        
        if self.video_url and not self.video_url.startswith(("https://youtube.com/", "https://www.youtube.com/")):
            raise ValueError(
                f"VALIDATION ERROR: Invalid video_url format. "
                f"Must start with https://youtube.com/ or https://www.youtube.com/. "
                f"Got: {self.video_url}"
            )


class YouTubeChannelDiscovery:
    """
    YouTube channel discovery with fail-fast/fail-loud/fail-safely principles.
    """
    
    def __init__(self, yt_dlp_path: str = "yt-dlp"):
        """
        Initialize channel discovery with fail-fast validation.
        
        Args:
            yt_dlp_path: Path to yt-dlp executable
            
        Raises:
            RuntimeError: If yt-dlp is not available or invalid
        """
        self.yt_dlp_path = yt_dlp_path
        self.config = get_config()
        
        # Fail-fast yt-dlp validation
        self._validate_yt_dlp()
        
        # Rate limiting settings
        self.rate_limit_delay = self.config.get("rate_limiting.services.youtube.rate", 2.0)
        
        logger.info("YouTubeChannelDiscovery initialized successfully")
    
    def _validate_yt_dlp(self) -> None:
        """
        Validate yt-dlp availability (fail-fast).
        
        Raises:
            RuntimeError: If yt-dlp validation fails
        """
        global _YT_DLP_VALIDATED
        
        try:
            logger.info(f"Validating yt-dlp at: {self.yt_dlp_path}")
            
            # Test yt-dlp availability
            result = subprocess.run(
                [self.yt_dlp_path, "--version"],
                capture_output=True,
                text=True,
                timeout=10  # Fast timeout for fail-fast
            )
            
            if result.returncode != 0:
                raise RuntimeError(
                    f"YT-DLP ERROR: yt-dlp command failed. "
                    f"Command: {self.yt_dlp_path} --version. "
                    f"Return code: {result.returncode}. "
                    f"Error: {result.stderr}"
                )
            
            version = result.stdout.strip()
            if not version:
                raise RuntimeError(
                    f"YT-DLP ERROR: yt-dlp returned empty version string. "
                    f"This indicates a broken installation."
                )
            
            _YT_DLP_VALIDATED = True
            logger.info(f"yt-dlp validation PASSED: {version}")
            
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"YT-DLP ERROR: yt-dlp command timed out. "
                f"This indicates yt-dlp is not responsive. "
                f"Command: {self.yt_dlp_path} --version"
            ) from None
        except FileNotFoundError:
            raise RuntimeError(
                f"YT-DLP ERROR: yt-dlp executable not found. "
                f"Path: {self.yt_dlp_path}. "
                f"Please install yt-dlp: pip install yt-dlp"
            ) from None
        except Exception as e:
            raise RuntimeError(
                f"YT-DLP ERROR: Unexpected error during yt-dlp validation. "
                f"Error: {e}"
            ) from e
    
    def validate_channel_url(self, channel_url: str) -> str:
        """
        Validate and normalize YouTube channel URL (fail-fast).
        
        Args:
            channel_url: Raw channel URL
            
        Returns:
            str: Normalized channel URL
            
        Raises:
            ValueError: If URL validation fails (fail-loud)
        """
        if not channel_url or not isinstance(channel_url, str):
            raise ValueError(
                f"CHANNEL URL ERROR: Channel URL is required and must be non-empty string. "
                f"Got: {channel_url} (type: {type(channel_url)})"
            )
        
        # Strip whitespace
        channel_url = channel_url.strip()
        
        if not channel_url:
            raise ValueError(
                f"CHANNEL URL ERROR: Channel URL cannot be empty or whitespace-only. "
                f"Provide a valid YouTube channel URL."
            )
        
        # Ensure HTTPS
        if channel_url.startswith("http://"):
            channel_url = channel_url.replace("http://", "https://", 1)
        elif not channel_url.startswith("https://"):
            channel_url = f"https://{channel_url}"
        
        # Validate YouTube domain
        parsed = urlparse(channel_url)
        if parsed.netloc not in ["youtube.com", "www.youtube.com", "m.youtube.com"]:
            raise ValueError(
                f"CHANNEL URL ERROR: Invalid YouTube domain. "
                f"Expected youtube.com or www.youtube.com, got: {parsed.netloc}. "
                f"URL: {channel_url}"
            )
        
        # Validate channel path patterns
        valid_patterns = [
            r"^/channel/[A-Za-z0-9_-]{10,}$",  # /channel/UC... (at least 10 chars)
            r"^/c/[A-Za-z0-9_-]+$",            # /c/channel_name
            r"^/user/[A-Za-z0-9_-]+$",         # /user/username
            r"^/@[A-Za-z0-9_.-]+$",            # /@handle
        ]
        
        path = parsed.path
        if not any(re.match(pattern, path) for pattern in valid_patterns):
            raise ValueError(
                f"CHANNEL URL ERROR: Invalid YouTube channel URL format. "
                f"Expected formats: /channel/ID, /c/name, /user/name, /@handle. "
                f"Got path: {path}. Full URL: {channel_url}"
            )
        
        # Normalize to www.youtube.com
        if parsed.netloc != "www.youtube.com":
            channel_url = channel_url.replace(parsed.netloc, "www.youtube.com")
        
        logger.debug(f"Channel URL validated and normalized: {channel_url}")
        return channel_url
    
    @rate_limit("youtube")
    @with_standard_error_handling("extract_channel_info")
    def extract_channel_info(self, channel_url: str) -> ChannelInfo:
        """
        Extract basic channel information (fail-safely).
        
        Args:
            channel_url: YouTube channel URL
            
        Returns:
            ChannelInfo: Channel metadata
            
        Raises:
            RuntimeError: If channel info extraction fails
        """
        # Validate URL first (fail-fast)
        normalized_url = self.validate_channel_url(channel_url)
        
        try:
            logger.info(f"Extracting channel info for: {normalized_url}")
            
            # Build yt-dlp command
            cmd = [
                self.yt_dlp_path,
                "--quiet",
                "--no-warnings",
                "--dump-json",
                "--flat-playlist",
                "--playlist-items", "1",  # Just get channel info, not all videos
                normalized_url
            ]
            
            logger.debug(f"Running command: {' '.join(cmd)}")
            
            # Execute with timeout
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60  # 1 minute timeout
            )
            
            if result.returncode != 0:
                raise RuntimeError(
                    f"CHANNEL EXTRACTION ERROR: yt-dlp failed to extract channel info. "
                    f"URL: {normalized_url}. "
                    f"Return code: {result.returncode}. "
                    f"Error: {result.stderr.strip()}"
                )
            
            if not result.stdout.strip():
                raise RuntimeError(
                    f"CHANNEL EXTRACTION ERROR: yt-dlp returned empty output. "
                    f"URL may be invalid or channel may not exist: {normalized_url}"
                )
            
            # Parse JSON output
            try:
                data = json.loads(result.stdout.strip().split('\n')[0])  # First line should be channel info
            except (json.JSONDecodeError, IndexError) as e:
                raise RuntimeError(
                    f"CHANNEL EXTRACTION ERROR: Failed to parse yt-dlp output as JSON. "
                    f"URL: {normalized_url}. "
                    f"Output: {result.stdout[:200]}... "
                    f"Error: {e}"
                ) from e
            
            # Extract channel information with validation
            channel_info = ChannelInfo(
                channel_id=data.get("channel_id", ""),
                channel_url=normalized_url,
                title=data.get("channel", data.get("uploader", "Unknown Channel")),
                description=data.get("description"),
                subscriber_count=data.get("subscriber_count"),
                video_count=data.get("playlist_count"),
                playlist_id=data.get("id")
            )
            
            logger.info(f"Channel info extracted successfully: {channel_info.title}")
            return channel_info
            
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"CHANNEL EXTRACTION ERROR: yt-dlp timed out extracting channel info. "
                f"URL: {normalized_url}. "
                f"Channel may be very large or network is slow."
            ) from None
        except Exception as e:
            logger.error(f"Channel info extraction failed: {e}")
            raise


def validate_channel_discovery_module():
    """
    Validate channel discovery module (fail-fast on import).
    
    Raises:
        RuntimeError: If module validation fails
    """
    global _MODULE_VALIDATED
    
    try:
        # Test yt-dlp availability
        discovery = YouTubeChannelDiscovery()
        
        # Test URL validation
        test_url = "https://www.youtube.com/@testchannel"
        normalized = discovery.validate_channel_url(test_url)
        if not normalized.startswith("https://www.youtube.com/"):
            raise RuntimeError("URL validation failed")
        
        _MODULE_VALIDATED = True
        logger.info("Channel discovery module validation PASSED")
        return True
        
    except Exception as e:
        logger.error(f"Channel discovery module validation FAILED: {e}")
        raise


# Run validation on import (fail-fast)
validate_channel_discovery_module()