#!/usr/bin/env python3
"""
Netflix Provider Configuration (Example)
Demonstrates how to add a new provider to the multi-provider ETL framework.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List
from core.base_pipeline import BaseConfig


@dataclass
class NetflixSourceConfig:
    """Netflix source data configuration"""
    netflix_data_file: str = "netflix_data.csv"
    content_ratings_file: str = "content_ratings.csv"
    
    # API Configuration
    api_enabled: bool = False
    netflix_api_url: str = "https://api.netflix.com/catalog"
    api_key: str = None
    
    def __post_init__(self):
        self.api_key = os.getenv("NETFLIX_API_KEY", self.api_key)


@dataclass
class NetflixTransformConfig:
    """Netflix transformation configuration"""
    # Netflix-specific filtering
    exclude_kids_content: bool = False
    min_rating: float = 0.0
    
    # Content categories
    movie_type_value: str = "Movie"
    series_type_value: str = "TV Show"
    
    # Output format
    fixed_values: dict = field(default_factory=lambda: {
        "platform": "netflix",
        "subscription_required": True,
        "hd_available": True,
        "source_id": 1
    })


@dataclass
class NetflixOutputConfig:
    """Netflix output configuration"""
    final_content_file: str = "netflix_final_content.csv"
    content_stats_file: str = "netflix_content_stats.json"
    
    def get_final_content_path(self, base_dir: Path) -> Path:
        return base_dir / self.final_content_file
    
    def get_stats_path(self, base_dir: Path) -> Path:
        return base_dir / self.content_stats_file


@dataclass
class NetflixConfig(BaseConfig):
    """Complete Netflix ETL pipeline configuration"""
    source: NetflixSourceConfig = field(default_factory=NetflixSourceConfig)
    transform: NetflixTransformConfig = field(default_factory=NetflixTransformConfig)
    output: NetflixOutputConfig = field(default_factory=NetflixOutputConfig)
    
    def __post_init__(self):
        super().__post_init__()
        self.provider_name = "netflix"
        self.provider_version = "1.0.0"
    
    def get_source_paths(self) -> dict:
        """Get all source file paths"""
        return {
            "netflix_data": self.base_dir / self.source.netflix_data_file,
            "content_ratings": self.base_dir / self.source.content_ratings_file,
        }


class NetflixConfigPresets:
    """Predefined Netflix configuration presets"""
    
    @staticmethod
    def development() -> NetflixConfig:
        """Development configuration"""
        config = NetflixConfig()
        config.continue_on_error = True
        config.validate_data = False
        return config
    
    @staticmethod
    def production() -> NetflixConfig:
        """Production configuration"""
        config = NetflixConfig()
        config.source.api_enabled = True
        config.continue_on_error = False
        config.validate_data = True
        return config
