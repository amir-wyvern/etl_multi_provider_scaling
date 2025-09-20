#!/usr/bin/env python3
"""
Filimo Enrichment Configuration
Configuration for IMDB and translation enrichment services.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
from core.base_pipeline import BaseConfig

try:
    from dotenv import load_dotenv
    load_dotenv('.env.filimo')
except ImportError:
    pass

@dataclass
class EnrichmentSourceConfig:
    """Enrichment data source configuration"""
    # IMDB API Configuration
    imdb_api_url: str = "https://api.imdbapi.dev"
    imdb_timeout: int = 10
    imdb_retry_attempts: int = 3
    
    # Translation Service Configuration
    translation_api_key: str = None
    translation_model: str = "gpt-3.5-turbo"
    translation_timeout: int = 30
    
    # Database Service Configuration (if using Metabase)
    database_api_key: str = None
    database_id: int = 2
    
    def __post_init__(self):
        self.translation_api_key = os.getenv("OPENAI_API_KEY")
        self.database_api_key = os.getenv("DATABASE_API_KEY")

@dataclass
class EnrichmentProcessingConfig:
    """Enrichment processing configuration"""
    # Rate Limiting
    api_delay_seconds: float = 0.5
    batch_size: int = 100
    max_parallel_workers: int = 4
    
    # Caching
    enable_caching: bool = True
    cache_ttl_hours: int = 24
    
    # Processing Strategy
    enrichment_strategy: str = "dual_branch"  # "imdb_only", "title_only", "dual_branch"
    confidence_threshold: float = 0.7
    
    # Content Type Settings
    movie_types: List[str] = field(default_factory=lambda: ["movie", "tvMovie"])
    series_types: List[str] = field(default_factory=lambda: ["tvSeries", "tvMiniSeries"])
    
    # Translation Settings
    auto_translate: bool = True
    fallback_to_original: bool = True
    translation_quality: str = "high"  # "fast", "balanced", "high"

@dataclass
class EnrichmentTranslationConfig:
    """Translation configuration for OpenAI API"""
    # API Configuration
    openai_api_key: Optional[str] = None
    model: str = "gpt-4o-mini"
    max_tokens: int = 100
    temperature: float = 0.3
    timeout: int = 30
    
    # Rate Limiting
    max_requests_per_minute: int = 50
    
    # Translation Settings
    enable_caching: bool = True
    batch_delay: float = 1.0

@dataclass
class EnrichmentOutputConfig:
    """Enrichment output configuration"""
    # Output files
    enriched_movies_file: str = "filimo_enriched_movies.csv"
    enriched_series_file: str = "filimo_enriched_series.csv"
    enrichment_metadata_file: str = "enrichment_metadata.json"
    enriched_series_with_db_file: str = "filimo_enriched_series_with_db.csv"
    enriched_movies_with_db_file: str = "filimo_enriched_movies_with_db.csv"
    # Output format
    include_confidence_scores: bool = True
    include_processing_metadata: bool = True
    save_intermediate_results: bool = False
    
    # Quality reporting
    generate_quality_report: bool = True
    quality_report_file: str = "enrichment_quality_report.json"
    
    def get_enriched_movies_path(self, base_dir) -> str:
        """Get path for enriched movies file"""
        # Save CSV files to results directory
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        return str(results_dir / self.enriched_movies_file)
    
    def get_enriched_series_path(self, base_dir) -> str:
        """Get path for enriched series file"""
        # Save CSV files to results directory
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        return str(results_dir / self.enriched_series_file)
    
    def get_metadata_path(self, base_dir) -> str:
        """Get path for enrichment metadata file"""
        # Keep JSON metadata in assets directory
        return str(base_dir / self.enrichment_metadata_file)
    
    def get_enriched_movies_with_db_path(self, base_dir) -> str:
        """Get path for enriched movies with database file"""
        # Save CSV files to results directory
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        return str(results_dir / self.enriched_movies_with_db_file)
    
    def get_enriched_series_with_db_path(self, base_dir) -> str:
        """Get path for enriched series with database file"""
        # Save CSV files to results directory
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        return str(results_dir / self.enriched_series_with_db_file)

    def get_quality_report_path(self, base_dir) -> str:
        """Get path for quality report file"""
        # Save quality report to results directory
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        return str(results_dir / self.quality_report_file)

@dataclass
class FilimoEnrichmentConfig(BaseConfig):
    """Complete Filimo enrichment configuration"""
    source: EnrichmentSourceConfig = field(default_factory=EnrichmentSourceConfig)
    processing: EnrichmentProcessingConfig = field(default_factory=EnrichmentProcessingConfig)
    translation: EnrichmentTranslationConfig = field(default_factory=EnrichmentTranslationConfig)
    output: EnrichmentOutputConfig = field(default_factory=EnrichmentOutputConfig)
    
    def __post_init__(self):
        super().__post_init__()
        self.provider_name = "filimo_enrichment"
        self.provider_version = "1.0.0"
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging for enrichment components"""
        import logging
        
        # Setup main enrichment logger
        logger = logging.getLogger("filimo_enrichment")
        logger.setLevel(getattr(logging, self.log_level.upper()))
        
        # Only add handlers if they don't already exist
        if not logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(
                f'%(asctime)s - filimo_enrichment - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
            # File handler (if enabled)
            if self.log_to_file:
                file_handler = logging.FileHandler(self.log_path)
                file_handler.setLevel(logging.DEBUG)
                file_formatter = logging.Formatter(
                    f'%(asctime)s - filimo_enrichment - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
                )
                file_handler.setFormatter(file_formatter)
                logger.addHandler(file_handler)
    
    def validate_enrichment_config(self) -> List[str]:
        """Validate enrichment-specific configuration"""
        issues = []
        
        # Check required API keys
        if not self.source.translation_api_key:
            issues.append("Translation API key not configured")
        
        # Validate processing settings
        if self.processing.batch_size <= 0:
            issues.append("Batch size must be positive")
        
        if self.processing.max_parallel_workers <= 0:
            issues.append("Max parallel workers must be positive")
        
        if not 0 <= self.processing.confidence_threshold <= 1:
            issues.append("Confidence threshold must be between 0 and 1")
        
        return issues

# Preset configurations for different environments
class EnrichmentConfigPresets:
    """Predefined enrichment configuration presets"""
    
    @staticmethod
    def development() -> FilimoEnrichmentConfig:
        """Development configuration - fast processing, minimal caching"""
        config = FilimoEnrichmentConfig()
        config.processing.batch_size = 50
        config.processing.max_parallel_workers = 2
        config.processing.api_delay_seconds = 0.2
        config.processing.enable_caching = False
        config.output.save_intermediate_results = True
        config.log_level = "DEBUG"
        return config
    
    @staticmethod
    def production() -> FilimoEnrichmentConfig:
        """Production configuration - optimized for reliability"""
        config = FilimoEnrichmentConfig()
        config.processing.batch_size = 100
        config.processing.max_parallel_workers = 4
        config.processing.api_delay_seconds = 0.5
        config.processing.enable_caching = True
        config.processing.confidence_threshold = 0.8
        config.output.save_intermediate_results = False
        config.log_level = "INFO"
        return config
    
    @staticmethod
    def performance() -> FilimoEnrichmentConfig:
        """High-performance configuration - maximum speed"""
        config = FilimoEnrichmentConfig()
        config.processing.batch_size = 200
        config.processing.max_parallel_workers = 8
        config.processing.api_delay_seconds = 0.1
        config.processing.enable_caching = True
        config.processing.confidence_threshold = 0.6
        config.output.save_intermediate_results = False
        config.log_level = "WARNING"
        return config
