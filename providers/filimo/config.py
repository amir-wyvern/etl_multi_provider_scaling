#!/usr/bin/env python3
"""
Filimo Provider Configuration
Specific configuration for Filimo ETL pipeline.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List
from core.base_pipeline import BaseConfig
from dotenv import load_dotenv

# Load Filimo-specific environment variables
load_dotenv('.env.filimo')


@dataclass
class FilimoSourceConfig:
    """Filimo source data configuration"""
    filimo_data_file: str = None
    imported_before_file: str = None
    movie_uid_exclusions_file: str = None
    parent_id_exclusions_file: str = None
    imdb_data_file: str = None
    
    # SFTP Configuration
    sftp_enabled: bool = False
    sftp_host: str = None
    sftp_port: int = None
    sftp_username: str = None
    sftp_password: str = None
    sftp_remote_dir: str = None
    
    # API Configuration  
    api_enabled: bool = False
    metabase_api_url: str = None
    api_key: str = None
    
    def __post_init__(self):
        # Load from environment variables - ALL REQUIRED
        self.filimo_data_file = self._get_required_env("FILIMO_DATA_FILE")
        self.imported_before_file = self._get_required_env("FILIMO_IMPORTED_BEFORE_FILE")
        self.movie_uid_exclusions_file = self._get_required_env("FILIMO_MOVIE_UID_FILE")
        self.parent_id_exclusions_file = self._get_required_env("FILIMO_PARENT_ID_FILE")
        self.imdb_data_file = self._get_required_env("FILIMO_IMDB_DATA_FILE")
        
        self.sftp_host = self._get_required_env("FILIMO_SFTP_HOST")
        self.sftp_port = int(self._get_required_env("FILIMO_SFTP_PORT"))
        self.sftp_username = self._get_required_env("FILIMO_SFTP_USER")
        self.sftp_password = self._get_required_env("FILIMO_SFTP_PASS")
        self.sftp_remote_dir = self._get_required_env("FILIMO_SFTP_REMOTE_DIR")
        
        self.metabase_api_url = self._get_required_env("FILIMO_API_URL")
        self.api_key = self._get_required_env("FILIMO_API_KEY")
    
    def _get_required_env(self, var_name: str) -> str:
        """Get required environment variable or raise error if missing"""
        value = os.getenv(var_name)
        if value is None:
            raise ValueError(f"Required environment variable '{var_name}' is not set. Please check your .env.filimo file.")
        return value


@dataclass
class FilimoTransformConfig:
    """Filimo transformation configuration"""
    # Filtering options
    preserve_parent_records: bool = True
    remove_iranian_documentaries: bool = True
    
    # Excluded title keywords
    excluded_title_keywords: List[str] = field(default_factory=lambda: [
        "مخصوص ناشنوایان",    # For deaf people
        "مخصوص نابینایان",     # For blind people  
        "موسیقی متن",          # Soundtrack
        "آلبوم موسیقی",        # Music album
        "پشت صحنه",           # Behind the scenes
        "آنچه خواهید دید"      # What you will see
    ])
    
    # Business rules
    movie_type_value: str = "movie"
    series_type_value: str = "serie"
    
    # Output format settings
    fixed_values: dict = field(default_factory=lambda: {
        "fileName": "پخش",
        "accessType": "free", 
        "price": 0,
        "quality": "خودکار",
        "sourceId": 8,
        "movie_type_id": 1,
        "series_type_id": 2
    })


@dataclass
class FilimoOutputConfig:
    """Filimo output configuration"""
    # Intermediate files
    save_intermediate_files: bool = True
    intermediate_movies_file: str = "filimo_new_movies.csv"
    intermediate_series_file: str = "filimo_new_series.csv"
    
    # Final output files
    final_movies_file: str = "filimo_final_movies.csv"
    final_series_file: str = "filimo_final_series.csv"
    
    # File format options
    csv_na_rep: str = "NAN"
    csv_index: bool = False
    
    def get_intermediate_movies_path(self, base_dir: Path) -> Path:
        return base_dir / self.intermediate_movies_file
    
    def get_intermediate_series_path(self, base_dir: Path) -> Path:
        return base_dir / self.intermediate_series_file
    
    def get_final_movies_path(self, base_dir: Path) -> Path:
        return base_dir / self.final_movies_file
    
    def get_final_series_path(self, base_dir: Path) -> Path:
        return base_dir / self.final_series_file


@dataclass
class FilimoConfig(BaseConfig):
    """Complete Filimo ETL pipeline configuration"""
    source: FilimoSourceConfig = field(default_factory=FilimoSourceConfig)
    transform: FilimoTransformConfig = field(default_factory=FilimoTransformConfig)
    output: FilimoOutputConfig = field(default_factory=FilimoOutputConfig)
    
    def __post_init__(self):
        super().__post_init__()
        # Set provider info
        self.provider_name = "filimo"
        self.provider_version = "2.0.0"
    
    def get_source_paths(self) -> dict:
        """Get all source file paths"""
        return {
            "filimo_data": self.base_dir / self.source.filimo_data_file,
            "imported_before": self.base_dir / self.source.imported_before_file,
            "movie_uid_exclusions": self.base_dir / self.source.movie_uid_exclusions_file,
            "parent_id_exclusions": self.base_dir / self.source.parent_id_exclusions_file,
            "imdb_data": self.base_dir / self.source.imdb_data_file,
        }
    
    def validate_filimo_specific(self) -> List[str]:
        """Validate Filimo-specific configuration"""
        issues = []
        
        # Check required source files exist
        source_paths = self.get_source_paths()
        required_files = ["filimo_data", "imported_before", "imdb_data"]
        
        for file_key in required_files:
            if not source_paths[file_key].exists():
                issues.append(f"Required Filimo source file not found: {source_paths[file_key]}")
        
        # Validate business rules
        if not self.transform.excluded_title_keywords:
            issues.append("No excluded title keywords configured for Filimo")
        
        return issues


# Preset configurations for different environments
class FilimoConfigPresets:
    """Predefined Filimo configuration presets"""
    
    @staticmethod
    def development() -> FilimoConfig:
        """Development configuration"""
        config = FilimoConfig()
        config.continue_on_error = True
        config.validate_data = False
        config.log_level = "DEBUG"
        config.log_to_file = True
        return config
    
    @staticmethod
    def production() -> FilimoConfig:
        """Production configuration"""
        config = FilimoConfig()
        config.source.sftp_enabled = True
        config.source.api_enabled = True
        config.continue_on_error = False
        config.validate_data = True
        config.log_level = "INFO"
        config.log_to_file = True
        return config
    
    @staticmethod
    def testing() -> FilimoConfig:
        """Testing configuration"""
        config = FilimoConfig()
        config.base_dir = Path("test_output")
        config.continue_on_error = False
        config.validate_data = True
        config.output.save_intermediate_files = False
        config.log_level = "DEBUG"
        return config
    
    @staticmethod
    def performance() -> FilimoConfig:
        """High-performance configuration"""
        config = FilimoConfig()
        config.output.save_intermediate_files = False
        config.validate_data = False
        config.log_level = "WARNING"
        config.log_to_file = False
        return config
