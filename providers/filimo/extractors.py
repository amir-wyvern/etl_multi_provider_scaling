#!/usr/bin/env python3
"""
Filimo Extractors
Data extraction components specific to Filimo provider.
"""

import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional
from core.base_pipeline import BaseExtractor, BaseMetrics
from .config import FilimoConfig


class FilimoCSVExtractor(BaseExtractor):
    """Extract data from CSV files for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config  # Type hint for IDE
    
    def extract(self, source_name: str, **kwargs) -> pd.DataFrame:
        """Extract data from CSV file"""
        source_paths = self.config.get_source_paths()
        
        if source_name not in source_paths:
            raise ValueError(f"Unknown Filimo source: {source_name}")
        
        file_path = source_paths[source_name]
        self.logger.info(f"Extracting {source_name} from: {file_path}")
        
        if not file_path.exists():
            raise FileNotFoundError(f"Filimo {source_name} file not found: {file_path}")
        
        start_time = datetime.now()
        df = pd.read_csv(file_path)
        duration = (datetime.now() - start_time).total_seconds()
        
        self._record_extraction(source_name, len(df), duration)
        self.logger.info(f"Extracted {len(df)} rows from {source_name}")
        
        return df


class FilimoSFTPExtractor(BaseExtractor):
    """Extract data from SFTP server for Filimo (future implementation)"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def extract(self, source_name: str, **kwargs) -> pd.DataFrame:
        """Extract data from SFTP server"""
        # TODO: Implement SFTP extraction
        self.logger.warning("SFTP extraction not yet implemented for Filimo")
        
        # Fallback to CSV extraction for now
        csv_extractor = FilimoCSVExtractor(self.config, self.metrics)
        return csv_extractor.extract(source_name, **kwargs)


class FilimoAPIExtractor(BaseExtractor):
    """Extract data from Metabase API for Filimo (future implementation)"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def extract(self, source_name: str, **kwargs) -> pd.DataFrame:
        """Extract data from Metabase API"""
        # TODO: Implement API extraction
        self.logger.warning("API extraction not yet implemented for Filimo")
        
        # Fallback to CSV extraction for now
        csv_extractor = FilimoCSVExtractor(self.config, self.metrics)
        return csv_extractor.extract(source_name, **kwargs)
