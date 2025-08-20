#!/usr/bin/env python3
"""
Filimo Loaders
Data loading components specific to Filimo provider.
"""

import pandas as pd
from pathlib import Path
from core.base_pipeline import BaseLoader, BaseMetrics
from .config import FilimoConfig


class FilimoCSVLoader(BaseLoader):
    """Load data to CSV files for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def load(self, df: pd.DataFrame, target_name: str, **kwargs):
        """Load DataFrame to CSV file"""
        file_path = self._get_target_path(target_name)
        
        self.logger.info(f"Loading {target_name} to: {file_path}")
        
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to CSV
        df.to_csv(
            file_path, 
            index=self.config.output.csv_index, 
            na_rep=self.config.output.csv_na_rep
        )
        
        # Record metrics
        file_size = file_path.stat().st_size if file_path.exists() else 0
        self._record_load(target_name, len(df), file_size)
        
        self.logger.info(f"Loaded {len(df)} rows to {target_name}")
        return file_path
    
    def _get_target_path(self, target_name: str) -> Path:
        """Get target file path based on target name"""
        if target_name == "intermediate_movies":
            return self.config.output.get_intermediate_movies_path(self.config.base_dir)
        elif target_name == "intermediate_series":
            return self.config.output.get_intermediate_series_path(self.config.base_dir)
        elif target_name == "final_movies":
            return self.config.output.get_final_movies_path(self.config.base_dir)
        elif target_name == "final_series":
            return self.config.output.get_final_series_path(self.config.base_dir)
        else:
            # Generic path for custom targets
            return self.config.base_dir / f"{target_name}.csv"


class FilimoMultiFormatLoader(BaseLoader):
    """Load data to multiple formats for Filimo (future implementation)"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
        self.csv_loader = FilimoCSVLoader(config, metrics)
    
    def load(self, df: pd.DataFrame, target_name: str, **kwargs):
        """Load DataFrame to multiple formats"""
        formats = kwargs.get('formats', ['csv'])
        results = {}
        
        for format_type in formats:
            if format_type == 'csv':
                results['csv'] = self.csv_loader.load(df, target_name, **kwargs)
            elif format_type == 'json':
                results['json'] = self._load_json(df, target_name, **kwargs)
            elif format_type == 'parquet':
                results['parquet'] = self._load_parquet(df, target_name, **kwargs)
            else:
                self.logger.warning(f"Unsupported format: {format_type}")
        
        return results
    
    def _load_json(self, df: pd.DataFrame, target_name: str, **kwargs) -> Path:
        """Load to JSON format"""
        file_path = self.config.base_dir / f"{target_name}.json"
        df.to_json(file_path, orient='records', indent=2)
        self.logger.info(f"Loaded {len(df)} rows to JSON: {file_path}")
        return file_path
    
    def _load_parquet(self, df: pd.DataFrame, target_name: str, **kwargs) -> Path:
        """Load to Parquet format"""
        file_path = self.config.base_dir / f"{target_name}.parquet"
        df.to_parquet(file_path, index=False)
        self.logger.info(f"Loaded {len(df)} rows to Parquet: {file_path}")
        return file_path
