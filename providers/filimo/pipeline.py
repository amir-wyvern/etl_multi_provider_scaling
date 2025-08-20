#!/usr/bin/env python3
"""
Filimo Pipeline
Complete ETL pipeline implementation for Filimo provider.
"""

import pandas as pd
from typing import Dict, Any
from core.base_pipeline import BasePipeline
from .config import FilimoConfig
from .extractors import FilimoCSVExtractor, FilimoSFTPExtractor, FilimoAPIExtractor
from .transformers import (
    FilimoDuplicateRemovalTransformer, FilimoTitleFilterTransformer,
    FilimoExclusionListTransformer, FilimoDocumentaryFilterTransformer,
    FilimoDataSplitterTransformer, FilimoMovieTransformer, FilimoSeriesTransformer
)
from .loaders import FilimoCSVLoader, FilimoMultiFormatLoader
from .validators import FilimoDataValidator, FilimoQualityValidator


class FilimoPipeline(BasePipeline):
    """Complete ETL pipeline for Filimo provider"""
    
    def __init__(self, config: FilimoConfig):
        super().__init__(config)
        self.config = config  # Type hint for IDE
    
    def _initialize_components(self):
        """Initialize Filimo-specific components"""
        # Initialize extractors
        if self.config.source.sftp_enabled:
            self.extractors["primary"] = FilimoSFTPExtractor(self.config, self.metrics)
        elif self.config.source.api_enabled:
            self.extractors["primary"] = FilimoAPIExtractor(self.config, self.metrics)
        else:
            self.extractors["primary"] = FilimoCSVExtractor(self.config, self.metrics)
        
        # Initialize transformers
        self.transformers["duplicate_removal"] = FilimoDuplicateRemovalTransformer(self.config, self.metrics)
        self.transformers["title_filter"] = FilimoTitleFilterTransformer(self.config, self.metrics)
        self.transformers["exclusion_lists"] = FilimoExclusionListTransformer(self.config, self.metrics)
        self.transformers["documentary_filter"] = FilimoDocumentaryFilterTransformer(self.config, self.metrics)
        self.transformers["data_splitter"] = FilimoDataSplitterTransformer(self.config, self.metrics)
        self.transformers["movie_transformer"] = FilimoMovieTransformer(self.config, self.metrics)
        self.transformers["series_transformer"] = FilimoSeriesTransformer(self.config, self.metrics)
        
        # Initialize loaders
        self.loaders["csv"] = FilimoCSVLoader(self.config, self.metrics)
        self.loaders["multi_format"] = FilimoMultiFormatLoader(self.config, self.metrics)
        
        # Initialize validators
        self.validators["data"] = FilimoDataValidator(self.config, self.metrics)
        self.validators["quality"] = FilimoQualityValidator(self.config, self.metrics)
    
    def extract_phase(self) -> Dict[str, pd.DataFrame]:
        """Extract phase for Filimo data"""
        self.logger.info("🔄 Starting Filimo EXTRACT phase")
        
        extracted_data = {}
        extractor = self.extractors["primary"]
        
        # Required sources
        required_sources = ["filimo_data", "imported_before", "imdb_data"]
        for source_name in required_sources:
            try:
                df = extractor.extract(source_name)
                extracted_data[source_name] = df
                self.logger.info(f"✅ Extracted {len(df)} rows from {source_name}")
                
            except Exception as e:
                error_msg = f"Failed to extract {source_name}: {e}"
                self.logger.error(error_msg)
                self.metrics.record_error(error_msg, "extract")
                
                if not self.config.continue_on_error:
                    raise
        
        # Optional sources
        optional_sources = ["movie_uid_exclusions", "parent_id_exclusions"]
        for source_name in optional_sources:
            try:
                df = extractor.extract(source_name)
                extracted_data[source_name] = df
                self.logger.info(f"✅ Extracted {len(df)} rows from {source_name}")
            except Exception as e:
                self.logger.warning(f"Optional source {source_name} not available: {e}")
                extracted_data[source_name] = None
                self.metrics.record_warning(f"Optional source not found: {source_name}", "extract")
        
        return extracted_data
    
    def transform_phase(self, extracted_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Transform phase for Filimo data"""
        self.logger.info("🔄 Starting Filimo TRANSFORM phase")
        
        df = extracted_data["filimo_data"]
        
        # Step 1: Remove duplicates while preserving parents
        df = self.transformers["duplicate_removal"].transform(
            df, 
            imported_before_df=extracted_data["imported_before"]
        )
        self.logger.info(f"After duplicate removal: {len(df)} rows")
        
        # Step 2: Filter excluded titles
        df = self.transformers["title_filter"].transform(df)
        self.logger.info(f"After title filtering: {len(df)} rows")
        
        # Step 3: Apply exclusion lists
        df = self.transformers["exclusion_lists"].transform(
            df,
            movie_uid_df=extracted_data["movie_uid_exclusions"],
            parent_id_df=extracted_data["parent_id_exclusions"]
        )
        self.logger.info(f"After exclusion lists: {len(df)} rows")
        
        # Step 4: Filter documentaries
        df = self.transformers["documentary_filter"].transform(df)
        self.logger.info(f"After documentary filtering: {len(df)} rows")
        
        # Step 5: Split into movies and series
        movies_df, series_df = self.transformers["data_splitter"].transform(df)
        self.logger.info(f"Split result: {len(movies_df)} movies, {len(series_df)} series")
        
        # Step 6: Transform to final format
        final_movies_df = self.transformers["movie_transformer"].transform(
            movies_df,
            imdb_df=extracted_data["imdb_data"]
        )
        final_series_df = self.transformers["series_transformer"].transform(
            series_df,
            imdb_df=extracted_data["imdb_data"]
        )
        
        self.logger.info(f"Final transformation: {len(final_movies_df)} movies, {len(final_series_df)} series")
        
        return {
            "intermediate_movies": movies_df,
            "intermediate_series": series_df,
            "final_movies": final_movies_df,
            "final_series": final_series_df,
        }
    
    def load_phase(self, transformed_data: Dict[str, pd.DataFrame]):
        """Load phase for Filimo data"""
        self.logger.info("🔄 Starting Filimo LOAD phase")
        
        loader = self.loaders["csv"]
        
        # Save intermediate files if configured
        if self.config.output.save_intermediate_files:
            loader.load(transformed_data["intermediate_movies"], "intermediate_movies")
            loader.load(transformed_data["intermediate_series"], "intermediate_series")
        
        # Save final files
        loader.load(transformed_data["final_movies"], "final_movies")
        loader.load(transformed_data["final_series"], "final_series")
    
    def _generate_result(self, transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Generate Filimo pipeline result summary"""
        return {
            "status": "success",
            "provider": {
                "name": self.config.provider_name,
                "version": self.config.provider_version,
                "duration": self.metrics.metrics["execution"]["duration_seconds"]
            },
            "results": {
                "final_movies": len(transformed_data["final_movies"]),
                "final_series": len(transformed_data["final_series"]),
                "intermediate_movies": len(transformed_data["intermediate_movies"]) if self.config.output.save_intermediate_files else None,
                "intermediate_series": len(transformed_data["intermediate_series"]) if self.config.output.save_intermediate_files else None,
            },
            "output_files": {
                "final_movies": str(self.config.output.get_final_movies_path(self.config.base_dir)),
                "final_series": str(self.config.output.get_final_series_path(self.config.base_dir)),
                "intermediate_movies": str(self.config.output.get_intermediate_movies_path(self.config.base_dir)) if self.config.output.save_intermediate_files else None,
                "intermediate_series": str(self.config.output.get_intermediate_series_path(self.config.base_dir)) if self.config.output.save_intermediate_files else None,
                "metrics": str(self.config.metrics_path) if self.config.save_metrics else None,
            },
            "metrics_summary": {
                "total_extracted": sum(op.get("count", 0) for op in self.metrics.metrics.get("phases", {}).get("extract", {}).get("operations", {}).values()),
                "total_loaded": sum(op.get("count", 0) for op in self.metrics.metrics.get("phases", {}).get("load", {}).get("operations", {}).values()),
                "errors": len(self.metrics.metrics["errors"]),
                "warnings": len(self.metrics.metrics["warnings"])
            }
        }
