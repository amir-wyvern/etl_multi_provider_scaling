#!/usr/bin/env python3
"""
Enhanced Filimo Pipeline
Combines ETL processing with enrichment capabilities.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
from core.base_pipeline import BasePipeline
from .config import FilimoConfig
from .pipeline import FilimoPipeline
from .enrichment.config import FilimoEnrichmentConfig, EnrichmentConfigPresets
from .enrichment.extractors import IMDBEnrichmentExtractor, BatchEnrichmentExtractor, DatabaseEnrichmentExtractor
from .enrichment.transformers import EnrichmentDataTransformer, EnrichmentDataAggregator, DatabaseEnrichmentTransformer
from .enrichment.loaders import EnrichmentDataLoader, EnrichmentDataExporter

class EnhancedFilimoPipeline(BasePipeline):
    """Enhanced Filimo pipeline with ETL + Enrichment"""
    
    def __init__(self, etl_config: FilimoConfig, enrichment_config: FilimoEnrichmentConfig = None):
        # Initialize with ETL config as base
        super().__init__(etl_config)
        self.etl_config = etl_config
        
        # Initialize enrichment config
        if enrichment_config is None:
            enrichment_config = FilimoEnrichmentConfig()

        self.enrichment_config = enrichment_config
        
        # Initialize ETL pipeline
        self.etl_pipeline = FilimoPipeline(etl_config)
        
        # Initialize enrichment components
        self.enrichment_extractors = {}
        self.enrichment_transformers = {}
        self.enrichment_loaders = {}
        
        self._initialize_enrichment_components()
    
    def _initialize_components(self):
        """Initialize both ETL and enrichment components"""
        # Initialize ETL components (inherited from base)
        super()._initialize_components()
        
        # Initialize enrichment components only if config is available
        if hasattr(self, 'enrichment_config'):
            self._initialize_enrichment_components()
    
    def _initialize_enrichment_components(self):
        """Initialize enrichment-specific components"""
        # Enrichment extractors
        self.enrichment_extractors["imdb"] = IMDBEnrichmentExtractor(self.enrichment_config, self.metrics)
        self.enrichment_extractors["batch_imdb"] = BatchEnrichmentExtractor(self.enrichment_config, self.metrics)
        self.enrichment_extractors["database"] = DatabaseEnrichmentExtractor(self.enrichment_config, self.metrics)
        
        # Enrichment transformers
        self.enrichment_transformers["data_cleaner"] = EnrichmentDataTransformer(self.enrichment_config, self.metrics)
        self.enrichment_transformers["database_cleaner"] = DatabaseEnrichmentTransformer(self.enrichment_config, self.metrics)
        self.enrichment_transformers["aggregator"] = EnrichmentDataAggregator(self.enrichment_config, self.metrics)
        
        # Enrichment loaders
        self.enrichment_loaders["csv"] = EnrichmentDataLoader(self.enrichment_config, self.metrics)
        self.enrichment_loaders["exporter"] = EnrichmentDataExporter(self.enrichment_config, self.metrics)
    
    def run_etl_only(self) -> Dict[str, Any]:
        """Run only the ETL pipeline (without enrichment)"""
        self.logger.info("🔄 Running ETL pipeline only")
        return self.etl_pipeline.run()
    
    def run_enrichment_only(self, etl_results: Dict[str, Any]) -> Dict[str, Any]:
        """Run only the enrichment pipeline on ETL results"""
        self.logger.info("🔍 Running enrichment pipeline only")
        
        try:
            # Load the ETL results
            movies_file = etl_results["output_files"]["final_movies"]
            series_file = etl_results["output_files"]["final_series"]
            
            movies_df = pd.read_csv(movies_file)
            series_df = pd.read_csv(series_file)
            
            self.logger.info(f"Loaded ETL results: {len(movies_df)} movies, {len(series_df)} series")
            
            # Enrich movies
            self.logger.info("🎬 Enriching movies with IMDB data")
            self.metrics.start_phase("enrichment_movies")
            enriched_movies = self._enrich_content(movies_df, "movie")
            self.metrics.end_phase("enrichment_movies")
            
            # Enrich series
            self.logger.info("📺 Enriching series with IMDB data")
            self.metrics.start_phase("enrichment_series")
            enriched_series = self._enrich_content(series_df, "series")
            self.metrics.end_phase("enrichment_series")
            
            # self.logger.info("📺 Enriching series with Database data")
            # # self.metrics.start_phase("enrichment_series")
            # enriched_series = self._enrich_content(series_df, "series")
            # self.metrics.end_phase("enrichment_series")
            
            # Transform enriched data
            self.logger.info("🔄 Transforming enriched data")
            self.metrics.start_phase("enrichment_transform")
            final_movies = self._transform_enriched_data(enriched_movies, "movies")
            final_series = self._transform_enriched_data(enriched_series, "series")
            self.metrics.end_phase("enrichment_transform")
            
            # Load enriched data
            self.logger.info("💾 Loading enriched data")
            self.metrics.start_phase("enrichment_load")
            output_files = self._load_enriched_data(final_movies, final_series)
            self.metrics.end_phase("enrichment_load")
            
            return {
                "status": "success",
                "enriched_movies": len(final_movies),
                "enriched_series": len(final_series),
                "output_files": output_files,
                "enrichment_metrics": self._get_enrichment_metrics(final_movies, final_series)
            }
            
        except Exception as e:
            self.logger.error(f"Enrichment pipeline failed: {e}")
            raise
    
    
    def run_enrichment_only_with_db(self, etl_results: Dict[str, Any]) -> Dict[str, Any]:
        """Run only the enrichment pipeline on ETL results"""
        self.logger.info("🔍 Running enrichment pipeline only")
        
        try:
            # Load the ETL results
            movies_file = etl_results["output_files"]["final_movies"]
            series_file = etl_results["output_files"]["final_series"]
            
            movies_df = pd.read_csv(movies_file)
            series_df = pd.read_csv(series_file)
            
            self.logger.info(f"Loaded ETL results: {len(movies_df)} movies, {len(series_df)} series")
            
            # Enrich movies
            self.logger.info("🎬 Enriching movies with Database data")
            self.metrics.start_phase("enrichment_movies_with_db")
            enriched_movies = self._enrich_content_with_db(movies_df, "movie")
            self.metrics.end_phase("enrichment_movies_with_db")
            
            # Enrich series
            self.logger.info("📺 Enriching series with Database data")
            self.metrics.start_phase("enrichment_series_with_db")
            enriched_series = self._enrich_content_with_db(series_df, "series")
            self.metrics.end_phase("enrichment_series_with_db")
            
            # Transform enriched data
            self.logger.info("🔄 Transforming database-enriched data")
            self.metrics.start_phase("enrichment_transform")
            final_movies = self._transform_database_enriched_data(enriched_movies, "movies")
            final_series = self._transform_database_enriched_data(enriched_series, "series")
            self.metrics.end_phase("enrichment_transform")
            
            # Load enriched data
            self.logger.info("💾 Loading enriched data")
            self.metrics.start_phase("enrichment_load")
            output_files = self._load_enriched_data_with_db(final_movies, final_series)
            self.metrics.end_phase("enrichment_load")
            
            return {
                "status": "success",
                "enriched_movies": len(final_movies),
                "enriched_series": len(final_series),
                "output_files": output_files,
                "enrichment_metrics": self._get_enrichment_metrics(final_movies, final_series)
            }
            
        except Exception as e:
            self.logger.error(f"Enrichment pipeline failed: {e}")
            raise
    
    def run_complete(self) -> Dict[str, Any]:
        """Run complete ETL + Enrichment pipeline"""
        self.logger.info("🚀 Starting Enhanced Filimo Pipeline (ETL + Enrichment)")
        
        try:
            # Phase 1: Run ETL pipeline
            self.logger.info("📥 Phase 1: ETL Processing")
            etl_results = self.run_etl_only()
            
            # Phase 2: Run enrichment
            self.logger.info("🔍 Phase 2: Data Enrichment")
            enrichment_results = self.run_enrichment_only(etl_results)
            
            # Combine results
            combined_results = {
                "status": "success",
                "pipeline_type": "enhanced_etl_enrichment",
                "etl_results": etl_results,
                "enrichment_results": enrichment_results,
                "total_processing_time": self.metrics.metrics["execution"]["duration_seconds"],
                "summary": {
                    "original_movies": etl_results["results"]["final_movies"],
                    "original_series": etl_results["results"]["final_series"],
                    "enriched_movies": enrichment_results["enriched_movies"],
                    "enriched_series": enrichment_results["enriched_series"],
                    "enrichment_success_rate": self._calculate_overall_success_rate(enrichment_results)
                }
            }
            
            self.logger.info("✅ Enhanced Filimo Pipeline completed successfully!")
            self.logger.info(f"📊 Final Summary:")
            self.logger.info(f"  - Movies: {combined_results['summary']['original_movies']} -> {combined_results['summary']['enriched_movies']}")
            self.logger.info(f"  - Series: {combined_results['summary']['original_series']} -> {combined_results['summary']['enriched_series']}")
            self.logger.info(f"  - Success Rate: {combined_results['summary']['enrichment_success_rate']:.2%}")
            
            return combined_results
            
        except Exception as e:
            self.logger.error(f"❌ Enhanced Filimo Pipeline failed: {e}")
            raise
    
    def _enrich_content_with_db(self, df: pd.DataFrame, content_type: str) -> pd.DataFrame:
        """Enrich content using the appropriate extractor"""
        # Choose extractor based on configuration

        extractor = self.enrichment_extractors["database"]
        source_name = "database_enrichment"
    
        return extractor.extract(source_name, input_df=df, content_type=content_type)
    
    def _enrich_content(self, df: pd.DataFrame, content_type: str) -> pd.DataFrame:
        """Enrich content using the appropriate extractor"""
        # Choose extractor based on configuration
        if self.enrichment_config.processing.batch_size > 1:
            extractor = self.enrichment_extractors["batch_imdb"]
            source_name = "batch_imdb_enrichment"
        else:
            extractor = self.enrichment_extractors["imdb"]
            source_name = "imdb_enrichment"
        
        return extractor.extract(source_name, input_df=df, content_type=content_type)
    
    def _transform_enriched_data(self, df: pd.DataFrame, content_type: str) -> pd.DataFrame:
        """Transform enriched data"""
        # Clean and format data
        cleaned_df = self.enrichment_transformers["data_cleaner"].transform(
            df, 
            content_type=content_type,
            operation="clean_and_format"
        )
        
        # Apply quality filtering if configured
        if self.enrichment_config.processing.confidence_threshold > 0:
            cleaned_df = self.enrichment_transformers["data_cleaner"].transform(
                cleaned_df,
                content_type=content_type,
                operation="quality_filter"
            )
        
        # Add confidence scores
        if self.enrichment_config.output.include_confidence_scores:
            cleaned_df = self.enrichment_transformers["data_cleaner"].transform(
                cleaned_df,
                content_type=content_type,
                operation="confidence_score"
            )
        
        return cleaned_df
    
    def _transform_database_enriched_data(self, df: pd.DataFrame, content_type: str) -> pd.DataFrame:
        """Transform database-enriched data using database transformer"""
        # Clean and format database data
        cleaned_df = self.enrichment_transformers["database_cleaner"].transform(
            df, 
            content_type=content_type,
            operation="clean_and_format"
        )
        
        # Apply quality filtering if configured
        if self.enrichment_config.processing.confidence_threshold > 0:
            cleaned_df = self.enrichment_transformers["database_cleaner"].transform(
                cleaned_df,
                content_type=content_type,
                operation="quality_filter"
            )
        
        # Add confidence scores
        if self.enrichment_config.output.include_confidence_scores:
            cleaned_df = self.enrichment_transformers["database_cleaner"].transform(
                cleaned_df,
                content_type=content_type,
                operation="confidence_score"
            )
        
        return cleaned_df
    
    def _load_enriched_data(self, movies_df: pd.DataFrame, series_df: pd.DataFrame) -> Dict[str, str]:
        """Load enriched data to files"""
        output_files = {}
        
        # Load movies
        movies_path = self.enrichment_loaders["csv"].load(movies_df, "enriched_movies")
        output_files["enriched_movies"] = str(movies_path)
        
        # Load series
        series_path = self.enrichment_loaders["csv"].load(series_df, "enriched_series")
        output_files["enriched_series"] = str(series_path)
        
        # Load metadata
        if self.enrichment_config.output.include_processing_metadata:
            combined_df = pd.concat([movies_df, series_df], ignore_index=True)
            metadata_path = self.enrichment_loaders["csv"].load(combined_df, "enrichment_metadata")
            output_files["enrichment_metadata"] = str(metadata_path)
        
        # Generate quality report
        if self.enrichment_config.output.generate_quality_report:
            quality_df = self.enrichment_transformers["aggregator"].transform(
                pd.concat([movies_df, series_df], ignore_index=True),
                operation="quality_report"
            )
            quality_path = self.enrichment_loaders["csv"].load(quality_df, "quality_report")
            output_files["quality_report"] = str(quality_path)
        
        return output_files
    
    def _load_enriched_data_with_db(self, movies_df: pd.DataFrame, series_df: pd.DataFrame) -> Dict[str, str]:
        """Load enriched data to files"""
        output_files = {}
        
        # Load movies
        movies_path = self.enrichment_loaders["csv"].load(movies_df, "enriched_movies_with_db")
        output_files["enriched_movies_with_db"] = str(movies_path)
        
        # Load series
        series_path = self.enrichment_loaders["csv"].load(series_df, "enriched_series_with_db")
        output_files["enriched_series_with_db"] = str(series_path)
        
        return output_files
    
    def _get_enrichment_metrics(self, movies_df: pd.DataFrame, series_df: pd.DataFrame) -> Dict[str, Any]:
        """Get enrichment metrics"""
        combined_df = pd.concat([movies_df, series_df], ignore_index=True)
        
        metrics = {
            "total_records": len(combined_df),
            "successful_enrichments": combined_df['enrichment_success'].sum() if 'enrichment_success' in combined_df.columns else 0,
            "success_rate": combined_df['enrichment_success'].mean() if 'enrichment_success' in combined_df.columns else 0.0,
            "avg_confidence": combined_df['enrichment_confidence'].mean() if 'enrichment_confidence' in combined_df.columns else 0.0,
            "avg_processing_time": combined_df['enrichment_processing_time'].mean() if 'enrichment_processing_time' in combined_df.columns else 0.0,
        }
        
        # Method breakdown
        if 'enrichment_method' in combined_df.columns:
            method_counts = combined_df['enrichment_method'].value_counts().to_dict()
            metrics["method_breakdown"] = method_counts
        
        return metrics
    
    def _calculate_overall_success_rate(self, enrichment_results: Dict[str, Any]) -> float:
        """Calculate overall enrichment success rate"""
        total_movies = enrichment_results.get("enriched_movies", 0)
        total_series = enrichment_results.get("enriched_series", 0)
        
        if total_movies + total_series == 0:
            return 0.0
        
        # This is a simplified calculation - in practice, you'd want to check actual success rates
        return enrichment_results.get("enrichment_metrics", {}).get("success_rate", 0.0)
    
    def extract_phase(self) -> Dict[str, pd.DataFrame]:
        """Extract phase - delegates to ETL pipeline"""
        return self.etl_pipeline.extract_phase()
    
    def transform_phase(self, extracted_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Transform phase - delegates to ETL pipeline"""
        return self.etl_pipeline.transform_phase(extracted_data)
    
    def load_phase(self, transformed_data: Dict[str, pd.DataFrame]):
        """Load phase - delegates to ETL pipeline"""
        return self.etl_pipeline.load_phase(transformed_data)
    
    def _generate_result(self, transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Generate enhanced pipeline result"""
        return {
            "status": "success",
            "pipeline_type": "enhanced_etl_enrichment",
            "provider": {
                "name": self.etl_config.provider_name,
                "version": self.etl_config.provider_version,
                "enrichment_version": self.enrichment_config.provider_version
            },
            "note": "This is the enhanced pipeline with enrichment capabilities"
        }

# Convenience functions for easy usage
def create_enhanced_pipeline(etl_config: FilimoConfig = None, enrichment_config: FilimoEnrichmentConfig = None) -> EnhancedFilimoPipeline:
    """Create an enhanced pipeline with default configurations"""
    if etl_config is None:
        etl_config = FilimoConfig()
    
    if enrichment_config is None:
        enrichment_config = FilimoEnrichmentConfig()
    
    return EnhancedFilimoPipeline(etl_config, enrichment_config)

def create_development_pipeline() -> EnhancedFilimoPipeline:
    """Create a development pipeline with debug settings"""
    from .config import FilimoConfigPresets
    
    etl_config = FilimoConfigPresets.development()
    enrichment_config = EnrichmentConfigPresets.development()
    
    return EnhancedFilimoPipeline(etl_config, enrichment_config)

def create_production_pipeline() -> EnhancedFilimoPipeline:
    """Create a production pipeline with optimized settings"""
    from .config import FilimoConfigPresets
    
    etl_config = FilimoConfigPresets.production()
    enrichment_config = EnrichmentConfigPresets.production()
    
    return EnhancedFilimoPipeline(etl_config, enrichment_config)
