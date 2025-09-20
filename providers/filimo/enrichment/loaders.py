#!/usr/bin/env python3
"""
Filimo Enrichment Loaders
Data loading components for enriched data.
"""

import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any, List
from core.base_pipeline import BaseLoader, BaseMetrics
from .config import FilimoEnrichmentConfig

class EnrichmentDataLoader(BaseLoader):
    """Load enriched data to various formats"""
    
    def __init__(self, config: FilimoEnrichmentConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def load(self, df: pd.DataFrame, target_name: str, **kwargs):
        """Load enriched DataFrame to target"""
        if target_name == "enriched_movies":
            return self._load_enriched_movies(df, **kwargs)
        elif target_name == "enriched_series":
            return self._load_enriched_series(df, **kwargs)
        if target_name == "enriched_movies_with_db":
            return self._load_enriched_movies_with_db(df, **kwargs)
        elif target_name == "enriched_series_with_db":
            return self._load_enriched_series_with_db(df, **kwargs)
        elif target_name == "enrichment_metadata":
            return self._load_enrichment_metadata(df, **kwargs)
        elif target_name == "quality_report":
            return self._load_quality_report(df, **kwargs)
        else:
            return self._load_generic(df, target_name, **kwargs)
    
    def _load_enriched_movies(self, df: pd.DataFrame, **kwargs) -> Path:
        """Load enriched movies data"""
        output_path = Path(self.config.output.get_enriched_movies_path(self.config.base_dir))
        return self._save_enriched_data(df, output_path, "movies")
    
    def _load_enriched_series(self, df: pd.DataFrame, **kwargs) -> Path:
        """Load enriched series data"""
        output_path = Path(self.config.output.get_enriched_series_path(self.config.base_dir))
        return self._save_enriched_data(df, output_path, "series")
    
    def _load_enriched_movies_with_db(self, df: pd.DataFrame, **kwargs) -> Path:
        """Load enriched movies data with database"""
        output_path = Path(self.config.output.get_enriched_movies_with_db_path(self.config.base_dir))
        return self._save_enriched_data(df, output_path, "movies")
    
    def _load_enriched_series_with_db(self, df: pd.DataFrame, **kwargs) -> Path:
        """Load enriched series data with database"""
        output_path = Path(self.config.output.get_enriched_series_with_db_path(self.config.base_dir))
        return self._save_enriched_data(df, output_path, "series")
    
    def _load_enrichment_metadata(self, df: pd.DataFrame, **kwargs) -> Path:
        """Load enrichment metadata"""
        metadata_path = Path(self.config.output.get_metadata_path(self.config.base_dir))
        
        # Create metadata
        metadata = self._create_enrichment_metadata(df)
        
        # Save metadata
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Enrichment metadata saved to: {metadata_path}")
        self._record_load("enrichment_metadata", len(metadata), metadata_path.stat().st_size)
        
        return metadata_path
    
    def _load_quality_report(self, df: pd.DataFrame, **kwargs) -> Path:
        """Load quality report"""
        if self.config.output.generate_quality_report:
            report_path = Path(self.config.output.get_quality_report_path(self.config.base_dir))
            
            # Create quality report
            quality_data = self._create_quality_report(df)
            
            # Save quality report
            report_path.parent.mkdir(parents=True, exist_ok=True)
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(quality_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Quality report saved to: {report_path}")
            self._record_load("quality_report", len(quality_data), report_path.stat().st_size)
            
            return report_path
        else:
            self.logger.info("Quality report generation disabled")
            return None
    
    def _load_generic(self, df: pd.DataFrame, target_name: str, **kwargs) -> Path:
        """Load data to generic CSV file"""
        output_path = self.config.base_dir / f"{target_name}.csv"
        return self._save_enriched_data(df, output_path, target_name)
    
    def _save_enriched_data(self, df: pd.DataFrame, output_path: Path, data_type: str) -> Path:
        """Save enriched data to CSV with proper formatting"""
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Prepare data for saving
        save_df = df.copy()
        
        # Format data for output
        if self.config.output.include_confidence_scores:
            # Ensure confidence scores are properly formatted
            confidence_cols = [col for col in save_df.columns if 'confidence' in col.lower()]
            for col in confidence_cols:
                save_df[col] = pd.to_numeric(save_df[col], errors='coerce').round(3)
        
        # Save to CSV
        save_df.to_csv(output_path, index=False, encoding='utf-8')
        
        # Record metrics
        file_size = output_path.stat().st_size
        self._record_load(f"enriched_{data_type}", len(save_df), file_size)
        
        self.logger.info(f"Enriched {data_type} data saved to: {output_path}")
        self.logger.info(f"  - Records: {len(save_df)}")
        self.logger.info(f"  - File size: {file_size / 1024:.1f} KB")
        
        return output_path
    
    def _create_enrichment_metadata(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create enrichment metadata"""
        metadata = {
            "enrichment_info": {
                "total_records": int(len(df)),
                "successful_enrichments": int(df['enrichment_success'].sum()) if 'enrichment_success' in df.columns else 0,
                "success_rate": float(df['enrichment_success'].mean()) if 'enrichment_success' in df.columns else 0.0,
                "avg_confidence": float(df['enrichment_confidence'].mean()) if 'enrichment_confidence' in df.columns else 0.0,
                "avg_processing_time": float(df['enrichment_processing_time'].mean()) if 'enrichment_processing_time' in df.columns else 0.0,
            },
            "method_breakdown": {},
            "quality_metrics": {},
            "processing_config": {
                "batch_size": int(self.config.processing.batch_size),
                "max_parallel_workers": int(self.config.processing.max_parallel_workers),
                "confidence_threshold": float(self.config.processing.confidence_threshold),
                "enrichment_strategy": str(self.config.processing.enrichment_strategy),
            }
        }
        
        # Method breakdown
        if 'enrichment_method' in df.columns:
            method_counts = df['enrichment_method'].value_counts().to_dict()
            # Convert numpy types to native Python types
            metadata["method_breakdown"] = {str(k): int(v) for k, v in method_counts.items()}
        
        # Quality metrics
        if 'data_quality_score' in df.columns:
            metadata["quality_metrics"] = {
                "avg_quality_score": float(df['data_quality_score'].mean()),
                "high_quality_records": int((df['data_quality_score'] >= 0.8).sum()),
                "low_quality_records": int((df['data_quality_score'] < 0.5).sum()),
            }
        
        return metadata
    
    def _create_quality_report(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create detailed quality report"""
        if df.empty:
            return {"error": "No data to analyze"}
        
        report = {
            "summary": {
                "total_records": int(len(df)),
                "analysis_timestamp": pd.Timestamp.now().isoformat(),
            },
            "enrichment_quality": {},
            "data_completeness": {},
            "confidence_analysis": {},
            "recommendations": []
        }
        
        # Enrichment quality
        if 'enrichment_success' in df.columns:
            success_rate = float(df['enrichment_success'].mean())
            report["enrichment_quality"] = {
                "success_rate": success_rate,
                "successful_count": int(df['enrichment_success'].sum()),
                "failed_count": int((~df['enrichment_success']).sum()),
            }
            
            if success_rate < 0.7:
                report["recommendations"].append("Low enrichment success rate - consider adjusting search parameters")
        
        # Data completeness
        imdb_fields = ['imdb_id', 'imdb_primary_title', 'imdb_start_year', 'imdb_rating', 'imdb_cover_url']
        completeness = {}
        for field in imdb_fields:
            if field in df.columns:
                complete_count = int((df[field] != 'NAN').sum())
                completeness[field] = {
                    "complete_count": complete_count,
                    "completeness_rate": float(complete_count / len(df))
                }
        
        report["data_completeness"] = completeness
        
        # Confidence analysis
        if 'enrichment_confidence' in df.columns:
            confidence_stats = df['enrichment_confidence'].describe()
            report["confidence_analysis"] = {
                "mean_confidence": float(confidence_stats['mean']),
                "median_confidence": float(confidence_stats['50%']),
                "min_confidence": float(confidence_stats['min']),
                "max_confidence": float(confidence_stats['max']),
                "high_confidence_records": int((df['enrichment_confidence'] >= 0.8).sum()),
                "low_confidence_records": int((df['enrichment_confidence'] < 0.5).sum()),
            }
            
            if confidence_stats['mean'] < 0.6:
                report["recommendations"].append("Low average confidence - consider improving title matching")
        
        return report

class EnrichmentDataExporter(BaseLoader):
    """Export enriched data to various formats"""
    
    def __init__(self, config: FilimoEnrichmentConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def load(self, df: pd.DataFrame, target_name: str, **kwargs):
        """Export data in multiple formats"""
        formats = kwargs.get('formats', ['csv'])
        results = {}
        
        for format_type in formats:
            if format_type == 'csv':
                results['csv'] = self._export_csv(df, target_name)
            elif format_type == 'json':
                results['json'] = self._export_json(df, target_name)
            elif format_type == 'excel':
                results['excel'] = self._export_excel(df, target_name)
            else:
                self.logger.warning(f"Unsupported export format: {format_type}")
        
        return results
    
    def _export_csv(self, df: pd.DataFrame, target_name: str) -> Path:
        """Export to CSV format"""
        output_path = self.config.base_dir / f"{target_name}.csv"
        df.to_csv(output_path, index=False, encoding='utf-8')
        self._record_load(f"export_csv_{target_name}", len(df), output_path.stat().st_size)
        return output_path
    
    def _export_json(self, df: pd.DataFrame, target_name: str) -> Path:
        """Export to JSON format"""
        output_path = self.config.base_dir / f"{target_name}.json"
        df.to_json(output_path, orient='records', indent=2, force_ascii=False)
        self._record_load(f"export_json_{target_name}", len(df), output_path.stat().st_size)
        return output_path
    
    def _export_excel(self, df: pd.DataFrame, target_name: str) -> Path:
        """Export to Excel format"""
        try:
            output_path = self.config.base_dir / f"{target_name}.xlsx"
            df.to_excel(output_path, index=False, engine='openpyxl')
            self._record_load(f"export_excel_{target_name}", len(df), output_path.stat().st_size)
            return output_path
        except ImportError:
            self.logger.error("openpyxl not available for Excel export")
            return None