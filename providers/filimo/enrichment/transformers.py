#!/usr/bin/env python3
"""
Filimo Enrichment Transformers
Data transformation components for enriched data.
"""

import pandas as pd
from typing import Dict, Any, List
from core.base_pipeline import BaseTransformer, BaseMetrics
from .config import FilimoEnrichmentConfig

class EnrichmentDataTransformer(BaseTransformer):
    """Transform enriched data for final output"""
    
    def __init__(self, config: FilimoEnrichmentConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform enriched data"""
        content_type = kwargs.get('content_type', 'unknown')
        operation = kwargs.get('operation', 'clean_and_format')
        
        if operation == 'clean_and_format':
            return self._clean_and_format_data(df, content_type)
        elif operation == 'quality_filter':
            return self._quality_filter_data(df, content_type)
        elif operation == 'confidence_score':
            return self._add_confidence_scores(df, content_type)
        else:
            raise ValueError(f"Unknown transformation operation: {operation}")
    
    def _clean_and_format_data(self, df: pd.DataFrame, content_type: str) -> pd.DataFrame:
        """Clean and format enriched data"""
        self.logger.info(f"Cleaning and formatting {len(df)} enriched {content_type} records")
        
        cleaned_df = df.copy()
        before_count = len(cleaned_df)
        
        # Clean IMDB data
        cleaned_df = self._clean_imdb_data(cleaned_df)
        
        # Format fields consistently
        cleaned_df = self._format_fields(cleaned_df)
        
        # Remove duplicates if any
        cleaned_df = self._remove_duplicates(cleaned_df)
        
        # Add quality indicators
        cleaned_df = self._add_quality_indicators(cleaned_df)
        
        after_count = len(cleaned_df)
        self._record_transformation("clean_and_format", before_count, after_count, {
            "content_type": content_type,
            "removed_duplicates": before_count - after_count
        })
        
        self.logger.info(f"Data cleaning completed: {before_count} -> {after_count} records")
        return cleaned_df
    
    def _clean_imdb_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean IMDB and Database data fields"""
        # Replace empty strings with NAN for IMDB columns
        imdb_columns = [col for col in df.columns if col.startswith('imdb_')]
        for col in imdb_columns:
            df[col] = df[col].replace('', 'NAN')
            df[col] = df[col].replace('null', 'NAN')
            df[col] = df[col].replace('None', 'NAN')
        
        # Replace empty strings with NAN for Database columns
        db_columns = [col for col in df.columns if col.startswith('db_')]
        for col in db_columns:
            df[col] = df[col].replace('', 'NAN')
            df[col] = df[col].replace('null', 'NAN')
            df[col] = df[col].replace('None', 'NAN')
        
        # Clean numeric IMDB fields
        numeric_imdb_fields = ['imdb_start_year', 'imdb_end_year', 'imdb_runtime_minutes', 'imdb_rating', 'imdb_votes']
        for field in numeric_imdb_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
                df[field] = df[field].fillna('NAN')
        
        # Clean numeric Database fields
        numeric_db_fields = ['db_year', 'db_duration', 'db_rate', 'db_vote_count', 'db_price', 'db_huma_id', 'db_imdb_id', 'db_tmdb_id']
        for field in numeric_db_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
                df[field] = df[field].fillna('NAN')
        
        return df
    
    def _format_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format fields consistently"""
        # Ensure string fields are properly formatted
        string_fields = ['provider_title_persian', 'provider_title_english', 'imdb_primary_title', 'imdb_original_title']
        for field in string_fields:
            if field in df.columns:
                df[field] = df[field].astype(str).str.strip()
                df[field] = df[field].replace('nan', 'NAN')
        
        # Format boolean fields
        boolean_fields = ['enrichment_success']
        for field in boolean_fields:
            if field in df.columns:
                df[field] = df[field].astype(bool)
        
        # Format numeric fields
        numeric_fields = ['enrichment_confidence', 'enrichment_processing_time']
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
                df[field] = df[field].fillna(0.0)
        
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate records based on provider_id"""
        if 'provider_id' in df.columns:
            before_count = len(df)
            df = df.drop_duplicates(subset=['provider_id'], keep='first')
            after_count = len(df)
            if before_count != after_count:
                self.logger.info(f"Removed {before_count - after_count} duplicate records")
        
        return df
    
    def _add_quality_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add data quality indicators"""
        # Add quality score based on available data
        df['data_quality_score'] = df.apply(self._calculate_quality_score, axis=1)
        
        # Add completeness indicators
        df['has_imdb_data'] = df['enrichment_success']
        df['has_cover_image'] = (df['imdb_cover_url'] != 'NAN') & (df['imdb_cover_url'].notna())
        df['has_rating'] = (df['imdb_rating'] != 'NAN') & (df['imdb_rating'].notna())
        
        return df
    
    def _calculate_quality_score(self, row: pd.Series) -> float:
        """Calculate data quality score for a record"""
        score = 0.0
        
        # Base score for successful enrichment
        if row.get('enrichment_success', False):
            score += 0.3
        
        # Score for confidence level
        confidence = row.get('enrichment_confidence', 0.0)
        if isinstance(confidence, (int, float)):
            score += confidence * 0.3
        
        # Score for available IMDB data
        imdb_fields = ['imdb_primary_title', 'imdb_start_year', 'imdb_genres', 'imdb_rating']
        available_imdb_fields = sum(1 for field in imdb_fields if field in row and row[field] != 'NAN' and pd.notna(row[field]))
        score += (available_imdb_fields / len(imdb_fields)) * 0.4
        
        return min(score, 1.0)
    
    def _quality_filter_data(self, df: pd.DataFrame, content_type: str) -> pd.DataFrame:
        """Filter data based on quality criteria"""
        self.logger.info(f"Applying quality filter to {len(df)} {content_type} records")
        
        before_count = len(df)
        
        # Filter based on confidence threshold
        if 'enrichment_confidence' in df.columns:
            threshold = self.config.processing.confidence_threshold
            df = df[df['enrichment_confidence'] >= threshold]
            self.logger.info(f"Filtered by confidence threshold ({threshold}): {len(df)} records remaining")
        
        # Filter out records with critical missing data
        critical_fields = ['provider_id', 'provider_title_persian']
        for field in critical_fields:
            if field in df.columns:
                df = df[df[field].notna() & (df[field] != 'NAN') & (df[field] != '')]
        
        after_count = len(df)
        self._record_transformation("quality_filter", before_count, after_count, {
            "content_type": content_type,
            "confidence_threshold": self.config.processing.confidence_threshold
        })
        
        self.logger.info(f"Quality filtering completed: {before_count} -> {after_count} records")
        return df
    
    def _add_confidence_scores(self, df: pd.DataFrame, content_type: str) -> pd.DataFrame:
        """Add detailed confidence scoring"""
        self.logger.info(f"Adding confidence scores to {len(df)} {content_type} records")
        
        # Calculate detailed confidence scores
        df['title_match_confidence'] = df.apply(self._calculate_title_match_confidence, axis=1)
        df['year_match_confidence'] = df.apply(self._calculate_year_match_confidence, axis=1)
        df['type_match_confidence'] = df.apply(self._calculate_type_match_confidence, axis=1)
        
        # Overall confidence score
        df['overall_confidence'] = (
            df['title_match_confidence'] * 0.5 +
            df['year_match_confidence'] * 0.3 +
            df['type_match_confidence'] * 0.2
        )
        
        self._record_transformation("confidence_scoring", len(df), len(df), {
            "content_type": content_type,
            "avg_confidence": df['overall_confidence'].mean()
        })
        
        return df
    
    def _calculate_title_match_confidence(self, row: pd.Series) -> float:
        """Calculate confidence based on title matching"""
        if not row.get('enrichment_success', False):
            return 0.0
        
        provider_title = str(row.get('provider_title_english', '')).lower()
        imdb_title = str(row.get('imdb_primary_title', '')).lower()
        
        if not provider_title or not imdb_title or provider_title == 'nan' or imdb_title == 'nan':
            return 0.0
        
        # Exact match
        if provider_title == imdb_title:
            return 1.0
        
        # Substring match
        if provider_title in imdb_title or imdb_title in provider_title:
            return 0.8
        
        # Word overlap
        provider_words = set(provider_title.split())
        imdb_words = set(imdb_title.split())
        overlap = len(provider_words.intersection(imdb_words))
        total = len(provider_words.union(imdb_words))
        
        return overlap / total if total > 0 else 0.0
    
    def _calculate_year_match_confidence(self, row: pd.Series) -> float:
        """Calculate confidence based on year matching"""
        if not row.get('enrichment_success', False):
            return 0.0
        
        imdb_year = row.get('imdb_start_year')
        if pd.isna(imdb_year) or imdb_year == 'NAN':
            return 0.5  # Neutral score if no year data
        
        # If we have year data, give higher confidence
        try:
            year = int(imdb_year)
            if 1900 <= year <= 2030:  # Reasonable year range
                return 1.0
        except (ValueError, TypeError):
            pass
        
        return 0.5
    
    def _calculate_type_match_confidence(self, row: pd.Series) -> float:
        """Calculate confidence based on content type matching"""
        if not row.get('enrichment_success', False):
            return 0.0
        
        content_type = row.get('content_type', '')
        imdb_type = row.get('imdb_type', '')
        
        if not content_type or not imdb_type or imdb_type == 'NAN':
            return 0.5
        
        # Check if IMDB type matches expected content type
        if content_type == 'movie' and imdb_type in self.config.processing.movie_types:
            return 1.0
        elif content_type == 'series' and imdb_type in self.config.processing.series_types:
            return 1.0
        else:
            return 0.3  # Lower confidence for type mismatch

class EnrichmentDataAggregator(BaseTransformer):
    """Aggregate enriched data for reporting and analysis"""
    
    def __init__(self, config: FilimoEnrichmentConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Aggregate enriched data"""
        operation = kwargs.get('operation', 'summary_stats')
        
        if operation == 'summary_stats':
            return self._create_summary_stats(df)
        elif operation == 'quality_report':
            return self._create_quality_report(df)
        else:
            raise ValueError(f"Unknown aggregation operation: {operation}")
    
    def _create_summary_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create summary statistics"""
        stats = {
            'total_records': len(df),
            'successful_enrichments': df['enrichment_success'].sum() if 'enrichment_success' in df.columns else 0,
            'avg_confidence': df['enrichment_confidence'].mean() if 'enrichment_confidence' in df.columns else 0.0,
            'avg_processing_time': df['enrichment_processing_time'].mean() if 'enrichment_processing_time' in df.columns else 0.0,
        }
        
        # Method breakdown
        if 'enrichment_method' in df.columns:
            method_counts = df['enrichment_method'].value_counts().to_dict()
            stats.update({f'method_{method}': count for method, count in method_counts.items()})
        
        return pd.DataFrame([stats])
    
    def _create_quality_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create quality report"""
        if df.empty:
            return pd.DataFrame()
        
        quality_metrics = {
            'total_records': len(df),
            'enrichment_success_rate': df['enrichment_success'].mean() if 'enrichment_success' in df.columns else 0.0,
            'avg_confidence_score': df['enrichment_confidence'].mean() if 'enrichment_confidence' in df.columns else 0.0,
            'high_confidence_records': (df['enrichment_confidence'] >= 0.8).sum() if 'enrichment_confidence' in df.columns else 0,
            'records_with_imdb_data': (df['imdb_id'] != 'NAN').sum() if 'imdb_id' in df.columns else 0,
            'records_with_ratings': (df['imdb_rating'] != 'NAN').sum() if 'imdb_rating' in df.columns else 0,
        }
        
        return pd.DataFrame([quality_metrics])


class DatabaseEnrichmentTransformer(BaseTransformer):
    """Transform database-enriched data for final output"""
    
    def __init__(self, config: FilimoEnrichmentConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform database-enriched data"""
        content_type = kwargs.get('content_type', 'unknown')
        operation = kwargs.get('operation', 'clean_and_format')
        
        if operation == 'clean_and_format':
            return self._clean_and_format_database_data(df, content_type)
        elif operation == 'quality_filter':
            return self._quality_filter_database_data(df, content_type)
        elif operation == 'confidence_score':
            return self._add_database_confidence_scores(df, content_type)
        else:
            raise ValueError(f"Unknown transformation operation: {operation}")
    
    def _clean_and_format_database_data(self, df: pd.DataFrame, content_type: str) -> pd.DataFrame:
        """Clean and format database-enriched data"""
        self.logger.info(f"Cleaning and formatting {len(df)} database-enriched {content_type} records")
        
        cleaned_df = df.copy()
        before_count = len(cleaned_df)
        
        # Clean database data
        cleaned_df = self._clean_database_data(cleaned_df)
        
        # Format fields consistently
        cleaned_df = self._format_database_fields(cleaned_df)
        
        # Remove duplicates if any
        cleaned_df = self._remove_duplicates(cleaned_df)
        
        # Add quality indicators
        cleaned_df = self._add_database_quality_indicators(cleaned_df)
        
        after_count = len(cleaned_df)
        self._record_transformation("clean_and_format_database", before_count, after_count, {
            "content_type": content_type,
            "removed_duplicates": before_count - after_count
        })
        
        self.logger.info(f"Database data cleaning completed: {before_count} -> {after_count} records")
        return cleaned_df
    
    def _clean_database_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean database data fields"""
        # Replace empty strings with NAN for Database columns
        db_columns = [col for col in df.columns if col.startswith('db_')]
        for col in db_columns:
            df[col] = df[col].replace('', 'NAN')
            df[col] = df[col].replace('null', 'NAN')
            df[col] = df[col].replace('None', 'NAN')
        
        # Clean numeric Database fields
        numeric_db_fields = ['db_year', 'db_duration', 'db_rate', 'db_vote_count', 'db_price', 'db_huma_id', 'db_imdb_id', 'db_tmdb_id']
        for field in numeric_db_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
                df[field] = df[field].fillna('NAN')
        
        return df
    
    def _format_database_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format database fields consistently"""
        # Ensure string fields are properly formatted
        string_fields = ['provider_title_persian', 'provider_title_english', 'db_title', 'db_title_fa', 'db_title_enfa', 'db_plot', 'db_plot_fa']
        for field in string_fields:
            if field in df.columns:
                df[field] = df[field].astype(str).str.strip()
                df[field] = df[field].replace('nan', 'NAN')
        
        # Format boolean fields
        boolean_fields = ['enrichment_success']
        for field in boolean_fields:
            if field in df.columns:
                df[field] = df[field].astype(bool)
        
        # Format numeric fields
        numeric_fields = ['enrichment_confidence', 'enrichment_processing_time']
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
                df[field] = df[field].fillna(0.0)
        
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate records based on provider_id"""
        if 'provider_id' in df.columns:
            before_count = len(df)
            df = df.drop_duplicates(subset=['provider_id'], keep='first')
            after_count = len(df)
            if before_count != after_count:
                self.logger.info(f"Removed {before_count - after_count} duplicate records")
        
        return df
    
    def _add_database_quality_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add database-specific quality indicators"""
        # Add quality score based on available database data
        df['database_quality_score'] = df.apply(self._calculate_database_quality_score, axis=1)
        
        # Add completeness indicators for database data
        df['has_database_data'] = df['enrichment_success']
        df['has_database_cover'] = (df['db_cover'] != 'NAN') & (df['db_cover'].notna())
        df['has_database_rating'] = (df['db_rate'] != 'NAN') & (df['db_rate'].notna())
        df['has_database_plot'] = (df['db_plot'] != 'NAN') & (df['db_plot'].notna())
        
        return df
    
    def _calculate_database_quality_score(self, row: pd.Series) -> float:
        """Calculate data quality score for database-enriched record"""
        score = 0.0
        
        # Base score for successful enrichment
        if row.get('enrichment_success', False):
            score += 0.3
        
        # Score for confidence level
        confidence = row.get('enrichment_confidence', 0.0)
        if isinstance(confidence, (int, float)):
            score += confidence * 0.3
        
        # Score for available database data
        db_fields = ['db_title', 'db_year', 'db_plot', 'db_rate', 'db_cover']
        available_db_fields = sum(1 for field in db_fields if field in row and row[field] != 'NAN' and pd.notna(row[field]))
        score += (available_db_fields / len(db_fields)) * 0.4
        
        return min(score, 1.0)
    
    def _quality_filter_database_data(self, df: pd.DataFrame, content_type: str) -> pd.DataFrame:
        """Filter database data based on quality criteria"""
        self.logger.info(f"Applying quality filter to {len(df)} database-enriched {content_type} records")
        
        before_count = len(df)
        
        # Filter based on confidence threshold
        if 'enrichment_confidence' in df.columns:
            threshold = self.config.processing.confidence_threshold
            print(df['enrichment_confidence'])
            df = df[df['enrichment_confidence'] >= threshold]
            self.logger.info(f"Filtered by confidence threshold ({threshold}): {len(df)} records remaining")
        
        # Filter out records with critical missing data
        critical_fields = ['provider_id', 'provider_title_persian']
        for field in critical_fields:
            if field in df.columns:
                df = df[df[field].notna() & (df[field] != 'NAN') & (df[field] != '')]
        
        after_count = len(df)
        self._record_transformation("quality_filter_database", before_count, after_count, {
            "content_type": content_type,
            "confidence_threshold": self.config.processing.confidence_threshold
        })
        
        self.logger.info(f"Database quality filtering completed: {before_count} -> {after_count} records")
        return df
    
    def _add_database_confidence_scores(self, df: pd.DataFrame, content_type: str) -> pd.DataFrame:
        """Add detailed confidence scoring for database data"""
        self.logger.info(f"Adding database confidence scores to {len(df)} {content_type} records")
        
        # Calculate database-specific confidence scores
        df['title_match_confidence'] = df.apply(self._calculate_database_title_match_confidence, axis=1)
        df['year_match_confidence'] = df.apply(self._calculate_database_year_match_confidence, axis=1)
        df['type_match_confidence'] = df.apply(self._calculate_database_type_match_confidence, axis=1)
        
        # Overall confidence score
        df['overall_confidence'] = (
            df['title_match_confidence'] * 0.5 +
            df['year_match_confidence'] * 0.3 +
            df['type_match_confidence'] * 0.2
        )
        
        self._record_transformation("database_confidence_scoring", len(df), len(df), {
            "content_type": content_type,
            "avg_confidence": df['overall_confidence'].mean()
        })
        
        return df
    
    def _calculate_database_title_match_confidence(self, row: pd.Series) -> float:
        """Calculate confidence based on database title matching"""
        if not row.get('enrichment_success', False):
            return 0.0
        
        provider_title = str(row.get('provider_title_english', '')).lower()
        db_title = str(row.get('db_title', '')).lower()
        
        if not provider_title or not db_title or provider_title == 'nan' or db_title == 'nan':
            return 0.0
        
        # Exact match
        if provider_title == db_title:
            return 1.0
        
        # Substring match
        if provider_title in db_title or db_title in provider_title:
            return 0.8
        
        # Word overlap
        provider_words = set(provider_title.split())
        db_words = set(db_title.split())
        overlap = len(provider_words.intersection(db_words))
        total = len(provider_words.union(db_words))
        
        return overlap / total if total > 0 else 0.0
    
    def _calculate_database_year_match_confidence(self, row: pd.Series) -> float:
        """Calculate confidence based on database year matching"""
        if not row.get('enrichment_success', False):
            return 0.0
        
        db_year = row.get('db_year')
        if pd.isna(db_year) or db_year == 'NAN':
            return 0.5  # Neutral score if no year data
        
        # If we have year data, give higher confidence
        try:
            year = int(db_year)
            if 1900 <= year <= 2030:  # Reasonable year range
                return 1.0
        except (ValueError, TypeError):
            pass
        
        return 0.5
    
    def _calculate_database_type_match_confidence(self, row: pd.Series) -> float:
        """Calculate confidence based on database content type matching"""
        if not row.get('enrichment_success', False):
            return 0.0
        
        content_type = row.get('content_type', '')
        db_type_id = row.get('db_movie_type_id', '')
        
        if not content_type or not db_type_id or db_type_id == 'NAN':
            return 0.5
        
        # Map database movie_type_id to content type
        # This mapping should be configured based on your database schema
        try:
            type_id = int(db_type_id)
            if content_type == 'movie' and type_id == 1:
                return 1.0
            elif content_type == 'series' and type_id == 2:
                return 1.0
            else:
                return 0.3  # Lower confidence for type mismatch
        except (ValueError, TypeError):
            return 0.5
