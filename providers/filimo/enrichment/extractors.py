#!/usr/bin/env python3
"""
Filimo Enrichment Extractors
Extractors for IMDB and translation data sources.
"""

import pandas as pd
from typing import Dict, Any, List
from core.base_pipeline import BaseExtractor, BaseMetrics
from .config import FilimoEnrichmentConfig
from .services import EnrichmentOrchestrator

class IMDBEnrichmentExtractor(BaseExtractor):
    """Extract IMDB data for enrichment"""
    
    def __init__(self, config: FilimoEnrichmentConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
        self.orchestrator = EnrichmentOrchestrator(config)
    
    def extract(self, source_name: str, **kwargs) -> pd.DataFrame:
        """Extract IMDB enrichment data"""
        if source_name == "imdb_enrichment":
            return self._extract_imdb_enrichment(**kwargs)
        else:
            raise ValueError(f"Unknown enrichment source: {source_name}")
    
    def _extract_imdb_enrichment(self, input_df: pd.DataFrame, content_type: str = "movie") -> pd.DataFrame:
        """Extract IMDB data for given input DataFrame"""
        self.logger.info(f"Starting IMDB enrichment for {len(input_df)} {content_type} records")
        
        if content_type == "series":
            return self._extract_series_enrichment(input_df)
        else:
            return self._extract_movie_enrichment(input_df)
    
    def _extract_series_enrichment(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Extract IMDB data for series, grouping episodes by series"""
        self.logger.info(f"Starting series enrichment with grouping strategy")
        
        # Group episodes by series (serial_parent_id)
        series_groups = input_df.groupby('id')
        unique_series = len(series_groups)
        
        self.logger.info(f"Found {unique_series} unique series from {len(input_df)} episodes")
        
        enriched_data = []
        successful_enrichments = 0
        total_processing_time = 0.0
        
        for series_id, episodes_df in series_groups:
            # Get series title from first episode (all episodes should have same series title)
            series_title = episodes_df.iloc[0].get('title', 'Unknown')
            episode_count = len(episodes_df)
            
            self.logger.info(f"Processing series {series_id}: '{series_title}' ({episode_count} episodes)")
            
            # Create series record for enrichment
            series_record = {
                'title': series_title,
                'titleEn': episodes_df.iloc[0].get('titleEn', ''),
                'ImdbId': episodes_df.iloc[0].get('ImdbId', ''),
                'serial_parent_id': series_id
            }
            
            # Enrich the series once
            result = self.orchestrator.enrich_record(series_record, "series")
            total_processing_time += result.processing_time
            
            # Apply enrichment to all episodes of this series
            for _, episode_row in episodes_df.iterrows():
                enriched_record = self._create_enriched_record(episode_row, result, "series")
                enriched_data.append(enriched_record)
            
            if result.success:
                successful_enrichments += episode_count
                self.logger.info(f"Successfully enriched series '{series_title}' using {result.method_used} - applied to {episode_count} episodes")
            else:
                self.logger.warning(f"Failed to enrich series '{series_title}' - {result.error_message} - applied to {episode_count} episodes")
            
            # Record metrics for this series
            self._record_extraction(f"imdb_enrichment_series", episode_count, result.processing_time)
        
        # Calculate success rate and log summary
        success_rate = successful_enrichments / len(input_df) if len(input_df) > 0 else 0
        avg_processing_time = total_processing_time / unique_series if unique_series > 0 else 0
        
        self.logger.info(f"Series enrichment completed:")
        self.logger.info(f"  - Unique series processed: {unique_series}")
        self.logger.info(f"  - Total episodes processed: {len(input_df)}")
        self.logger.info(f"  - Successful enrichments: {successful_enrichments}")
        self.logger.info(f"  - Success rate: {success_rate:.2%}")
        self.logger.info(f"  - Average processing time per series: {avg_processing_time:.2f} seconds")
        
        return pd.DataFrame(enriched_data)
    
    def _extract_movie_enrichment(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Extract IMDB data for movies (individual processing)"""
        self.logger.info(f"Starting movie enrichment (individual processing)")
        
        enriched_data = []
        successful_enrichments = 0
        total_processing_time = 0.0
        
        for index, row in input_df.iterrows():
            self.logger.info(f"Processing {index + 1}/{len(input_df)}: {row.get('title', 'Unknown')}")
            
            # Enrich the record
            result = self.orchestrator.enrich_record(row.to_dict(), "movie")
            total_processing_time += result.processing_time
            
            # Create enriched record
            enriched_record = self._create_enriched_record(row, result, "movie")
            enriched_data.append(enriched_record)
            
            if result.success:
                successful_enrichments += 1
                self.logger.debug(f"Successfully enriched: {row.get('title', 'Unknown')} using {result.method_used}")
            else:
                self.logger.warning(f"Failed to enrich: {row.get('title', 'Unknown')} - {result.error_message}")
            
            # Record metrics for this record
            self._record_extraction(f"imdb_enrichment_movie", 1, result.processing_time)
        
        # Calculate success rate and log summary
        success_rate = successful_enrichments / len(input_df) if len(input_df) > 0 else 0
        avg_processing_time = total_processing_time / len(input_df) if len(input_df) > 0 else 0
        
        self.logger.info(f"Movie enrichment completed:")
        self.logger.info(f"  - Records processed: {len(input_df)}")
        self.logger.info(f"  - Successful enrichments: {successful_enrichments}")
        self.logger.info(f"  - Success rate: {success_rate:.2%}")
        self.logger.info(f"  - Average processing time: {avg_processing_time:.2f} seconds")
        
        return pd.DataFrame(enriched_data)
    
    def _create_enriched_record(self, original_row: pd.Series, enrichment_result, content_type: str) -> Dict[str, Any]:
        """Create enriched record combining original data with IMDB data"""
        enriched = {
            # Original provider data
            "provider_id": original_row.get("id", ""),
            "provider_title_persian": original_row.get("title", ""),
            "provider_title_english": original_row.get("titleEn", ""),
            "provider_url": original_row.get("url", ""),
            "provider_external_link": original_row.get("externalLink", ""),
            "provider_imdb_id": original_row.get("ImdbId", ""),
            "content_type": content_type,
            
            # Enrichment metadata
            "enrichment_success": enrichment_result.success,
            "enrichment_method": enrichment_result.method_used,
            "enrichment_confidence": enrichment_result.confidence_score,
            "enrichment_processing_time": enrichment_result.processing_time,
            "enrichment_error": enrichment_result.error_message if not enrichment_result.success else "",
        }
        
        # Add IMDB data if enrichment was successful
        if enrichment_result.success and enrichment_result.data:
            imdb_data = enrichment_result.data
            
            # Extract rating data
            rating_data = imdb_data.get("rating", {})
            aggregate_rating = rating_data.get("aggregateRating", "NAN") if rating_data else "NAN"
            vote_count = rating_data.get("voteCount", "NAN") if rating_data else "NAN"
            
            # Extract primary image data
            primary_image = imdb_data.get("primaryImage", {})
            cover_url = primary_image.get("url", "NAN") if primary_image else "NAN"
            cover_width = primary_image.get("width", "NAN") if primary_image else "NAN"
            cover_height = primary_image.get("height", "NAN") if primary_image else "NAN"
            
            # Extract directors
            directors = imdb_data.get("directors", [])
            directors_list = [f"{d.get('displayName', '')} ({d.get('id', '')})" for d in directors] if directors else []
            directors_str = "; ".join(directors_list) if directors_list else "NAN"
            
            # Extract writers
            writers = imdb_data.get("writers", [])
            writers_list = [f"{w.get('displayName', '')} ({w.get('id', '')})" for w in writers] if writers else []
            writers_str = "; ".join(writers_list) if writers_list else "NAN"
            
            # Extract stars
            stars = imdb_data.get("stars", [])
            stars_list = [f"{s.get('displayName', '')} ({s.get('id', '')})" for s in stars] if stars else []
            stars_str = "; ".join(stars_list) if stars_list else "NAN"
            
            # Extract origin countries
            origin_countries = imdb_data.get("originCountries", [])
            countries_list = [f"{c.get('name', '')} ({c.get('code', '')})" for c in origin_countries] if origin_countries else []
            countries_str = "; ".join(countries_list) if countries_list else "NAN"
            
            # Extract spoken languages
            spoken_languages = imdb_data.get("spokenLanguages", [])
            languages_list = [f"{l.get('name', '')} ({l.get('code', '')})" for l in spoken_languages] if spoken_languages else []
            languages_str = "; ".join(languages_list) if languages_list else "NAN"
            
            # Extract interests
            interests = imdb_data.get("interests", [])
            interests_list = [f"{i.get('name', '')} ({i.get('id', '')})" for i in interests] if interests else []
            interests_str = "; ".join(interests_list) if interests_list else "NAN"
            
            enriched.update({
                "imdb_id": imdb_data.get("id", "NAN"),
                "imdb_type": imdb_data.get("type", "NAN"),
                "imdb_primary_title": imdb_data.get("primaryTitle", "NAN"),
                "imdb_original_title": imdb_data.get("originalTitle", "NAN"),
                "imdb_start_year": imdb_data.get("startYear", "NAN"),
                "imdb_end_year": imdb_data.get("endYear", "NAN"),
                "imdb_genres": ", ".join(imdb_data.get("genres", [])) if imdb_data.get("genres") else "NAN",
                "imdb_rating": aggregate_rating,
                "imdb_votes": vote_count,
                "imdb_cover_url": cover_url,
                "imdb_cover_width": cover_width,
                "imdb_cover_height": cover_height,
                "imdb_plot": imdb_data.get("plot", "NAN"),
                "imdb_directors": directors_str,
                "imdb_writers": writers_str,
                "imdb_stars": stars_str,
                "imdb_origin_countries": countries_str,
                "imdb_spoken_languages": languages_str,
                "imdb_interests": interests_str,
            })
        else:
            # Add NAN values for failed enrichments
            imdb_fields = [
                "imdb_id", "imdb_type", "imdb_primary_title", "imdb_original_title",
                "imdb_start_year", "imdb_end_year", "imdb_genres", "imdb_rating", 
                "imdb_votes", "imdb_cover_url", "imdb_cover_width", "imdb_cover_height",
                "imdb_plot", "imdb_directors", "imdb_writers", "imdb_stars",
                "imdb_origin_countries", "imdb_spoken_languages", "imdb_interests"
            ]
            for field in imdb_fields:
                enriched[field] = "NAN"
        
        # Add series-specific fields if applicable
        if content_type == "series":
            enriched.update({
                "series_id": original_row.get("series_id", ""),
                "episode_no": original_row.get("episodeNo", ""),
                "season_no": original_row.get("seasonNo", ""),
            })
        
        return enriched

class BatchEnrichmentExtractor(BaseExtractor):
    """Extract enrichment data in batches for better performance"""
    
    def __init__(self, config: FilimoEnrichmentConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
        self.orchestrator = EnrichmentOrchestrator(config)
    
    def extract(self, source_name: str, **kwargs) -> pd.DataFrame:
        """Extract enrichment data in batches"""
        if source_name == "batch_imdb_enrichment":
            return self._extract_batch_enrichment(**kwargs)
        else:
            raise ValueError(f"Unknown enrichment source: {source_name}")
    
    def _extract_batch_enrichment(self, input_df: pd.DataFrame, content_type: str = "movie") -> pd.DataFrame:
        """Extract enrichment data in batches for better performance"""
        self.logger.info(f"Starting batch IMDB enrichment for {len(input_df)} {content_type} records")
        
        if content_type == "series":
            return self._extract_batch_series_enrichment(input_df)
        else:
            return self._extract_batch_movie_enrichment(input_df)
    
    def _extract_batch_series_enrichment(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Extract series enrichment in batches, grouping episodes by series"""
        self.logger.info(f"Starting batch series enrichment with grouping strategy")
        
        # Group episodes by series (serial_parent_id)
        series_groups = input_df.groupby('id')
        unique_series = len(series_groups)
        
        self.logger.info(f"Found {unique_series} unique series from {len(input_df)} episodes")
        
        # Process series in batches
        batch_size = self.config.processing.batch_size
        series_list = list(series_groups)
        total_batches = (len(series_list) + batch_size - 1) // batch_size
        
        all_enriched_data = []
        total_successful = 0
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(series_list))
            batch_series = series_list[start_idx:end_idx]
            
            self.logger.info(f"Processing series batch {batch_num + 1}/{total_batches} ({len(batch_series)} series)")
            
            # Process each series in the batch
            for series_id, episodes_df in batch_series:
                series_title = episodes_df.iloc[0].get('title', 'Unknown')
                episode_count = len(episodes_df)
                
                self.logger.info(f"Processing series {series_id}: '{series_title}' ({episode_count} episodes)")
                
                # Create series record for enrichment
                series_record = {
                    'title': series_title,
                    'titleEn': episodes_df.iloc[0].get('titleEn', ''),
                    'ImdbId': episodes_df.iloc[0].get('ImdbId', ''),
                    'serial_parent_id': series_id
                }
                
                # Enrich the series once
                result = self.orchestrator.enrich_record(series_record, "series")
                
                # Apply enrichment to all episodes of this series
                for _, episode_row in episodes_df.iterrows():
                    enriched_record = self._create_enriched_record(episode_row, result, "series")
                    all_enriched_data.append(enriched_record)
                
                if result.success:
                    total_successful += episode_count
                    self.logger.info(f"Successfully enriched series '{series_title}' - applied to {episode_count} episodes")
                else:
                    self.logger.warning(f"Failed to enrich series '{series_title}' - applied to {episode_count} episodes")
            
            self.logger.info(f"Series batch {batch_num + 1} completed")
        
        # Log final summary
        success_rate = total_successful / len(input_df) if len(input_df) > 0 else 0
        self.logger.info(f"Batch series enrichment completed: {total_successful}/{len(input_df)} episodes successful ({success_rate:.2%})")
        
        return pd.DataFrame(all_enriched_data)
    
    def _extract_batch_movie_enrichment(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Extract movie enrichment in batches (individual processing)"""
        self.logger.info(f"Starting batch movie enrichment (individual processing)")
        
        batch_size = self.config.processing.batch_size
        total_batches = (len(input_df) + batch_size - 1) // batch_size
        
        all_enriched_data = []
        total_successful = 0
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(input_df))
            batch_df = input_df.iloc[start_idx:end_idx]
            
            self.logger.info(f"Processing movie batch {batch_num + 1}/{total_batches} ({len(batch_df)} records)")
            
            # Process batch
            batch_enriched = self._process_batch(batch_df, "movie")
            all_enriched_data.extend(batch_enriched)
            
            # Count successful enrichments in this batch
            batch_successful = sum(1 for record in batch_enriched if record.get("enrichment_success", False))
            total_successful += batch_successful
            
            self.logger.info(f"Movie batch {batch_num + 1} completed: {batch_successful}/{len(batch_df)} successful")
        
        # Log final summary
        success_rate = total_successful / len(input_df) if len(input_df) > 0 else 0
        self.logger.info(f"Batch movie enrichment completed: {total_successful}/{len(input_df)} successful ({success_rate:.2%})")
        
        return pd.DataFrame(all_enriched_data)
    
    def _process_batch(self, batch_df: pd.DataFrame, content_type: str) -> List[Dict[str, Any]]:
        """Process a single batch of records"""
        batch_enriched = []
        
        for index, row in batch_df.iterrows():
            # Enrich the record
            result = self.orchestrator.enrich_record(row.to_dict(), content_type)
            
            # Create enriched record
            enriched_record = self._create_enriched_record(row, result, content_type)
            batch_enriched.append(enriched_record)
            
            # Record metrics
            self._record_extraction(f"batch_imdb_enrichment_{content_type}", 1, result.processing_time)
        
        return batch_enriched
    
    def _create_enriched_record(self, original_row: pd.Series, enrichment_result, content_type: str) -> Dict[str, Any]:
        """Create enriched record (same as IMDBEnrichmentExtractor)"""
        # This is the same implementation as in IMDBEnrichmentExtractor
        # In a real implementation, you might want to extract this to a shared utility class
        
        enriched = {
            # Original provider data
            "provider_id": original_row.get("id", ""),
            "provider_title_persian": original_row.get("title", ""),
            "provider_title_english": original_row.get("titleEn", ""),
            "provider_url": original_row.get("url", ""),
            "provider_external_link": original_row.get("externalLink", ""),
            "provider_imdb_id": original_row.get("ImdbId", ""),
            "content_type": content_type,
            
            # Enrichment metadata
            "enrichment_success": enrichment_result.success,
            "enrichment_method": enrichment_result.method_used,
            "enrichment_confidence": enrichment_result.confidence_score,
            "enrichment_processing_time": enrichment_result.processing_time,
            "enrichment_error": enrichment_result.error_message if not enrichment_result.success else "",
        }
        
        # Add IMDB data if enrichment was successful
        if enrichment_result.success and enrichment_result.data:
            imdb_data = enrichment_result.data
            
            # Extract rating data
            rating_data = imdb_data.get("rating", {})
            aggregate_rating = rating_data.get("aggregateRating", "NAN") if rating_data else "NAN"
            vote_count = rating_data.get("voteCount", "NAN") if rating_data else "NAN"
            
            # Extract primary image data
            primary_image = imdb_data.get("primaryImage", {})
            cover_url = primary_image.get("url", "NAN") if primary_image else "NAN"
            cover_width = primary_image.get("width", "NAN") if primary_image else "NAN"
            cover_height = primary_image.get("height", "NAN") if primary_image else "NAN"
            
            # Extract directors
            directors = imdb_data.get("directors", [])
            directors_list = [f"{d.get('displayName', '')} ({d.get('id', '')})" for d in directors] if directors else []
            directors_str = "; ".join(directors_list) if directors_list else "NAN"
            
            # Extract writers
            writers = imdb_data.get("writers", [])
            writers_list = [f"{w.get('displayName', '')} ({w.get('id', '')})" for w in writers] if writers else []
            writers_str = "; ".join(writers_list) if writers_list else "NAN"
            
            # Extract stars
            stars = imdb_data.get("stars", [])
            stars_list = [f"{s.get('displayName', '')} ({s.get('id', '')})" for s in stars] if stars else []
            stars_str = "; ".join(stars_list) if stars_list else "NAN"
            
            # Extract origin countries
            origin_countries = imdb_data.get("originCountries", [])
            countries_list = [f"{c.get('name', '')} ({c.get('code', '')})" for c in origin_countries] if origin_countries else []
            countries_str = "; ".join(countries_list) if countries_list else "NAN"
            
            # Extract spoken languages
            spoken_languages = imdb_data.get("spokenLanguages", [])
            languages_list = [f"{l.get('name', '')} ({l.get('code', '')})" for l in spoken_languages] if spoken_languages else []
            languages_str = "; ".join(languages_list) if languages_list else "NAN"
            
            # Extract interests
            interests = imdb_data.get("interests", [])
            interests_list = [f"{i.get('name', '')} ({i.get('id', '')})" for i in interests] if interests else []
            interests_str = "; ".join(interests_list) if interests_list else "NAN"
            
            enriched.update({
                "imdb_id": imdb_data.get("id", "NAN"),
                "imdb_type": imdb_data.get("type", "NAN"),
                "imdb_primary_title": imdb_data.get("primaryTitle", "NAN"),
                "imdb_original_title": imdb_data.get("originalTitle", "NAN"),
                "imdb_start_year": imdb_data.get("startYear", "NAN"),
                "imdb_end_year": imdb_data.get("endYear", "NAN"),
                "imdb_genres": ", ".join(imdb_data.get("genres", [])) if imdb_data.get("genres") else "NAN",
                "imdb_rating": aggregate_rating,
                "imdb_votes": vote_count,
                "imdb_cover_url": cover_url,
                "imdb_cover_width": cover_width,
                "imdb_cover_height": cover_height,
                "imdb_plot": imdb_data.get("plot", "NAN"),
                "imdb_directors": directors_str,
                "imdb_writers": writers_str,
                "imdb_stars": stars_str,
                "imdb_origin_countries": countries_str,
                "imdb_spoken_languages": languages_str,
                "imdb_interests": interests_str,
            })
        else:
            # Add NAN values for failed enrichments
            imdb_fields = [
                "imdb_id", "imdb_type", "imdb_primary_title", "imdb_original_title",
                "imdb_start_year", "imdb_end_year", "imdb_genres", "imdb_rating", 
                "imdb_votes", "imdb_cover_url", "imdb_cover_width", "imdb_cover_height",
                "imdb_plot", "imdb_directors", "imdb_writers", "imdb_stars",
                "imdb_origin_countries", "imdb_spoken_languages", "imdb_interests"
            ]
            for field in imdb_fields:
                enriched[field] = "NAN"
        
        # Add series-specific fields if applicable
        if content_type == "series":
            enriched.update({
                "series_id": original_row.get("series_id", ""),
                "episode_no": original_row.get("episodeNo", ""),
                "season_no": original_row.get("seasonNo", ""),
            })
        
        return enriched


class DatabaseEnrichmentExtractor(BaseExtractor):
    """Extract Database data for enrichment"""
    
    def __init__(self, config: FilimoEnrichmentConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
        self.orchestrator = EnrichmentOrchestrator(config)
    
    def extract(self, source_name: str, **kwargs) -> pd.DataFrame:
        """Extract Database enrichment data"""
        if source_name == "database_enrichment":
            return self._extract_database_enrichment(**kwargs)
        else:
            raise ValueError(f"Unknown enrichment source: {source_name}")

    def _extract_database_enrichment(self, input_df: pd.DataFrame, content_type: str = "movie") -> pd.DataFrame:
        """Extract IMDB data for given input DataFrame"""
        self.logger.info(f"Starting Database enrichment for {len(input_df)} {content_type} records")
        
        if content_type == "series":
            return self._extract_series_enrichment(input_df)
        else:
            return self._extract_movie_enrichment(input_df)

    def _extract_series_enrichment(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Extract Database data for series, grouping episodes by series"""
        self.logger.info(f"Starting series enrichment with grouping strategy")
        
        # Group episodes by series (serial_parent_id)
        series_groups = input_df.groupby('id')
        unique_series = len(series_groups)
        
        self.logger.info(f"Found {unique_series} unique series from {len(input_df)} episodes")
        
        enriched_data = []
        successful_enrichments = 0
        total_processing_time = 0.0
        
        for series_id, episodes_df in series_groups:
            # Get series title from first episode (all episodes should have same series title)
            series_title = episodes_df.iloc[0].get('title', 'Unknown')
            episode_count = len(episodes_df)
            
            self.logger.info(f"Database Processing series {series_id}: '{series_title}' ({episode_count} episodes)")
            
            # Create series record for enrichment
            series_record = {
                'title': series_title,
                'titleEn': episodes_df.iloc[0].get('titleEn', ''),
                'ImdbId': episodes_df.iloc[0].get('ImdbId', ''),
                'serial_parent_id': series_id
            }
            
            # Enrich the series once
            result = self.orchestrator.enrich_record(series_record, "series", use_db=True)
            total_processing_time += result.processing_time
            
            # Apply enrichment to all episodes of this series
            for _, episode_row in episodes_df.iterrows():
                enriched_record = self._create_enriched_record(episode_row, result, "series")
                enriched_data.append(enriched_record)
            
            if result.success:
                successful_enrichments += episode_count
                self.logger.info(f"Successfully enriched series '{series_title}' using {result.method_used} - applied to {episode_count} episodes")
            else:
                self.logger.warning(f"Failed to enrich series '{series_title}' - {result.error_message} - applied to {episode_count} episodes")
            
            # Record metrics for this series
            self._record_extraction(f"imdb_enrichment_series", episode_count, result.processing_time)
        
        # Calculate success rate and log summary
        success_rate = successful_enrichments / len(input_df) if len(input_df) > 0 else 0
        avg_processing_time = total_processing_time / unique_series if unique_series > 0 else 0
        
        self.logger.info(f"Series enrichment completed:")
        self.logger.info(f"  - Unique series processed: {unique_series}")
        self.logger.info(f"  - Total episodes processed: {len(input_df)}")
        self.logger.info(f"  - Successful enrichments: {successful_enrichments}")
        self.logger.info(f"  - Success rate: {success_rate:.2%}")
        self.logger.info(f"  - Average processing time per series: {avg_processing_time:.2f} seconds")
        
        return pd.DataFrame(enriched_data)
    
    def _extract_movie_enrichment(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Extract IMDB data for movies (individual processing)"""
        self.logger.info(f"Starting movie enrichment (individual processing)")
        
        enriched_data = []
        successful_enrichments = 0
        total_processing_time = 0.0
        
        for index, row in input_df.iterrows():
            self.logger.info(f"Processing {index + 1}/{len(input_df)}: {row.get('title', 'Unknown')}")
            
            # Enrich the record
            row_dict = {
                k: (None if (pd.isna(v) or (isinstance(v, str) and v.strip().lower() == "nan")) else v)
                for k, v in row.to_dict().items()
            }
            result = self.orchestrator.enrich_record(row_dict, "movie", use_db=True)
            total_processing_time += result.processing_time
            
            # Create enriched record
            enriched_record = self._create_enriched_record(row, result, "movie")
            enriched_data.append(enriched_record)
            
            if result.success:
                successful_enrichments += 1
                self.logger.debug(f"Successfully enriched: {row.get('title', 'Unknown')} using {result.method_used}")
            else:
                self.logger.warning(f"Failed to enrich: {row.get('title', 'Unknown')} - {result.error_message}")
            
            # Record metrics for this record
            self._record_extraction(f"database_enrichment_movie", 1, result.processing_time)
        
        # Calculate success rate and log summary
        success_rate = successful_enrichments / len(input_df) if len(input_df) > 0 else 0
        avg_processing_time = total_processing_time / len(input_df) if len(input_df) > 0 else 0
        
        self.logger.info(f"Movie enrichment completed:")
        self.logger.info(f"  - Records processed: {len(input_df)}")
        self.logger.info(f"  - Successful enrichments: {successful_enrichments}")
        self.logger.info(f"  - Success rate: {success_rate:.2%}")
        self.logger.info(f"  - Average processing time: {avg_processing_time:.2f} seconds")
        
        return pd.DataFrame(enriched_data)

    def _create_enriched_record(self, original_row: pd.Series, enrichment_result, content_type: str) -> Dict[str, Any]:
        """Create enriched record combining original data with Database data"""
        enriched = {
            # Original provider data
            "provider_id": original_row.get("id", ""),
            "provider_title_persian": original_row.get("title", ""),
            "provider_title_english": original_row.get("titleEn", ""),
            "provider_url": original_row.get("url", ""),
            "provider_external_link": original_row.get("externalLink", ""),
            "provider_imdb_id": original_row.get("ImdbId", ""),
            "content_type": content_type,
            
            # Enrichment metadata
            "enrichment_success": enrichment_result.success,
            "enrichment_method": enrichment_result.method_used,
            "enrichment_confidence": enrichment_result.confidence_score,
            "enrichment_processing_time": enrichment_result.processing_time,
            "enrichment_error": enrichment_result.error_message if not enrichment_result.success else "",
        }
        
        # Add Database data if enrichment was successful
        if enrichment_result.success and enrichment_result.data:
            db_data = enrichment_result.data
            enriched.update({
                "db_huma_id": db_data.get("huma_id", "NAN"),
                "db_imdb_id": db_data.get("imdb_id", "NAN"),
                "db_tmdb_id": db_data.get("tmdb_id", "NAN"),
                "db_title": db_data.get("title", "NAN"),
                "db_title_fa": db_data.get("title_fa", "NAN"),
                "db_title_enfa": db_data.get("title_enfa", "NAN"),
                "db_year": db_data.get("year", "NAN"),
                "db_release_date": db_data.get("release_data", "NAN"),
                "db_cover": db_data.get("cover", "NAN"),
                "db_backdrop": db_data.get("backdrop", "NAN"),
                "db_plot": db_data.get("plot", "NAN"),
                "db_plot_fa": db_data.get("plot_fa", "NAN"),
                "db_rate": db_data.get("rate", "NAN"),
                "db_vote_count": db_data.get("vote_count", "NAN"),
                "db_mpaa": db_data.get("mpaa", "NAN"),
                "db_duration": db_data.get("duration", "NAN"),
                "db_series_years": db_data.get("series_years", "NAN"),
                "db_movie_status_id": db_data.get("movie_status_id", "NAN"),
                "db_movie_type_id": db_data.get("movie_type_id", "NAN"),
                "db_price": db_data.get("price", "NAN"),
                "db_access_type": db_data.get("access_type", "NAN"),
                "db_created_at": db_data.get("created_at", "NAN"),
                "db_updated_at": db_data.get("updated_at", "NAN"),
            })
        else:
            # Add NAN values for failed enrichments
            db_fields = [
                "db_huma_id", "db_imdb_id", "db_tmdb_id", "db_title", "db_title_fa", "db_title_enfa",
                "db_year", "db_release_date", "db_cover", "db_backdrop", "db_plot", "db_plot_fa",
                "db_rate", "db_vote_count", "db_mpaa", "db_duration", "db_series_years",
                "db_movie_status_id", "db_movie_type_id", "db_price", "db_access_type",
                "db_created_at", "db_updated_at"
            ]
            for field in db_fields:
                enriched[field] = "NAN"
        
        # Add series-specific fields if applicable
        if content_type == "series":
            enriched.update({
                "series_id": original_row.get("series_id", ""),
                "episode_no": original_row.get("episodeNo", ""),
                "season_no": original_row.get("seasonNo", ""),
            })
        
        return enriched
