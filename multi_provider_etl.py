#!/usr/bin/env python3
"""
Multi-Provider ETL Framework
Main orchestrator for managing multiple ETL providers.
"""

import argparse
import sys
import os
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
from core.base_pipeline import registry, BasePipeline, BaseConfig
from providers.filimo.config import FilimoConfig, FilimoConfigPresets
from providers.filimo.pipeline import FilimoPipeline
from providers.filimo.enhanced_pipeline import (
    EnhancedFilimoPipeline, 
    create_development_pipeline, 
    create_production_pipeline
)
from providers.filimo.enrichment.config import FilimoEnrichmentConfig, EnrichmentConfigPresets


class MultiProviderETLOrchestrator:
    """Orchestrator for managing multiple ETL providers"""
    
    def __init__(self):
        self.registry = registry
        self._register_providers()
    
    def _register_providers(self):
        """Register all available providers"""
        # Register Filimo provider
        self.registry.register_provider("filimo", FilimoPipeline, FilimoConfig)
        # Register Enhanced Filimo provider
        self.registry.register_provider("filimo_enhanced", EnhancedFilimoPipeline, FilimoConfig)
    
    def list_providers(self) -> List[str]:
        """List all available providers"""
        return self.registry.list_providers()
    
    def create_pipeline(self, provider_name: str, config: BaseConfig = None) -> BasePipeline:
        """Create pipeline for specific provider"""
        return self.registry.create_pipeline(provider_name, config)
    
    def run_provider(self, provider_name: str, config: BaseConfig = None) -> Dict[str, Any]:
        """Run ETL pipeline for specific provider"""
        pipeline = self.create_pipeline(provider_name, config)
        return pipeline.run()
    
    def run_multiple_providers(self, provider_configs: Dict[str, BaseConfig]) -> Dict[str, Dict[str, Any]]:
        """Run ETL pipelines for multiple providers"""
        results = {}
        
        for provider_name, config in provider_configs.items():
            try:
                print(f"\n🚀 Running {provider_name} provider...")
                result = self.run_provider(provider_name, config)
                results[provider_name] = result
                print(f"✅ {provider_name} completed successfully!")
            except Exception as e:
                print(f"❌ {provider_name} failed: {e}")
                results[provider_name] = {
                    "status": "failed",
                    "error": str(e)
                }
        
        return results


def aggregate_results(results_dir: str = "results") -> Dict[str, Any]:
    """
    Aggregate all results from the results directory into a structured JSON format.
    
    Args:
        results_dir: Path to the results directory
        
    Returns:
        Dictionary containing aggregated results
    """
    results_path = Path(results_dir)
    if not results_path.exists():
        raise FileNotFoundError(f"Results directory not found: {results_dir}")
    
    aggregated_data = []
    
    # Read provider data (final movies and series)
    provider_files = {
        'movies': results_path / 'filimo_final_movies.csv',
        'series': results_path / 'filimo_final_series.csv'
    }
    
    # Read enrichment data
    enrichment_files = {
        'movies': results_path / 'filimo_enriched_movies.csv',
        'series': results_path / 'filimo_enriched_series.csv'
    }
    
    # Read database enrichment data if available
    db_enrichment_files = {
        'movies': results_path / 'filimo_enriched_movies_with_db.csv',
        'series': results_path / 'filimo_enriched_series_with_db.csv'
    }
    
    for content_type in ['movies', 'series']:
        print(f"🔄 Processing {content_type}...")
        
        # Load provider data
        provider_file = provider_files[content_type]
        if not provider_file.exists():
            print(f"⚠️  Provider file not found: {provider_file}")
            continue
            
        try:
            provider_df = pd.read_csv(provider_file)
            print(f"   📊 Loaded {len(provider_df)} {content_type} from provider data")
        except Exception as e:
            print(f"❌ Error loading provider data: {e}")
            continue
        
        # Load enrichment data
        enrichment_file = enrichment_files[content_type]
        enrichment_df = None
        if enrichment_file.exists():
            try:
                enrichment_df = pd.read_csv(enrichment_file)
                print(f"   🎯 Loaded {len(enrichment_df)} {content_type} from enrichment data")
            except Exception as e:
                print(f"⚠️  Error loading enrichment data: {e}")
        
        # Load database enrichment data
        db_enrichment_file = db_enrichment_files[content_type]
        db_enrichment_df = None
        if db_enrichment_file.exists():
            try:
                db_enrichment_df = pd.read_csv(db_enrichment_file)
                print(f"   🗄️  Loaded {len(db_enrichment_df)} {content_type} from database enrichment data")
            except Exception as e:
                print(f"⚠️  Error loading database enrichment data: {e}")
        
        # Process each provider record
        for _, row in provider_df.iterrows():
            # Convert numpy types to Python types for JSON serialization
            def safe_get(value, default=''):
                if pd.isna(value) or value is None:
                    return default
                # Convert numpy types to Python types
                if hasattr(value, 'item'):  # numpy scalar
                    return value.item()
                return value
            

            imdb_id = safe_get(row.get('ImdbId'), '')
            if imdb_id.startswith('tt'):
                imdb_id = str(imdb_id[2:])
            record = {
                "provider": {
                    "id": safe_get(row.get('id'), ''),
                    "title": safe_get(row.get('title'), ''),
                    "title_en": safe_get(row.get('titleEn'), ''),
                    "file_name": safe_get(row.get('fileName'), ''),
                    "url": safe_get(row.get('url'), ''),
                    "imdb_id": safe_get(row.get('ImdbId'), ''),
                    "source_id": safe_get(row.get('sourceId'), ''),
                    "external_link": safe_get(row.get('externalLink'), ''),
                    "type": safe_get( 'series' if row.get('type') == 2 else 'movie', ''),
                    "episode_no": int(row.get('episodeNo', 0)) if content_type == "series" and not pd.isna(row.get('episodeNo', 0)) else None,
                    "season_no": int(row.get('seasonNo', 0)) if content_type == "series" and not pd.isna(row.get('seasonNo', 0)) else None,
                    "access_type": safe_get(row.get('accessType'), ''),
                    "price": float(row.get('price', 0)) if not pd.isna(row.get('price', 0)) else 0,
                    "quality": safe_get(row.get('quality'), ''),
                    "cover": safe_get(row.get('cover'), ''),
                    "backdrop": safe_get(row.get('backdrop'), ''),
                    "plot": safe_get(row.get('plot'), '')
                }
            }
            
            # Add IMDB data if available
            if enrichment_df is not None:
                # Try to match by provider ID
                enrichment_match = enrichment_df[enrichment_df['provider_id'] == row.get('id')]
                if not enrichment_match.empty:
                    enrichment_row = enrichment_match.iloc[0]
                    if enrichment_row.get('enrichment_success', False):
                        # Convert numpy types to Python types for JSON serialization
                        def safe_get(value, default=''):
                            if pd.isna(value) or value is None:
                                return default
                            # Convert numpy types to Python types
                            if hasattr(value, 'item'):  # numpy scalar
                                return value.item()
                            return value
                        
                        imdb_id = safe_get(enrichment_row.get('imdb_id'), '')
                        if imdb_id.startswith('tt'):
                            imdb_id = str(imdb_id[2:])

                        record["imdb"] = {
                            "imdb_id": imdb_id,
                            "type": safe_get(enrichment_row.get('imdb_type'), ''),
                            "primary_title": safe_get(enrichment_row.get('imdb_primary_title'), ''),
                            "original_title": safe_get(enrichment_row.get('imdb_original_title'), ''),
                            "start_year": safe_get(enrichment_row.get('imdb_start_year'), ''),
                            "end_year": safe_get(enrichment_row.get('imdb_end_year'), ''),
                            "genres": safe_get(enrichment_row.get('imdb_genres'), ''),
                            "rating": safe_get(enrichment_row.get('imdb_rating'), ''),
                            "votes": safe_get(enrichment_row.get('imdb_votes'), ''),
                            "cover_url": safe_get(enrichment_row.get('imdb_cover_url'), ''),
                            "cover_width": safe_get(enrichment_row.get('imdb_cover_width'), ''),
                            "cover_height": safe_get(enrichment_row.get('imdb_cover_height'), ''),
                            "plot": safe_get(enrichment_row.get('imdb_plot'), ''),
                            "directors": safe_get(enrichment_row.get('imdb_directors'), ''),
                            "writers": safe_get(enrichment_row.get('imdb_writers'), ''),
                            "stars": safe_get(enrichment_row.get('imdb_stars'), ''),
                            "origin_countries": safe_get(enrichment_row.get('imdb_origin_countries'), ''),
                            "spoken_languages": safe_get(enrichment_row.get('imdb_spoken_languages'), ''),
                            "interests": safe_get(enrichment_row.get('imdb_interests'), ''),
                            "enrichment_method": safe_get(enrichment_row.get('enrichment_method'), ''),
                            "confidence": float(enrichment_row.get('enrichment_confidence', 0)) if not pd.isna(enrichment_row.get('enrichment_confidence', 0)) else 0,
                            "data_quality_score": float(enrichment_row.get('data_quality_score', 0)) if not pd.isna(enrichment_row.get('data_quality_score', 0)) else 0
                        }
            
            # Add database data if available
            if db_enrichment_df is not None:
                # Try to match by provider ID
                db_match = db_enrichment_df[db_enrichment_df['provider_id'] == row.get('id')]
                if not db_match.empty:
                    db_row = db_match.iloc[0]
                    if db_row.get('enrichment_success', False):
                        # Convert numpy types to Python types for JSON serialization
                        def safe_get(value, default=''):
                            if pd.isna(value) or value is None:
                                return default
                            # Convert numpy types to Python types
                            if hasattr(value, 'item'):  # numpy scalar
                                return value.item()
                            return value

                        def safe_convert_id(value):
                            """Safely convert ID values handling all NaN cases"""
                            if pd.isna(value) or value is None or str(value).strip() == '' or str(value).lower() == 'nan':
                                return ''
                            try:
                                float_val = float(value)
                                if pd.isna(float_val) or float_val == 0:
                                    return ''
                                return str(int(float_val))
                            except (ValueError, TypeError):
                                return ''
                        

                        record["database"] = {
                            "huma_id": safe_convert_id(safe_get(db_row.get('db_huma_id'), '')),
                            "imdb_id": safe_convert_id(safe_get(db_row.get('db_imdb_id'), '')),
                            "tmdb_id": safe_convert_id(safe_get(db_row.get('db_tmdb_id'), '')),
                            "title": safe_get(db_row.get('db_title'), ''),
                            "title_fa": safe_get(db_row.get('db_title_fa'), ''),
                            "title_enfa": safe_get(db_row.get('db_title_enfa'), ''),
                            "year": safe_get(db_row.get('db_year'), ''),
                            "release_date": safe_get(db_row.get('db_release_date'), ''),
                            "cover": safe_get(db_row.get('db_cover'), ''),
                            "backdrop": safe_get(db_row.get('db_backdrop'), ''),
                            "plot": safe_get(db_row.get('db_plot'), ''),
                            "plot_fa": safe_get(db_row.get('db_plot_fa'), ''),
                            "rate": safe_get(db_row.get('db_rate'), ''),
                            "vote_count": safe_get(db_row.get('db_vote_count'), ''),
                            "mpaa": safe_get(db_row.get('db_mpaa'), ''),
                            "duration": safe_get(db_row.get('db_duration'), ''),
                            "series_years": safe_get(db_row.get('db_series_years'), ''),
                            "movie_status_id": safe_get(db_row.get('db_movie_status_id'), ''),
                            "movie_type_id": safe_get(db_row.get('db_movie_type_id'), ''),
                            "price": safe_get(db_row.get('db_price'), ''),
                            "access_type": safe_get(db_row.get('db_access_type'), ''),
                            "created_at": safe_get(db_row.get('db_created_at'), ''),
                            "updated_at": safe_get(db_row.get('db_updated_at'), ''),
                            "enrichment_method": safe_get(db_row.get('enrichment_method'), ''),
                            "confidence": float(db_row.get('enrichment_confidence', 0)) if not pd.isna(db_row.get('enrichment_confidence', 0)) else 0,
                            "database_quality_score": float(db_row.get('database_quality_score', 0)) if not pd.isna(db_row.get('database_quality_score', 0)) else 0
                        }
            
            aggregated_data.append(record)
    
    # Calculate enrichment statistics
    records_with_imdb = [r for r in aggregated_data if "imdb" in r]
    records_with_database = [r for r in aggregated_data if "database" in r]
    
    # Count unique series IDs instead of episodes
    movies = [r for r in aggregated_data if r["provider"]["type"] == "movie"]
    series = [r for r in aggregated_data if r["provider"]["type"] == "series"]
    
    # Get unique series IDs (the first column in the CSV is the series ID)
    unique_series_ids = set()
    for record in series:
        # The series ID is stored in the 'id' field of the provider data
        series_id = record.get('provider', {}).get('id')
        if series_id:
            unique_series_ids.add(series_id)
    
    # Count enrichment methods by content type
    movies_with_imdb = [r for r in records_with_imdb if r["provider"]["type"] == "movie"]
    series_with_imdb = [r for r in records_with_imdb if r["provider"]["type"] == "series"]
    movies_with_database = [r for r in records_with_database if r["provider"]["type"] == "movie"]
    series_with_database = [r for r in records_with_database if r["provider"]["type"] == "series"]
    
    # Count IMDB enrichment methods
    imdb_enrichment_via_id_movies = len([r for r in movies_with_imdb if r["imdb"].get("enrichment_method") == "imdb_id"])
    imdb_enrichment_via_id_series = len([r for r in series_with_imdb if r["imdb"].get("enrichment_method") == "imdb_id"])
    imdb_enrichment_via_title_movies = len([r for r in movies_with_imdb if r["imdb"].get("enrichment_method") == "title_search"])
    imdb_enrichment_via_title_series = len([r for r in series_with_imdb if r["imdb"].get("enrichment_method") == "title_search"])
    
    # Count database enrichment methods
    database_enrichment_movies = len([r for r in movies_with_database if r["database"].get("enrichment_method") == "database_title"])
    database_enrichment_series = len([r for r in series_with_database if r["database"].get("enrichment_method") == "database_title"])
    
    # Count unique series with enrichments
    unique_series_with_imdb = set()
    unique_series_with_database = set()
    
    for record in series_with_imdb:
        series_id = record.get('provider', {}).get('id')
        if series_id:
            unique_series_with_imdb.add(series_id)
    
    for record in series_with_database:
        series_id = record.get('provider', {}).get('id')
        if series_id:
            unique_series_with_database.add(series_id)
    
    return {
        "total_records": len(aggregated_data),
        "movies_count": len(movies),
        "series_count": len(unique_series_ids),  # Count unique series instead of episodes
        "total_episodes": len(series),  # Total episodes for reference
        "records_with_imdb": len(records_with_imdb),
        "records_with_database": len(records_with_database),
        "enrichment_stats": {
            # Total counts (keep existing)
            "imdb_enrichment_via_id": imdb_enrichment_via_id_movies + imdb_enrichment_via_id_series,
            "imdb_enrichment_via_title_search": imdb_enrichment_via_title_movies + imdb_enrichment_via_title_series,
            "database_enrichment_via_title": database_enrichment_movies + database_enrichment_series,
            "total_imdb_enrichments": len(records_with_imdb),
            "total_database_enrichments": len(records_with_database),
            "unique_series_with_imdb": len(unique_series_with_imdb),
            "unique_series_with_database": len(unique_series_with_database),
            # Breakdown by content type
            "movies": {
                "imdb_enrichment_via_id": imdb_enrichment_via_id_movies,
                "imdb_enrichment_via_title_search": imdb_enrichment_via_title_movies,
                "database_enrichment_via_title": database_enrichment_movies,
                "total_imdb_enrichments": len(movies_with_imdb),
                "total_database_enrichments": len(movies_with_database)
            },
            "series": {
                "imdb_enrichment_via_id": imdb_enrichment_via_id_series,
                "imdb_enrichment_via_title_search": imdb_enrichment_via_title_series,
                "database_enrichment_via_title": database_enrichment_series,
                "total_imdb_enrichments": len(unique_series_with_imdb),
                "total_database_enrichments": len(unique_series_with_database)
            }
        },
        "data": aggregated_data
    }


def create_config_from_preset(provider_name: str, preset_name: str) -> BaseConfig:
    """Create configuration from preset"""
    if provider_name in ["filimo", "filimo_enhanced"]:
        if preset_name == "development":
            return FilimoConfigPresets.development()
        elif preset_name == "production":
            return FilimoConfigPresets.production()
        elif preset_name == "testing":
            return FilimoConfigPresets.testing()
        elif preset_name == "performance":
            return FilimoConfigPresets.performance()
        else:
            return FilimoConfig()
    else:
        raise ValueError(f"Unknown provider: {provider_name}")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Multi-Provider ETL Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic ETL Pipeline
  python multi_provider_etl.py --provider filimo                    # Run Filimo ETL with default config
  python multi_provider_etl.py --provider filimo --preset production  # Run with production preset
  
  # Enhanced ETL + Enrichment Pipeline
  python multi_provider_etl.py --provider filimo_enhanced           # Run enhanced pipeline (ETL + Enrichment)
  python multi_provider_etl.py --provider filimo_enhanced --mode complete  # Run complete pipeline
  python multi_provider_etl.py --provider filimo_enhanced --mode etl-only  # Run ETL only
  python multi_provider_etl.py --provider filimo_enhanced --mode enrichment-only  # Run enrichment only
  
  # Enrichment Configuration
  python multi_provider_etl.py --provider filimo_enhanced --enrichment-strategy dual-branch
  python multi_provider_etl.py --provider filimo_enhanced --batch-size 100 --max-workers 4
  
  # General Options
  python multi_provider_etl.py --list-providers                     # List all available providers
  python multi_provider_etl.py --aggregate-results                  # Aggregate all results into JSON
  python multi_provider_etl.py --provider filimo --output-dir /tmp  # Custom output directory
        """
    )
    
    # Provider selection
    parser.add_argument(
        "--provider",
        type=str,
        choices=["filimo", "filimo_enhanced"],
        help="ETL provider to run (filimo: basic ETL, filimo_enhanced: ETL + enrichment)"
    )
    
    parser.add_argument(
        "--list-providers",
        action="store_true",
        help="List all available providers"
    )
    
    # Configuration options
    parser.add_argument(
        "--preset",
        choices=["development", "production", "testing", "performance"],
        help="Configuration preset to use"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Override output directory"
    )
    
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue processing on errors"
    )
    
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip data validation"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level"
    )
    
    parser.add_argument(
        "--log-to-file",
        action="store_true",
        help="Enable file logging"
    )
    
    # Output options
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress non-error output"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show configuration and exit without running"
    )
    
    # Enhanced Pipeline Options
    parser.add_argument(
        "--mode",
        choices=["complete", "etl-only", "enrichment-only", "enrichment-only-with-db"],
        default="complete",
        help="Pipeline execution mode (complete: ETL + enrichment, etl-only: ETL only, enrichment-only: enrichment only, enrichment-only-with-db: enrichment only with database)"
    )
    
    # Enrichment Configuration
    parser.add_argument(
        "--enrichment-strategy",
        choices=["dual-branch", "imdb-only", "title-only"],
        default="dual-branch",
        help="Enrichment strategy (dual-branch: try IMDB ID then title search, imdb-only: use existing IMDB IDs only, title-only: use title search only)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Enrichment batch size (number of records to process in parallel)"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        help="Maximum number of parallel workers for enrichment"
    )
    
    parser.add_argument(
        "--confidence-threshold",
        type=float,
        help="Minimum confidence threshold for enrichment results (0.0 to 1.0)"
    )
    
    parser.add_argument(
        "--api-delay",
        type=float,
        help="Delay between API calls in seconds (rate limiting)"
    )
    
    parser.add_argument(
        "--enable-caching",
        action="store_true",
        help="Enable caching for enrichment API calls"
    )
    
    parser.add_argument(
        "--include-confidence",
        action="store_true",
        help="Include confidence scores in output"
    )
    
    parser.add_argument(
        "--include-metadata",
        action="store_true",
        help="Include processing metadata in output"
    )
    
    parser.add_argument(
        "--generate-quality-report",
        action="store_true",
        help="Generate data quality report"
    )
    
    parser.add_argument(
        "--aggregate-results",
        action="store_true",
        help="Aggregate all results from results/ directory into aggregate-results.json"
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize orchestrator
        orchestrator = MultiProviderETLOrchestrator()
        
        # List providers if requested
        if args.list_providers:
            providers = orchestrator.list_providers()
            print("📋 Available ETL Providers:")
            for provider in providers:
                print(f"  - {provider}")
            return 0
        
        # Handle aggregation if requested
        if args.aggregate_results:
            print("🔄 Aggregating results from results/ directory...")
            try:
                aggregated_data = aggregate_results("results")
                
                # Save to JSON file
                output_file = "aggregate-results.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(aggregated_data, f, ensure_ascii=False, indent=2)
                
                print(f"✅ Aggregation completed successfully!")
                print(f"📊 Summary:")
                print(f"   - Total records: {aggregated_data['total_records']}")
                print(f"   - Movies: {aggregated_data['movies_count']}")
                print(f"   - Unique series: {aggregated_data['series_count']}")
                print(f"   - Total episodes: {aggregated_data.get('total_episodes', 0)}")
                print(f"   - Records with IMDB data: {aggregated_data['records_with_imdb']}")
                print(f"   - Records with database data: {aggregated_data['records_with_database']}")
                
                # Display enrichment statistics
                if 'enrichment_stats' in aggregated_data:
                    stats = aggregated_data['enrichment_stats']
                    print(f"🎯 Enrichment Statistics:")
                    print(f"   📊 Total:")
                    print(f"      - IMDB enrichment via ID: {stats['imdb_enrichment_via_id']}")
                    print(f"      - IMDB enrichment via title search: {stats['imdb_enrichment_via_title_search']}")
                    print(f"      - Database enrichment via title: {stats['database_enrichment_via_title']}")
                    print(f"      - Total IMDB enrichments: {stats['total_imdb_enrichments']}")
                    print(f"      - Total database enrichments: {stats['total_database_enrichments']}")
                    print(f"      - Unique series with IMDB data: {stats['unique_series_with_imdb']}")
                    print(f"      - Unique series with database data: {stats['unique_series_with_database']}")
                    
                    if 'movies' in stats:
                        print(f"   🎬 Movies:")
                        print(f"      - IMDB enrichment via ID: {stats['movies']['imdb_enrichment_via_id']}")
                        print(f"      - IMDB enrichment via title search: {stats['movies']['imdb_enrichment_via_title_search']}")
                        print(f"      - Database enrichment via title: {stats['movies']['database_enrichment_via_title']}")
                        print(f"      - Total IMDB enrichments: {stats['movies']['total_imdb_enrichments']}")
                        print(f"      - Total database enrichments: {stats['movies']['total_database_enrichments']}")
                    
                    if 'series' in stats:
                        print(f"   📺 Series (unique):")
                        print(f"      - IMDB enrichment via ID: {stats['series']['imdb_enrichment_via_id']}")
                        print(f"      - IMDB enrichment via title search: {stats['series']['imdb_enrichment_via_title_search']}")
                        print(f"      - Database enrichment via title: {stats['series']['database_enrichment_via_title']}")
                        print(f"      - Total IMDB enrichments: {stats['series']['total_imdb_enrichments']}")
                        print(f"      - Total database enrichments: {stats['series']['total_database_enrichments']}")
                
                print(f"📁 Output saved to: {output_file}")
                
                return 0
            except Exception as e:
                print(f"❌ Aggregation failed: {e}")
                # if args.verbose:
                import traceback
                traceback.print_exc()
                return 1
        
        # Validate provider selection
        if not args.provider:
            print("❌ Error: --provider is required (use --list-providers to see available providers)")
            return 1
        
        if args.provider not in orchestrator.list_providers():
            print(f"❌ Error: Unknown provider '{args.provider}'")
            print(f"Available providers: {', '.join(orchestrator.list_providers())}")
            return 1
        
        # Create configuration
        if args.preset:
            config = create_config_from_preset(args.provider, args.preset)
            print(f"🔧 Using {args.preset} preset for {args.provider}")
        else:
            config = create_config_from_preset(args.provider, "development")
            print(f"🔧 Using default configuration for {args.provider}")
        
        # Create enhanced pipeline if needed
        if args.provider == "filimo_enhanced":
            # Create enrichment config
            enrichment_config = FilimoEnrichmentConfig()
            
            # Apply enrichment-specific overrides
            if args.enrichment_strategy:
                enrichment_config.processing.enrichment_strategy = args.enrichment_strategy.replace("-", "_")
                print(f"🎯 Enrichment strategy: {args.enrichment_strategy}")
            
            if args.batch_size:
                enrichment_config.processing.batch_size = args.batch_size
                print(f"📦 Batch size: {args.batch_size}")
            
            if args.max_workers:
                enrichment_config.processing.max_parallel_workers = args.max_workers
                print(f"👥 Max workers: {args.max_workers}")
            
            if args.confidence_threshold:
                enrichment_config.processing.confidence_threshold = args.confidence_threshold
                print(f"🎯 Confidence threshold: {args.confidence_threshold}")
            
            if args.api_delay:
                enrichment_config.processing.api_delay_seconds = args.api_delay
                print(f"⏱️ API delay: {args.api_delay}s")
            
            if args.enable_caching:
                enrichment_config.processing.enable_caching = True
                print("💾 Caching enabled")
            
            if args.include_confidence:
                enrichment_config.output.include_confidence_scores = True
                print("📊 Confidence scores included")
            
            if args.include_metadata:
                enrichment_config.output.include_processing_metadata = True
                print("📋 Processing metadata included")
            
            if args.generate_quality_report:
                enrichment_config.output.generate_quality_report = True
                print("📈 Quality report enabled")
            
            # Create enhanced pipeline
            pipeline = EnhancedFilimoPipeline(config, enrichment_config)
        else:
            # Create regular pipeline
            pipeline = orchestrator.create_pipeline(args.provider, config)
        
        # Apply command line overrides
        if args.output_dir:
            config.base_dir = Path(args.output_dir)
            print(f"📁 Output directory: {config.base_dir}")
        
        if args.continue_on_error:
            config.continue_on_error = True
            print("⚠️  Continue on error enabled")
        
        if args.no_validate:
            config.validate_data = False
            print("⚠️  Data validation disabled")
        
        if args.log_level:
            config.log_level = args.log_level
        
        if args.log_to_file:
            config.log_to_file = True
            print(f"📝 File logging enabled: {config.log_path}")
        
        # Show configuration summary
        if not args.quiet:
            print(f"\n📋 Configuration Summary:")
            print(f"   Provider: {config.provider_name} v{config.provider_version}")
            print(f"   Output Directory: {config.base_dir}")
            print(f"   Validate Data: {config.validate_data}")
            print(f"   Continue on Error: {config.continue_on_error}")
            print(f"   Log Level: {config.log_level}")
            print(f"   Log to File: {config.log_to_file}")
        
        # Dry run check
        if args.dry_run:
            print("\n🏃 Dry run mode - configuration validated, exiting without processing")
            return 0
        
        # Run the pipeline
        print(f"\n🚀 Starting {args.provider} Pipeline...")
        
        if args.provider == "filimo_enhanced":
            # Run enhanced pipeline with specified mode
            if args.mode == "complete":
                print("🔄 Running complete pipeline (ETL + Enrichment)...")
                result = pipeline.run_complete()
            elif args.mode == "etl-only":
                print("📊 Running ETL only...")
                result = pipeline.run_etl_only()
            elif args.mode == "enrichment-only-with-db":
                print("🎯 Running enrichment only with database...")
                etl_result = pipeline.run_etl_only()
                result = pipeline.run_enrichment_only_with_db(etl_result)

            elif args.mode == "enrichment-only":
                print("🎯 Running enrichment only...")
                # For enrichment-only, we need ETL results first
                print("⚠️ Note: Enrichment-only mode requires existing ETL results")
                print("   Consider running ETL first or using complete mode")
                etl_result = pipeline.run_etl_only()
                result = pipeline.run_enrichment_only(etl_result)
        else:
            # Run regular ETL pipeline
            result = orchestrator.run_provider(args.provider, config)
        
        # Display results
        if not args.quiet:
            print(f"\n🎉 {args.provider} pipeline completed successfully!")
            
            # Display results based on pipeline type
            if args.provider == "filimo_enhanced":
                print(f"📊 Enhanced Pipeline Results:")
                if "summary" in result:
                    summary = result["summary"]
                    print(f"   - ETL Movies: {summary.get('etl_movies', 0)}")
                    print(f"   - ETL Series: {summary.get('etl_series', 0)}")
                    print(f"   - Enriched Movies: {summary.get('enriched_movies', 0)}")
                    print(f"   - Enriched Series: {summary.get('enriched_series', 0)}")
                    print(f"   - Total Processing Time: {summary.get('total_duration', 0):.2f}s")
                
                if "enrichment_stats" in result:
                    stats = result["enrichment_stats"]
                    print(f"🎯 Enrichment Statistics:")
                    print(f"   - Success Rate: {stats.get('success_rate', 0):.1f}%")
                    print(f"   - Average Confidence: {stats.get('avg_confidence', 0):.2f}")
                    print(f"   - API Calls Made: {stats.get('api_calls', 0)}")
                    print(f"   - Cache Hits: {stats.get('cache_hits', 0)}")
            else:
                print(f"📊 ETL Results:")
                for key, value in result.get("results", {}).items():
                    if value is not None:
                        print(f"   - {key}: {value}")
            
            print(f"⏱️  Duration: {result.get('provider', {}).get('duration', 0):.2f} seconds")
            
            if result.get("metrics_summary"):
                metrics = result["metrics_summary"]
                print(f"📈 Metrics:")
                print(f"   - Errors: {metrics.get('errors', 0)}")
                print(f"   - Warnings: {metrics.get('warnings', 0)}")
            
            if args.verbose and result.get("output_files"):
                print(f"\n📁 Output Files:")
                for file_type, file_path in result["output_files"].items():
                    if file_path:
                        file_size = Path(file_path).stat().st_size if Path(file_path).exists() else 0
                        print(f"   - {file_type}: {file_path} ({file_size:,} bytes)")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrupted by user")
        return 130
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
