import argparse
import json
import pandas as pd
from typing import Dict, Optional
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('match_engine')

class MatchEngine:
    def __init__(self, enrichment_report_path: str):
        self.enrichment_data = self._load_enrichment_data(enrichment_report_path)
        
    def _load_enrichment_data(self, path: str) -> Dict:
        """Load enrichment data from JSON file"""
        logger.info(f"Loading enrichment data from {path}")
        with open(path, 'r') as f:
            return json.load(f)
    
    def select_best_imdb_id(self, content: Dict) -> Optional[str]:
        """
        Select the best IMDb ID according to the flowchart logic:
        1. Check if database record exists and has IMDb ID
        2. Check if provider has valid IMDb ID
        3. If none found, return None (NaN)
        """
        try:
            content_id = content['provider'].get('id', 'unknown')
            provider_imdb_id = content['provider'].get('imdb_id')
            if provider_imdb_id == 'NAN':
                provider_imdb_id = None
            
            # Check if database record exists and has IMDb ID
            if 'database' in content and content['database'].get('imdb_id'):
                imdb_id = content['database']['imdb_id']
                
                imdb_id = imdb_id.zfill(7)

                logger.debug(f"Using database IMDb ID {imdb_id} for content {content_id}")
                return imdb_id, "database"
            
            # Check if provider has IMDb ID and it's verified by IMDb API

            elif 'imdb' in content and content['imdb'].get('imdb_id') == provider_imdb_id:
                logger.debug(f"Using Provider IMDb ID {provider_imdb_id} for content {content_id}")
                return provider_imdb_id, "provider"
            
            elif  'imdb' in content and content['provider'].get('imdb_id') is None :
                logger.debug(f"Using IMDb API IMDb ID {content['imdb'].get('imdb_id')} for content {content_id}")
                return content['imdb'].get('imdb_id'), "imdb_api"
            
            elif provider_imdb_id and 'imdb' not in content:
                error_msg = f"IMDb ID not verified by IMDb API for content {content_id}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            else:
                # If no valid IMDb ID found, return None (will be converted to NaN in DataFrame)
                logger.debug(f"No valid IMDb ID found for content {content_id}")
                return None, None
                # This is the "wrong condition" from flowchart
            
        except Exception as e:
            logger.error(f"Error processing content {content['provider'].get('id', 'unknown')}: {str(e)}")
            return None, None

    def process_content(self) -> pd.DataFrame:
        """Process all content and create output DataFrame with required structure"""
        logger.info("Processing content records")
        
        records = []
        for item in self.enrichment_data['data']:
            try:
                provider_data = item['provider']
                imdb_id, imdb_id_source = self.select_best_imdb_id(item)
                record = {
                    'Id': provider_data.get('id'),
                    'imdbId': imdb_id,
                    'imdbIdSource': imdb_id_source,
                    'title': provider_data.get('title'),
                    'titleEn': provider_data.get('title_en'),
                    'episodeNo': provider_data.get('episode_no'),
                    'seasonNo': provider_data.get('season_no'),
                    'url': provider_data.get('url'),
                    'sourceId': provider_data.get('id'),  # Using provider ID as source ID
                    'externalLink': provider_data.get('external_link'),
                    'type': provider_data.get('type'),
                    
                    # Optional fields
                    'cover': provider_data.get('cover'),
                    'backdrop': provider_data.get('backdrop'),
                    'plot': provider_data.get('plot'),
                    
                    # Fixed fields
                    'fileName': 'play',
                    'accessType': 'free',
                    'price': 0,
                    'quality': 'auto'
                }
                records.append(record)
                
            except Exception as e:
                logger.error(f"Error processing record: {str(e)}")
                continue
        
        df = pd.DataFrame(records)
        logger.info(f"Processed {len(df)} records successfully")
        
        # Basic validation of output
        missing_required = df[['Id', 'title', 'titleEn', 'url', 'type', 'imdbId']].isnull().sum()
        if missing_required.any():
            logger.warning(f"Missing required fields in some records:\n{missing_required[missing_required > 0]}")
            
        return df

def main():
    parser = argparse.ArgumentParser(description='Match Engine for IMDb ID selection')
    parser.add_argument('--match-engine', action='store_true', help='Run the match engine')
    parser.add_argument('--input-file', required=True, help='Path to enrichment quality report JSON file')
    parser.add_argument('--output-file', required=True, help='Path to output CSV file')
    args = parser.parse_args()
    
    if args.match_engine:
        try:
            logger.info("Starting Match Engine")
            
            # Validate input file exists
            if not Path(args.input_file).exists():
                raise FileNotFoundError(f"Input file not found: {args.input_file}")
            
            # Create output directory if it doesn't exist
            output_path = Path(args.output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            engine = MatchEngine(args.input_file)
            results_df = engine.process_content()
            results_df.to_csv(args.output_file, index=False)
            logger.info(f"Results saved to {args.output_file}")
            
            # Print summary statistics
            logger.info("\nProcessing Summary:")
            logger.info(f"Total records processed: {len(results_df)}")
            logger.info(f"Records with IMDb ID: {results_df['imdbId'].notna().sum()}")
            logger.info(f"Records without IMDb ID: {results_df['imdbId'].isna().sum()}")
            
        except Exception as e:
            logger.error(f"Match Engine failed: {str(e)}")
            raise

if __name__ == "__main__":
    main()
