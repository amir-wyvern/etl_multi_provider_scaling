#!/usr/bin/env python3
"""
Filimo Enrichment Services
Core services for IMDB API, translation, and data enrichment.
"""

import requests
import time
import logging
import os
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from .config import FilimoEnrichmentConfig


# import logging
# from typing import List, Dict, Any, Optional, Tuple
# from metabase_client import ClientMetabase


try:
    import openai
except ImportError:
    openai = None

@dataclass
class EnrichmentResult:
    """Result of enrichment operation"""
    success: bool
    data: Optional[Dict[str, Any]] = None
    confidence_score: float = 0.0
    method_used: str = ""
    error_message: str = ""
    processing_time: float = 0.0

class IMDBService:
    """IMDB API service with caching and rate limiting"""
    
    def __init__(self, config: FilimoEnrichmentConfig):
        self.config = config
        self.logger = logging.getLogger("filimo_enrichment.IMDBService")
        self.cache = {}
        self.last_request_time = 0
    
    def get_by_id(self, imdb_id: str) -> EnrichmentResult:
        """Get IMDB data by ID"""
        start_time = time.time()
        
        try:
            # Clean and validate IMDB ID
            clean_id = self._clean_imdb_id(imdb_id)
            if not clean_id:
                return EnrichmentResult(
                    success=False,
                    error_message=f"Invalid IMDB ID: {imdb_id}",
                    processing_time=time.time() - start_time
                )
            
            # Rate limiting
            self._enforce_rate_limit()
            
            # Check cache
            cache_key = f"imdb_id_{clean_id}"
            if self.config.processing.enable_caching and cache_key in self.cache:
                self.logger.debug(f"Cache hit for IMDB ID: {clean_id}")
                return EnrichmentResult(
                    success=True,
                    data=self.cache[cache_key],
                    confidence_score=1.0,
                    method_used="cached_imdb_id",
                    processing_time=time.time() - start_time
                )
            
            # API call
            url = f"{self.config.source.imdb_api_url}/titles/{clean_id}"
            response = requests.get(url, timeout=self.config.source.imdb_timeout)
            response.raise_for_status()
            
            data = response.json()
            
            # Validate response
            if not self._validate_imdb_response(data):
                return EnrichmentResult(
                    success=False,
                    error_message="Invalid IMDB API response",
                    processing_time=time.time() - start_time
                )
            
            # Cache result
            if self.config.processing.enable_caching:
                self.cache[cache_key] = data
            
            return EnrichmentResult(
                success=True,
                data=data,
                confidence_score=1.0,
                method_used="imdb_id",
                processing_time=time.time() - start_time
            )
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"IMDB ID lookup failed for {imdb_id}: {e}")
            return EnrichmentResult(
                success=False,
                error_message=f"API request failed: {str(e)}",
                processing_time=time.time() - start_time
            )
        except Exception as e:
            self.logger.error(f"Unexpected error in IMDB ID lookup for {imdb_id}: {e}")
            return EnrichmentResult(
                success=False,
                error_message=f"Unexpected error: {str(e)}",
                processing_time=time.time() - start_time
            )
    
    def search_by_title(self, title: str, content_type: str = "movie") -> EnrichmentResult:
        """Search IMDB by title"""
        start_time = time.time()
        
        try:
            if not title or not title.strip():
                return EnrichmentResult(
                    success=False,
                    error_message="Empty title provided",
                    processing_time=time.time() - start_time
                )
            
            self._enforce_rate_limit()
            
            # Check cache
            cache_key = f"imdb_search_{title}_{content_type}"
            if self.config.processing.enable_caching and cache_key in self.cache:
                self.logger.debug(f"Cache hit for title search: {title}")
                cached_data = self.cache[cache_key]
                best_match = self._find_best_match(cached_data, content_type, title)
                
                # If we found a match, get detailed data using the IMDB ID
                if best_match and best_match.get("id"):
                    detailed_data = self._get_detailed_data_by_id(best_match["id"])
                    if detailed_data:
                        # Use detailed data but keep method as cached_title_search
                        best_match = detailed_data
                
                return EnrichmentResult(
                    success=best_match is not None,
                    data=best_match,
                    confidence_score=self._calculate_confidence(title, best_match) if best_match else 0.0,
                    method_used="cached_title_search",
                    processing_time=time.time() - start_time
                )
            
            # API call
            url = f"{self.config.source.imdb_api_url}/search/titles"
            params = {"query": title.strip()}
            response = requests.get(url, params=params, timeout=self.config.source.imdb_timeout)
            response.raise_for_status()
            
            data = response.json()
            
            # Find best match
            best_match = self._find_best_match(data, content_type, title)
            
            # If we found a match, get detailed data using the IMDB ID
            if best_match and best_match.get("id"):
                detailed_data = self._get_detailed_data_by_id(best_match["id"])
                if detailed_data:
                    # Use detailed data but keep method as title_search
                    best_match = detailed_data
            
            # Cache result
            if self.config.processing.enable_caching:
                self.cache[cache_key] = data
            
            return EnrichmentResult(
                success=best_match is not None,
                data=best_match,
                confidence_score=self._calculate_confidence(title, best_match) if best_match else 0.0,
                method_used="title_search",
                processing_time=time.time() - start_time
            )
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"IMDB title search failed for {title}: {e}")
            return EnrichmentResult(
                success=False,
                error_message=f"API request failed: {str(e)}",
                processing_time=time.time() - start_time
            )
        except Exception as e:
            self.logger.error(f"Unexpected error in IMDB title search for {title}: {e}")
            return EnrichmentResult(
                success=False,
                error_message=f"Unexpected error: {str(e)}",
                processing_time=time.time() - start_time
            )
    
    def _clean_imdb_id(self, imdb_id: str) -> Optional[str]:
        """Clean and validate IMDB ID"""
        if not imdb_id or imdb_id == "NAN" or not str(imdb_id).strip():
            return None
        
        # Convert to string and strip whitespace
        clean_id = str(imdb_id).strip()
        
        # Add 'tt' prefix if not present
        if not clean_id.startswith('tt'):
            clean_id = f"tt{clean_id}"
        
        # Basic validation - should be tt followed by digits
        if not clean_id[2:].isdigit():
            return None
        
        return clean_id
    
    def _validate_imdb_response(self, data: Dict[str, Any]) -> bool:
        """Validate IMDB API response"""
        if not isinstance(data, dict):
            return False
        
        # Check for required fields
        required_fields = ['id', 'primaryTitle']
        return all(field in data for field in required_fields)
    
    def _enforce_rate_limit(self):
        """Enforce rate limiting between API calls"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.config.processing.api_delay_seconds:
            sleep_time = self.config.processing.api_delay_seconds - time_since_last
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _find_best_match(self, search_results: Dict, content_type: str, original_title: str) -> Optional[Dict]:
        """Find the best matching result from search results"""
        # Handle different response formats
        results = search_results.get("results", search_results.get("titles", []))
        if not results or not isinstance(results, list):
            return None
        
        # Filter by content type
        if content_type == "movie":
            filtered = [r for r in results if r.get("type") in self.config.processing.movie_types]
        else:
            filtered = [r for r in results if r.get("type") in self.config.processing.series_types]
        
        if not filtered:
            return None
        
        # Find best match by title similarity
        best_match = None
        best_score = 0
        
        for result in filtered:
            score = self._calculate_title_similarity(original_title, result.get("primaryTitle", ""))
            if score > best_score:
                best_score = score
                best_match = result
        
        return best_match if best_score > self.config.processing.confidence_threshold else None
    
    def _calculate_title_similarity(self, title1: str, title2: str) -> float:
        """Calculate similarity between two titles"""
        if not title1 or not title2:
            return 0.0
        
        title1_lower = title1.lower().strip()
        title2_lower = title2.lower().strip()
        
        # Exact match
        if title1_lower == title2_lower:
            return 1.0
        
        # Substring match
        if title1_lower in title2_lower or title2_lower in title1_lower:
            return 0.8
        
        # Word overlap scoring
        words1 = set(word for word in title1_lower.split() if len(word) > 2)
        words2 = set(word for word in title2_lower.split() if len(word) > 2)
        
        if not words1 or not words2:
            return 0.0
        
        overlap = len(words1.intersection(words2))
        total = len(words1.union(words2))
        
        return overlap / total if total > 0 else 0.0
    
    def _calculate_confidence(self, original_title: str, result: Dict) -> float:
        """Calculate confidence score for enrichment result"""
        if not result:
            return 0.0
        
        confidence = 0.0
        
        # Title similarity (60% weight)
        title_similarity = self._calculate_title_similarity(
            original_title, 
            result.get("primaryTitle", "")
        )
        confidence += title_similarity * 0.6
        
        # Year match (20% weight)
        if "startYear" in result and result["startYear"]:
            confidence += 0.2
        
        # Type match (20% weight)
        if result.get("type") in self.config.processing.movie_types + self.config.processing.series_types:
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def _get_detailed_data_by_id(self, imdb_id: str) -> Optional[Dict]:
        """Get detailed data by IMDB ID (internal method for title search enhancement)"""
        try:
            # Clean and validate IMDB ID
            clean_id = self._clean_imdb_id(imdb_id)
            if not clean_id:
                return None
            
            # Check cache first
            cache_key = f"imdb_id_{clean_id}"
            if self.config.processing.enable_caching and cache_key in self.cache:
                self.logger.debug(f"Cache hit for detailed data: {clean_id}")
                return self.cache[cache_key]
            
            # Rate limiting
            self._enforce_rate_limit()
            
            # API call for detailed data
            url = f"{self.config.source.imdb_api_url}/titles/{clean_id}"
            response = requests.get(url, timeout=self.config.source.imdb_timeout)
            response.raise_for_status()
            
            detailed_data = response.json()
            
            # Validate response
            if not self._validate_imdb_response(detailed_data):
                return None
            
            # Cache detailed result
            if self.config.processing.enable_caching:
                self.cache[cache_key] = detailed_data
            
            self.logger.debug(f"Retrieved detailed data for {clean_id}")
            return detailed_data
            
        except Exception as e:
            self.logger.warning(f"Failed to get detailed data for {imdb_id}: {e}")
            return None


class TranslationService:
    """Service for translating Persian movie/series titles to English using OpenAI"""
    
    def __init__(self, config: FilimoEnrichmentConfig):
        """Initialize the translation service with config"""
        self.config = config
        self.logger = logging.getLogger("filimo_enrichment.TranslationService")
        
        # Check if openai is available
        if openai is None:
            raise ImportError("OpenAI library is required for translation service. Install with: pip install openai")
        
        # Get API key from config or environment
        self.api_key = config.translation.openai_api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key is required. Set OPENAI_API_KEY environment variable or configure in translation settings.")
        
        self.client = openai.OpenAI(api_key=self.api_key)
        
        # Rate limiting from config
        self.request_count = 0
        self.max_requests_per_minute = config.translation.max_requests_per_minute
        self.last_reset_time = time.time()
        
        # Cache for translations
        self.cache = {}
    
    def _check_rate_limit(self):
        """Check and enforce rate limiting"""
        current_time = time.time()
        
        # Reset counter every minute
        if current_time - self.last_reset_time >= 60:
            self.request_count = 0
            self.last_reset_time = current_time
        
        # Check if we've exceeded the rate limit
        if self.request_count >= self.max_requests_per_minute:
            sleep_time = 60 - (current_time - self.last_reset_time)
            if sleep_time > 0:
                self.logger.warning("Rate limit reached. Sleeping for %.2f seconds...", sleep_time)
                time.sleep(sleep_time)
                self.request_count = 0
                self.last_reset_time = time.time()
        
        self.request_count += 1
    
    def translate_title(self, persian_title: str, content_type: str = "movie") -> Optional[str]:
        """
        Translate Persian title to English using OpenAI
        
        Args:
            persian_title: The Persian title to translate
            content_type: Type of content ("movie" or "series")
            
        Returns:
            English translation or None if translation fails
        """
        if not persian_title or persian_title.strip() == '' or persian_title == 'NAN':
            return None
        
        try:
            # Check cache first
            cache_key = f"translation_{persian_title}_{content_type}"
            if self.config.translation.enable_caching and cache_key in self.cache:
                self.logger.debug(f"Cache hit for translation: {persian_title}")
                return self.cache[cache_key]
            
            self._check_rate_limit()
            
            # Create the prompt
            prompt = self._create_translation_prompt(persian_title, content_type)
            
            # Call OpenAI API with config parameters
            response = self.client.chat.completions.create(
                model=self.config.translation.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert translator specializing in Persian to English translation for entertainment content (movies, TV series, documentaries). You provide accurate, natural, and culturally appropriate translations."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=self.config.translation.max_tokens,
                temperature=self.config.translation.temperature,
                timeout=self.config.translation.timeout
            )
            
            translation = response.choices[0].message.content.strip()
            
            # Clean up the translation
            translation = self._clean_translation(translation)
            
            # Cache result if caching is enabled
            if self.config.translation.enable_caching:
                self.cache[cache_key] = translation
            
            self.logger.info(f"Translated '{persian_title}' -> '{translation}'")
            return translation
            
        except Exception as e:
            self.logger.error(f"Error translating '{persian_title}': {e}")
            return None
    
    def _create_translation_prompt(self, persian_title: str, content_type: str) -> str:
        """Create a comprehensive prompt for title translation"""
        
        content_type_text = "movie" if content_type == "movie" else "TV series"
        
        prompt = f"""Translate the following Persian {content_type_text} title to English. 

IMPORTANT INSTRUCTIONS:
1. Provide ONLY the English translation, nothing else
2. Keep the translation natural and engaging for English-speaking audiences
3. Maintain the original meaning and tone
4. For series titles, keep episode/season references if present
5. Use proper English capitalization (Title Case)
6. If the title contains numbers or special characters, preserve them
7. Do not add explanations, comments, or additional text
8. If the title is already in English or mixed language, return it as-is

Persian {content_type_text} title: "{persian_title}"

English translation:"""
        
        return prompt
    
    def _clean_translation(self, translation: str) -> str:
        """Clean and validate the translation"""
        if not translation:
            return ""
        
        # Remove common prefixes that might be added
        prefixes_to_remove = [
            "English translation:",
            "Translation:",
            "English title:",
            "Title:",
            "The English translation is:",
            "Here's the translation:"
        ]
        
        for prefix in prefixes_to_remove:
            if translation.startswith(prefix):
                translation = translation[len(prefix):].strip()
        
        # Remove quotes if the entire translation is wrapped in them
        if translation.startswith('"') and translation.endswith('"'):
            translation = translation[1:-1]
        
        # Clean up extra whitespace
        translation = translation.strip()
        
        return translation
    
    def batch_translate(self, titles: list, content_type: str = "movie") -> Dict[str, str]:
        """
        Translate multiple titles in batch
        
        Args:
            titles: List of Persian titles to translate
            content_type: Type of content ("movie" or "series")
            
        Returns:
            Dictionary mapping original titles to translations
        """
        results = {}
        delay = self.config.translation.batch_delay
        
        self.logger.info(f"Starting batch translation of {len(titles)} {content_type} titles...")
        
        for i, title in enumerate(titles, 1):
            if title and title.strip() and title != 'NAN':
                self.logger.info(f"Translating {i}/{len(titles)}: {title}")
                translation = self.translate_title(title, content_type)
                results[title] = translation
                
                # Add delay between requests to be respectful to the API
                if i < len(titles):
                    time.sleep(delay)
            else:
                self.logger.warning(f"Skipping invalid title: {title}")
                results[title] = None
        
        successful_translations = sum(1 for v in results.values() if v is not None)
        self.logger.info(f"Batch translation completed. {successful_translations}/{len(titles)} successful")
        
        return results



















class DatabaseService:
    """Service for querying database via Metabase API"""
    
    def __init__(self, config: FilimoEnrichmentConfig):
        self.config = config
        self.logger = logging.getLogger("filimo_enrichment.DatabaseService")
        self.cache = {}
        self.last_request_time = 0
    
    # def __init__(self, base_url: str = 'https://metabase.huma.ir', 
    #              api_key: str = 'mb_EOBOqLm35J4HUz3ZM3Oicyzv0w3pfRAXN9eAW4gKz4I=', 
    #              database_id: int = 2):

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.url = "https://metabase.huma.ir/api/dataset"
        self.api_key = config.source.database_api_key
        self.database_id = config.source.database_id
        self.session = requests.Session()
    
        # Set up headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'X-API-Key': self.api_key
        })

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute raw SQL query and return results as list of dictionaries
        
        Args:
            query: SQL query string
            limit: Optional limit for results
            
        Returns:
            List of dictionaries where each dict represents a row
            
        Example:
            >>> client = ClientMetabase()
            >>> results = client.execute_query("SELECT * FROM play_movie_files WHERE movie_id = 69163 LIMIT 2")
            >>> print(results)
            [{'id': 139004, 'uri': 'https://...', 'file_name': 'Peppermint_2019...', ...}, ...]
        """
        # Add limit if specified
        # Prepare payload
        payload = {
            "database": self.database_id,
            "type": "native",
            "native": {
                "query": query,
                "template-tags": {}
            },
            "parameters": []
        }
        
        
        try:
            self.logger.info(f"Executing SQL query: {query}")
            response = self.session.post(self.url, json=payload, timeout=30)
            response.raise_for_status()
            
            # Parse response
            data = response.json()
            # Check if query was successful
            if 'data' not in data:
                raise Exception(f"Unexpected response format: {data}")
            
            # Extract rows and columns
            rows = data['data'].get('rows', [])
            cols = data['data'].get('cols', [])
            
            # Convert to list of dictionaries
            result = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(cols):
                    col_name = col.get('name', f'col_{i}')
                    row_dict[col_name] = row[i] if i < len(row) else None
                result.append(row_dict)
            self.logger.info(f"Query executed successfully. Returned {len(result)} rows")
            return result
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            raise Exception(f"Metabase API request failed: {e}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response: {e}")
            raise Exception(f"Invalid JSON response: {e}")
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise

    def search_by_imdb_id(self, imdb_id: str) -> List[Dict[str, Any]]:
        """
        Search movies by IMDB ID in database
        
        Args:
            imdb_id: IMDB ID to search for
            
        Returns:
            List of movie records from database
        """
        query = f"SELECT * FROM play_movie_movies WHERE imdb_id='{imdb_id}' AND movie_status_id=1"
        self.logger.info(f"Searching database by IMDB ID: {imdb_id}")
        data = self.execute_query(query)
    
        return EnrichmentResult(success=True, data=data)
    
    def search_by_title(self, title: str = None, title_en: str = None) -> List[Dict[str, Any]]:
        """
        Search movies by title in database
        
        Args:
            title: Persian title
            title_en: English title
            
        Returns:
            List of movie records from database
        """
        # Build where clause based on available titles
        if title and title_en:
            where_clause = f"(title_fa ilike '%{title}%' or title ilike '%{title_en}%')"
        elif title:
            where_clause = f"title_fa ilike '%{title}%'"
        elif title_en:
            where_clause = f"title ilike '%{title_en}%'"
        else:
            self.logger.warning("No title provided for database search")
            return []
        
        query = f"SELECT * FROM play_movie_movies WHERE {where_clause} AND movie_status_id=1"
        self.logger.info(f"Searching database by title: {where_clause}")
        data = self.execute_query(query)
    
        return EnrichmentResult(success=True, data=data)
    
    
    def find_most_relevant_db_result(self, db_results: List[Dict[str, Any]], 
                                   search_title: str, search_title_en: str = None) -> Optional[Dict[str, Any]]:
        """
        Find the most relevant database result based on title matching
        
        Args:
            db_results: List of database results
            search_title: Original search title
            search_title_en: English search title
            
        Returns:
            Most relevant database record or None
        """
        if not db_results:
            return None
        
        if len(db_results) == 1:
            return EnrichmentResult(success=True,data= db_results[0])
        
        # Simple scoring based on title similarity
        best_match = None
        best_score = 0
        
        for result in db_results:
            score = 0
            db_title = result.get('title', '').lower()
            db_title_en = result.get('title_en', '').lower()
            db_title_fa = result.get('title_fa', '').lower()
            
            search_title_lower = search_title.lower()
            search_title_en_lower = (search_title_en or '').lower()
            
            # Check exact matches
            if search_title_lower in db_title or search_title_lower in db_title_fa:
                score += 100
            if search_title_en_lower and search_title_en_lower in db_title_en:
                score += 100
            
            # Check partial matches
            if search_title_lower in db_title or search_title_lower in db_title_fa:
                score += 50
            if search_title_en_lower and search_title_en_lower in db_title_en:
                score += 50
            
            if score > best_score:
                best_score = score
                best_match = result
        
        self.logger.info(f"Best database match found with score: {best_score}")
        return EnrichmentResult(
            success=True,
            data=best_match,
            confidence_score=1.0,
            method_used="database",)



class EnrichmentOrchestrator:
    """Orchestrates the enrichment process"""
    
    def __init__(self, config: FilimoEnrichmentConfig):
        self.config = config
        self.imdb_service = IMDBService(config)
        self.translation_service = TranslationService(config)
        self.database_service = DatabaseService(config)
        self.logger = logging.getLogger("filimo_enrichment.Orchestrator")

    def enrich_record(self, record: Dict[str, Any], content_type: str, use_db: bool = False) -> EnrichmentResult:
        """Enrich a single record using the best available method"""
        start_time = time.time()
        
        if use_db:

            result = self.database_service.search_by_title(record.get("title"), record.get("titleEn"))
            result_relevant = self.database_service.find_most_relevant_db_result(result.data, record.get("title"), record.get("titleEn"))
            if result_relevant:
                result_relevant.method_used = "database_title"
                result_relevant.processing_time = time.time() - start_time
                return result_relevant

        else:
            # Strategy 1: Try IMDB ID if available
            imdb_id = record.get("ImdbId", "")
            if imdb_id and imdb_id != "NAN" and str(imdb_id).strip():
                self.logger.info(f"Trying IMDB ID enrichment for: {imdb_id}")
                result = self.imdb_service.get_by_id(imdb_id)
                if result.success:
                    result.method_used = "imdb_id"
                    result.processing_time = time.time() - start_time
                    return result
            
            # Strategy 2: Try title search
            title_en = record.get("titleEn", "")
            if not title_en or title_en == "NAN" or not title_en.strip():
                # Translate Persian title if needed
                if self.config.processing.auto_translate:
                    persian_title = record.get("title", "")
                    if persian_title and persian_title.strip():
                        self.logger.info(f"Translating Persian title: {persian_title}")
                        title_en = self.translation_service.translate_title(persian_title, content_type)
            
            if title_en and title_en.strip():
                self.logger.info(f"Trying title search enrichment for: {title_en}")
                result = self.imdb_service.search_by_title(title_en, content_type)
                if result.success:
                    result.method_used = "title_search"
                    result.processing_time = time.time() - start_time
                    return result
            
        # Strategy 3: No enrichment possible
        return EnrichmentResult(
            success=False,
            error_message="No valid IMDB ID or title available for enrichment",
            processing_time=time.time() - start_time
        )
