#!/usr/bin/env python3
"""
Filimo Transformers
Data transformation components specific to Filimo provider.
"""

import pandas as pd
from typing import Tuple, Optional
from core.base_pipeline import BaseTransformer, BaseMetrics
from .config import FilimoConfig


class FilimoDuplicateRemovalTransformer(BaseTransformer):
    """Remove duplicate records while preserving parent records for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Remove duplicates but preserve parent records"""
        imported_before_df = kwargs.get('imported_before_df')
        if imported_before_df is None:
            raise ValueError("imported_before_df required for Filimo duplicate removal")
        
        self.logger.info("Removing duplicate UIDs while preserving parent records")
        
        before_count = len(df)
        
        duplicate_condition = df["uid"].astype(str).isin(imported_before_df["uri"])
        parent_condition = df["serial_is_parent"] == "yes"
        
        # Keep rows that are either: NOT duplicates OR are parent records
        if self.config.transform.preserve_parent_records:
            filtered_df = df[~duplicate_condition | parent_condition]
            parents_kept = len(df[duplicate_condition & parent_condition])
            details = {"parents_kept": parents_kept}
            self.logger.info(f"Kept {parents_kept} parent records despite being duplicates")
        else:
            filtered_df = df[~duplicate_condition]
            details = {"parents_kept": 0}
        
        self._record_transformation("duplicate_removal", before_count, len(filtered_df), details)
        return filtered_df


class FilimoTitleFilterTransformer(BaseTransformer):
    """Filter out rows with excluded title keywords for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Remove rows with excluded title values"""
        self.logger.info("Filtering out rows with excluded title keywords")
        
        filtered_df = df.copy()
        before_count = len(df)
        
        for keyword in self.config.transform.excluded_title_keywords:
            keyword_before = len(filtered_df)
            filtered_df = filtered_df[
                ~filtered_df["title"].str.contains(keyword, na=False)
            ]
            keyword_removed = keyword_before - len(filtered_df)
            if keyword_removed > 0:
                self.logger.info(f"Removed {keyword_removed} rows containing '{keyword}'")
        
        self._record_transformation("title_filtering", before_count, len(filtered_df))
        return filtered_df


class FilimoExclusionListTransformer(BaseTransformer):
    """Remove rows based on exclusion lists for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Remove rows based on exclusion lists"""
        movie_uid_df = kwargs.get('movie_uid_df')
        parent_id_df = kwargs.get('parent_id_df')
        
        self.logger.info("Applying exclusion lists")
        
        filtered_df = df.copy()
        before_count = len(df)
        
        # Remove excluded UIDs
        if movie_uid_df is not None and "uid" in movie_uid_df.columns:
            excluded_uids = movie_uid_df["uid"].astype(str).tolist()
            uid_before = len(filtered_df)
            filtered_df = filtered_df[~filtered_df["uid"].astype(str).isin(excluded_uids)]
            uid_after = len(filtered_df)
            self.logger.info(f"Removed {uid_before - uid_after} rows with excluded UIDs")
        
        # Remove excluded parent IDs
        if parent_id_df is not None and "parent_id" in parent_id_df.columns:
            excluded_parent_ids = parent_id_df["parent_id"].astype(str).tolist()
            parent_before = len(filtered_df)
            filtered_df = filtered_df[
                ~filtered_df["serial_parent_id"].astype(str).isin(excluded_parent_ids)
            ]
            parent_after = len(filtered_df)
            self.logger.info(f"Removed {parent_before - parent_after} rows with excluded parent IDs")
        
        self._record_transformation("exclusion_lists", before_count, len(filtered_df))
        return filtered_df


class FilimoDocumentaryFilterTransformer(BaseTransformer):
    """Remove Iranian documentary content for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Remove Iranian documentaries"""
        if not self.config.transform.remove_iranian_documentaries:
            return df
        
        self.logger.info("Removing Iranian documentaries")
        before_count = len(df)
        
        filtered_df = df[
            ~((df["genre"] == "مستند") & (df["country"] == "ایران"))
        ]
        
        self._record_transformation("documentary_filtering", before_count, len(filtered_df))
        return filtered_df


class FilimoDataSplitterTransformer(BaseTransformer):
    """Split data into movies and series for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def transform(self, df: pd.DataFrame, **kwargs) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Split DataFrame into movies and series"""
        self.logger.info("Splitting data into movies and series")
        
        movies_df = df[df["type"] == self.config.transform.movie_type_value].copy()
        series_df = df[df["type"] == self.config.transform.series_type_value].copy()
        
        # Handle other types
        other_types = df[~df["type"].isin([
            self.config.transform.movie_type_value, 
            self.config.transform.series_type_value
        ])].copy()
        
        if not other_types.empty:
            self.logger.warning(f"Found {len(other_types)} rows with other types: {other_types['type'].unique()}")
            series_df = pd.concat([series_df, other_types], ignore_index=True)
        
        self.logger.info(f"Split complete: {len(movies_df)} movies, {len(series_df)} series")
        
        # Record metrics for both outputs
        self._record_transformation("data_splitting_movies", len(df), len(movies_df))
        self._record_transformation("data_splitting_series", len(df), len(series_df))
        
        return movies_df, series_df


class FilimoMovieTransformer(BaseTransformer):
    """Transform movies to final format for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform movies to final format"""
        imdb_df = kwargs.get('imdb_df')
        if imdb_df is None:
            raise ValueError("imdb_df required for Filimo movie transformation")
        
        self.logger.info("Transforming movies to final format")
        
        transformed_data = []
        fixed_values = self.config.transform.fixed_values
        
        for _, row in df.iterrows():
            new_row = {
                "id": row["uid"],
                "title": row["title"],
                "titleEn": row["title_en"],
                "fileName": fixed_values["fileName"],
                "episodeNo": row["serial_episode"],
                "seasonNo": row["serial_season"],
                "accessType": fixed_values["accessType"],
                "price": fixed_values["price"],
                "quality": fixed_values["quality"],
                "url": row["uid"],
                "sourceId": fixed_values["sourceId"],
                "ImdbId": self._get_imdb_id_for_movie(row, imdb_df),
                "externalLink": f"filimo://movie?id={row['uid']}",
                "type": fixed_values["movie_type_id"],
                "cover": row["cover_image_url"],
                "backdrop": row["background_image_url"],
                "plot": row["plot"],
            }
            transformed_data.append(new_row)
        
        columns_order = [
            "id", "title", "titleEn", "fileName", "episodeNo", "seasonNo",
            "accessType", "price", "quality", "url", "sourceId", "ImdbId",
            "externalLink", "type", "cover", "backdrop", "plot"
        ]
        
        result_df = pd.DataFrame(transformed_data, columns=columns_order)
        self.logger.info(f"Transformed {len(result_df)} movies")
        
        self._record_transformation("movie_formatting", len(df), len(result_df))
        return result_df
    
    def _get_imdb_id_for_movie(self, row: pd.Series, imdb_df: pd.DataFrame) -> str:
        """Get IMDB ID for movie with fallback logic"""
        # Primary: Check done_imdb.csv
        matching_row = imdb_df[imdb_df["uid"].astype(str).str.lower() == str(row["uid"]).lower()]
        if not matching_row.empty:
            correct_imdb = matching_row.iloc[0]["Correct_imdb"]
            if pd.notna(correct_imdb) and isinstance(correct_imdb, str) and "/title/tt" in correct_imdb:
                return correct_imdb.split("/title/tt")[1].split("/")[0]
        
        # Secondary: Check imdb_url
        if pd.notna(row["imdb_url"]) and isinstance(row["imdb_url"], str) and "/title/tt" in row["imdb_url"]:
            return row["imdb_url"].split("/title/tt")[1].split("/")[0]
        
        return self.config.output.csv_na_rep


class FilimoSeriesTransformer(BaseTransformer):
    """Transform series to final format for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform series to final format"""
        imdb_df = kwargs.get('imdb_df')
        if imdb_df is None:
            raise ValueError("imdb_df required for Filimo series transformation")
        
        self.logger.info("Transforming series to final format")
        
        transformed_data = []
        fixed_values = self.config.transform.fixed_values
        
        for _, row in df.iterrows():
            new_row = {
                "id": row["serial_parent_id"] if pd.notna(row["serial_parent_id"]) else row["uid"],
                "title": self._get_parent_title(row, df, "title"),
                "titleEn": self._get_parent_title(row, df, "title_en"),
                "fileName": fixed_values["fileName"],
                "episodeNo": row["serial_episode"],
                "seasonNo": row["serial_season"],
                "accessType": fixed_values["accessType"],
                "price": fixed_values["price"],
                "quality": fixed_values["quality"],
                "url": row["uid"],
                "sourceId": fixed_values["sourceId"],
                "ImdbId": self._get_imdb_id_for_series(row, imdb_df, df),
                "externalLink": f"filimo://movie?id={row['uid']}",
                "type": fixed_values["series_type_id"],
                "cover": row["cover_image_url"],
                "backdrop": row["background_image_url"],
                "plot": row["plot"],
            }
            transformed_data.append(new_row)
        
        columns_order = [
            "id", "title", "titleEn", "fileName", "episodeNo", "seasonNo",
            "accessType", "price", "quality", "url", "sourceId", "ImdbId",
            "externalLink", "type", "cover", "backdrop", "plot"
        ]
        
        result_df = pd.DataFrame(transformed_data, columns=columns_order)
        
        # Remove parent records (keep only episodes)
        episode_uids = df[df["serial_is_parent"] != "yes"]["uid"].tolist()
        result_df = result_df[result_df["url"].isin(episode_uids)]
        
        self.logger.info(f"Transformed {len(result_df)} series episodes")
        
        self._record_transformation("series_formatting", len(df), len(result_df))
        return result_df
    
    def _get_parent_title(self, row: pd.Series, original_df: pd.DataFrame, column_name: str) -> str:
        """Get title from parent row using serial_parent_id"""
        if pd.notna(row["serial_parent_id"]):
            parent_row = original_df[original_df["uid"].astype(str) == str(row["serial_parent_id"])]
            if not parent_row.empty:
                return parent_row.iloc[0][column_name]
        return row[column_name]
    
    def _get_imdb_id_for_series(self, row: pd.Series, imdb_df: pd.DataFrame, original_df: pd.DataFrame) -> str:
        """Get IMDB ID for series with fallback logic"""
        if pd.notna(row["serial_parent_id"]):
            # Primary: Check done_imdb.csv using serial_parent_id
            matching_row = imdb_df[imdb_df["uid"] == row["serial_parent_id"]]
            if not matching_row.empty:
                correct_imdb = matching_row.iloc[0]["Correct_imdb"]
                if pd.notna(correct_imdb) and isinstance(correct_imdb, str) and "/title/tt" in correct_imdb:
                    return correct_imdb.split("/title/tt")[1].split("/")[0]
            
            # Secondary: Check imdb_url of parent
            parent_row = original_df[original_df["uid"] == row["serial_parent_id"]]
            if not parent_row.empty:
                parent_imdb_url = parent_row.iloc[0]["imdb_url"]
                if pd.notna(parent_imdb_url) and isinstance(parent_imdb_url, str) and "/title/tt" in parent_imdb_url:
                    return parent_imdb_url.split("/title/tt")[1].split("/")[0]
        
        return self.config.output.csv_na_rep
