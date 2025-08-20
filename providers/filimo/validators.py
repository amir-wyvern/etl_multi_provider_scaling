#!/usr/bin/env python3
"""
Filimo Validators
Data validation components specific to Filimo provider.
"""

import pandas as pd
from typing import List, Dict
from core.base_pipeline import BaseValidator, BaseMetrics
from .config import FilimoConfig


class FilimoDataValidator(BaseValidator):
    """Data quality validation for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def validate(self, df: pd.DataFrame, validation_name: str, **kwargs) -> bool:
        """Validate Filimo data"""
        validation_type = validation_name.split('_')[0] if '_' in validation_name else validation_name
        
        if validation_type == "required":
            return self._validate_required_columns(df, validation_name)
        elif validation_type == "business":
            return self._validate_business_rules(df, validation_name)
        elif validation_type == "format":
            return self._validate_format_rules(df, validation_name)
        else:
            self.logger.warning(f"Unknown validation type: {validation_type}")
            return True
    
    def _validate_required_columns(self, df: pd.DataFrame, validation_name: str) -> bool:
        """Validate that required columns exist"""
        required_columns = self._get_required_columns(validation_name)
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            details = {"missing_columns": missing_columns, "required_columns": required_columns}
            self._record_validation(validation_name, False, details)
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        self._record_validation(validation_name, True, {"columns_checked": required_columns})
        return True
    
    def _validate_business_rules(self, df: pd.DataFrame, validation_name: str) -> bool:
        """Validate business rules"""
        issues = []
        
        # Check for required fields
        if 'uid' in df.columns:
            null_uids = df['uid'].isnull().sum()
            if null_uids > 0:
                issues.append(f"{null_uids} rows with null UIDs")
        
        # Check for valid types
        if 'type' in df.columns:
            valid_types = [self.config.transform.movie_type_value, self.config.transform.series_type_value]
            invalid_types = df[~df['type'].isin(valid_types)]['type'].unique()
            if len(invalid_types) > 0:
                issues.append(f"Invalid types found: {list(invalid_types)}")
        
        # Check serial parent relationships
        if 'serial_parent_id' in df.columns and 'serial_is_parent' in df.columns:
            episodes_without_parent = df[
                (df['serial_is_parent'] != 'yes') & 
                (df['serial_parent_id'].isnull())
            ]
            if len(episodes_without_parent) > 0:
                issues.append(f"{len(episodes_without_parent)} episodes without parent ID")
        
        if issues:
            details = {"issues": issues}
            self._record_validation(validation_name, False, details)
            for issue in issues:
                self.logger.warning(f"Business rule issue: {issue}")
            return False
        
        self._record_validation(validation_name, True, {"rules_checked": ["uid_not_null", "valid_types", "parent_relationships"]})
        return True
    
    def _validate_format_rules(self, df: pd.DataFrame, validation_name: str) -> bool:
        """Validate format-specific rules"""
        issues = []
        
        # Check final format columns if this is a final dataset
        if "final" in validation_name:
            expected_columns = [
                "id", "title", "titleEn", "fileName", "episodeNo", "seasonNo",
                "accessType", "price", "quality", "url", "sourceId", "ImdbId",
                "externalLink", "type", "cover", "backdrop", "plot"
            ]
            missing_final_columns = [col for col in expected_columns if col not in df.columns]
            if missing_final_columns:
                issues.append(f"Missing final format columns: {missing_final_columns}")
        
        # Check data types
        if 'price' in df.columns:
            non_numeric_prices = df[pd.to_numeric(df['price'], errors='coerce').isnull()]
            if len(non_numeric_prices) > 0:
                issues.append(f"{len(non_numeric_prices)} rows with non-numeric prices")
        
        if 'type' in df.columns:
            if "final" in validation_name:
                valid_final_types = [1, 2]  # Movie = 1, Series = 2
                invalid_final_types = df[~df['type'].isin(valid_final_types)]['type'].unique()
                if len(invalid_final_types) > 0:
                    issues.append(f"Invalid final format types: {list(invalid_final_types)}")
        
        if issues:
            details = {"issues": issues}
            self._record_validation(validation_name, False, details)
            for issue in issues:
                self.logger.warning(f"Format rule issue: {issue}")
            return False
        
        self._record_validation(validation_name, True, {"format_rules_checked": True})
        return True
    
    def _get_required_columns(self, validation_name: str) -> List[str]:
        """Get required columns based on validation context"""
        if "filimo_data" in validation_name:
            return ["uid", "title", "type", "serial_is_parent"]
        elif "imported_before" in validation_name:
            return ["uri"]
        elif "imdb_data" in validation_name:
            return ["uid", "Correct_imdb"]
        elif "final" in validation_name:
            return [
                "id", "title", "titleEn", "fileName", "episodeNo", "seasonNo",
                "accessType", "price", "quality", "url", "sourceId", "ImdbId",
                "externalLink", "type", "cover", "backdrop", "plot"
            ]
        else:
            return ["uid"]  # Default minimum requirement


class FilimoQualityValidator(BaseValidator):
    """Data quality validation for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
    
    def validate(self, df: pd.DataFrame, validation_name: str, **kwargs) -> bool:
        """Validate data quality"""
        quality_issues = []
        
        # Check for duplicate UIDs
        if 'uid' in df.columns:
            duplicate_uids = df[df['uid'].duplicated()]
            if len(duplicate_uids) > 0:
                quality_issues.append(f"{len(duplicate_uids)} duplicate UIDs found")
        
        # Check for missing critical data
        critical_columns = ['title', 'type']
        for col in critical_columns:
            if col in df.columns:
                missing_count = df[col].isnull().sum()
                if missing_count > 0:
                    quality_issues.append(f"{missing_count} rows missing {col}")
        
        # Check data consistency
        if 'serial_is_parent' in df.columns and 'serial_parent_id' in df.columns:
            # Parents should not have parent IDs
            parent_with_parent = df[
                (df['serial_is_parent'] == 'yes') & 
                (df['serial_parent_id'].notnull())
            ]
            if len(parent_with_parent) > 0:
                quality_issues.append(f"{len(parent_with_parent)} parent records have parent IDs")
        
        # Check URL formats
        if 'imdb_url' in df.columns:
            invalid_imdb_urls = df[
                df['imdb_url'].notnull() & 
                ~df['imdb_url'].str.contains('imdb.com', na=False)
            ]
            if len(invalid_imdb_urls) > 0:
                quality_issues.append(f"{len(invalid_imdb_urls)} invalid IMDB URLs")
        
        if quality_issues:
            details = {"quality_issues": quality_issues}
            self._record_validation(validation_name, False, details)
            for issue in quality_issues:
                self.logger.warning(f"Quality issue: {issue}")
            return False
        
        self._record_validation(validation_name, True, {"quality_checks_passed": True})
        return True
