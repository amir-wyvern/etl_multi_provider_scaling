#!/usr/bin/env python3
"""
Base ETL Pipeline Framework
Abstract base classes and interfaces for multi-provider ETL pipelines.
"""

import pandas as pd
import logging
import json
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Type
from dataclasses import dataclass, field


@dataclass
class BaseConfig:
    """Base configuration for all providers"""
    # Provider identification
    provider_name: str = "unknown"
    provider_version: str = "1.0.0"
    
    # Common paths
    base_dir: Path = Path("assets")
    
    # Processing options
    continue_on_error: bool = False
    validate_data: bool = True
    save_metrics: bool = True
    
    # Logging
    log_level: str = "INFO"
    log_to_file: bool = False
    
    def __post_init__(self):
        """Ensure base directory exists"""
        self.base_dir.mkdir(exist_ok=True)
    
    @property
    def metrics_path(self) -> Path:
        return self.base_dir / f"{self.provider_name}_metrics.json"
    
    @property
    def log_path(self) -> Path:
        return self.base_dir / f"{self.provider_name}_pipeline.log"


class BaseMetrics:
    """Base metrics tracking for all providers"""
    
    def __init__(self, config: BaseConfig):
        self.config = config
        self.start_time = datetime.now()
        self.metrics = {
            "provider_info": {
                "name": config.provider_name,
                "version": config.provider_version,
            },
            "execution": {
                "start_time": self.start_time.isoformat(),
                "end_time": None,
                "duration_seconds": None,
                "status": "running"
            },
            "phases": {},
            "errors": [],
            "warnings": []
        }
    
    def start_phase(self, phase: str):
        """Mark the start of a phase"""
        if phase not in self.metrics["phases"]:
            self.metrics["phases"][phase] = {}
        self.metrics["phases"][phase]["start_time"] = datetime.now().isoformat()
    
    def end_phase(self, phase: str):
        """Mark the end of a phase"""
        if phase in self.metrics["phases"]:
            self.metrics["phases"][phase]["end_time"] = datetime.now().isoformat()
    
    def record_operation(self, phase: str, operation: str, data: Dict[str, Any]):
        """Record operation metrics"""
        if phase not in self.metrics["phases"]:
            self.metrics["phases"][phase] = {}
        if "operations" not in self.metrics["phases"][phase]:
            self.metrics["phases"][phase]["operations"] = {}
        
        self.metrics["phases"][phase]["operations"][operation] = data
    
    def record_error(self, error: str, phase: str = None):
        """Record error"""
        self.metrics["errors"].append({
            "error": error,
            "phase": phase,
            "timestamp": datetime.now().isoformat()
        })
    
    def record_warning(self, warning: str, phase: str = None):
        """Record warning"""
        self.metrics["warnings"].append({
            "warning": warning,
            "phase": phase,
            "timestamp": datetime.now().isoformat()
        })
    
    def finalize(self, status: str = "completed"):
        """Finalize metrics"""
        end_time = datetime.now()
        self.metrics["execution"]["end_time"] = end_time.isoformat()
        self.metrics["execution"]["duration_seconds"] = (end_time - self.start_time).total_seconds()
        self.metrics["execution"]["status"] = status
    
    def save_metrics(self):
        """Save metrics to file"""
        if self.config.save_metrics:
            # Convert numpy types to native Python types for JSON serialization
            serializable_metrics = self._convert_to_serializable(self.metrics)
            with open(self.config.metrics_path, 'w') as f:
                json.dump(serializable_metrics, f, indent=2, default=str)
    
    def _convert_to_serializable(self, obj):
        """Convert numpy types to native Python types for JSON serialization"""
        import numpy as np
        
        if isinstance(obj, dict):
            return {key: self._convert_to_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_to_serializable(item) for item in obj]
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif hasattr(obj, 'item'):  # For other numpy scalar types
            return obj.item()
        else:
            return obj
    
    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary"""
        return self.metrics


class BaseExtractor(ABC):
    """Abstract base class for all extractors"""
    
    def __init__(self, config: BaseConfig, metrics: BaseMetrics):
        self.config = config
        self.metrics = metrics
        self.logger = logging.getLogger(f"{config.provider_name}.{self.__class__.__name__}")
    
    @abstractmethod
    def extract(self, source_name: str, **kwargs) -> pd.DataFrame:
        """Extract data from source"""
        pass
    
    def _record_extraction(self, source_name: str, count: int, duration: float = None):
        """Record extraction metrics"""
        data = {"count": count}
        if duration:
            data["duration"] = duration
        self.metrics.record_operation("extract", source_name, data)


class BaseTransformer(ABC):
    """Abstract base class for all transformers"""
    
    def __init__(self, config: BaseConfig, metrics: BaseMetrics):
        self.config = config
        self.metrics = metrics
        self.logger = logging.getLogger(f"{config.provider_name}.{self.__class__.__name__}")
    
    @abstractmethod
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform the data"""
        pass
    
    def _record_transformation(self, operation: str, before: int, after: int, details: Dict = None):
        """Record transformation metrics"""
        data = {
            "input_count": before,
            "output_count": after,
            "removed_count": before - after,
            "removal_rate": ((before - after) / before * 100) if before > 0 else 0
        }
        if details:
            data["details"] = details
        self.metrics.record_operation("transform", operation, data)


class BaseLoader(ABC):
    """Abstract base class for all loaders"""
    
    def __init__(self, config: BaseConfig, metrics: BaseMetrics):
        self.config = config
        self.metrics = metrics
        self.logger = logging.getLogger(f"{config.provider_name}.{self.__class__.__name__}")
    
    @abstractmethod
    def load(self, df: pd.DataFrame, target_name: str, **kwargs):
        """Load data to target"""
        pass
    
    def _record_load(self, target_name: str, count: int, file_size: int = None):
        """Record load metrics"""
        data = {"count": count}
        if file_size:
            data["file_size"] = file_size
        self.metrics.record_operation("load", target_name, data)


class BaseValidator(ABC):
    """Abstract base class for data validators"""
    
    def __init__(self, config: BaseConfig, metrics: BaseMetrics):
        self.config = config
        self.metrics = metrics
        self.logger = logging.getLogger(f"{config.provider_name}.{self.__class__.__name__}")
    
    @abstractmethod
    def validate(self, df: pd.DataFrame, validation_name: str, **kwargs) -> bool:
        """Validate data"""
        pass
    
    def _record_validation(self, validation_name: str, passed: bool, details: Dict = None):
        """Record validation results"""
        data = {"passed": passed}
        if details:
            data["details"] = details
        self.metrics.record_operation("validation", validation_name, data)


class BasePipeline(ABC):
    """Abstract base class for all ETL pipelines"""
    
    def __init__(self, config: BaseConfig):
        self.config = config
        self.metrics = BaseMetrics(config)
        self.logger = logging.getLogger(f"{config.provider_name}.Pipeline")
        
        # Initialize components
        self.extractors: Dict[str, BaseExtractor] = {}
        self.transformers: Dict[str, BaseTransformer] = {}
        self.loaders: Dict[str, BaseLoader] = {}
        self.validators: Dict[str, BaseValidator] = {}
        
        # Setup logging
        self._setup_logging()
        
        # Initialize provider-specific components
        self._initialize_components()
    
    def _setup_logging(self):
        """Setup logging for the pipeline"""
        logger = logging.getLogger(self.config.provider_name)
        logger.setLevel(getattr(logging, self.config.log_level.upper()))
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            f'%(asctime)s - {self.config.provider_name} - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler (if enabled)
        if self.config.log_to_file:
            file_handler = logging.FileHandler(self.config.log_path)
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                f'%(asctime)s - {self.config.provider_name} - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
    
    @abstractmethod
    def _initialize_components(self):
        """Initialize provider-specific components"""
        pass
    
    @abstractmethod
    def extract_phase(self) -> Dict[str, pd.DataFrame]:
        """Extract phase implementation"""
        pass
    
    @abstractmethod
    def transform_phase(self, extracted_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Transform phase implementation"""
        pass
    
    @abstractmethod
    def load_phase(self, transformed_data: Dict[str, pd.DataFrame]):
        """Load phase implementation"""
        pass
    
    def validate_phase(self, data: Dict[str, pd.DataFrame]) -> bool:
        """Optional validation phase"""
        if not self.config.validate_data:
            return True
        
        self.logger.info("🔍 Starting VALIDATION phase")
        self.metrics.start_phase("validation")
        
        all_valid = True
        for validator_name, validator in self.validators.items():
            for data_name, df in data.items():
                try:
                    is_valid = validator.validate(df, f"{validator_name}_{data_name}")
                    if not is_valid:
                        all_valid = False
                        self.logger.warning(f"Validation failed: {validator_name} on {data_name}")
                except Exception as e:
                    error_msg = f"Validation error in {validator_name}: {e}"
                    self.logger.error(error_msg)
                    self.metrics.record_error(error_msg, "validation")
                    if not self.config.continue_on_error:
                        raise
                    all_valid = False
        
        self.metrics.end_phase("validation")
        self.logger.info(f"✅ VALIDATION phase completed - {'PASSED' if all_valid else 'FAILED'}")
        return all_valid
    
    def run(self) -> Dict[str, Any]:
        """Run the complete ETL pipeline"""
        self.logger.info(f"🚀 Starting {self.config.provider_name} ETL Pipeline v{self.config.provider_version}")
        
        try:
            # Extract phase
            self.logger.info("📥 Starting EXTRACT phase")
            self.metrics.start_phase("extract")
            extracted_data = self.extract_phase()
            self.metrics.end_phase("extract")
            self.logger.info("✅ EXTRACT phase completed")
            
            # Validate extracted data
            if not self.validate_phase(extracted_data):
                self.logger.warning("⚠️ Validation failed for extracted data")
                if not self.config.continue_on_error:
                    raise ValueError("Data validation failed")
            
            # Transform phase
            self.logger.info("🔄 Starting TRANSFORM phase")
            self.metrics.start_phase("transform")
            transformed_data = self.transform_phase(extracted_data)
            self.metrics.end_phase("transform")
            self.logger.info("✅ TRANSFORM phase completed")
            
            # Validate transformed data
            if not self.validate_phase(transformed_data):
                self.logger.warning("⚠️ Validation failed for transformed data")
                if not self.config.continue_on_error:
                    raise ValueError("Data validation failed")
            
            # Load phase
            self.logger.info("💾 Starting LOAD phase")
            self.metrics.start_phase("load")
            self.load_phase(transformed_data)
            self.metrics.end_phase("load")
            self.logger.info("✅ LOAD phase completed")
            
            # Finalize
            self.metrics.finalize("completed")
            self.metrics.save_metrics()
            
            # Generate result
            result = self._generate_result(transformed_data)
            
            self.logger.info("✅ ETL Pipeline completed successfully!")
            self.logger.info(f"⏱️ Duration: {self.metrics.metrics['execution']['duration_seconds']:.2f} seconds")
            
            return result
            
        except Exception as e:
            self.metrics.record_error(str(e), "pipeline")
            self.metrics.finalize("failed")
            self.metrics.save_metrics()
            self.logger.error(f"❌ ETL Pipeline failed: {e}")
            raise
    
    @abstractmethod
    def _generate_result(self, transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Generate pipeline result summary"""
        pass


class PipelineRegistry:
    """Registry for managing multiple pipeline providers"""
    
    def __init__(self):
        self._pipelines: Dict[str, Type[BasePipeline]] = {}
        self._configs: Dict[str, Type[BaseConfig]] = {}
    
    def register_provider(
        self, 
        provider_name: str, 
        pipeline_class: Type[BasePipeline], 
        config_class: Type[BaseConfig]
    ):
        """Register a new provider"""
        self._pipelines[provider_name] = pipeline_class
        self._configs[provider_name] = config_class
        logging.info(f"Registered provider: {provider_name}")
    
    def get_pipeline_class(self, provider_name: str) -> Type[BasePipeline]:
        """Get pipeline class for provider"""
        if provider_name not in self._pipelines:
            raise ValueError(f"Provider '{provider_name}' not registered")
        return self._pipelines[provider_name]
    
    def get_config_class(self, provider_name: str) -> Type[BaseConfig]:
        """Get config class for provider"""
        if provider_name not in self._configs:
            raise ValueError(f"Provider '{provider_name}' not registered")
        return self._configs[provider_name]
    
    def list_providers(self) -> List[str]:
        """List all registered providers"""
        return list(self._pipelines.keys())
    
    def create_pipeline(self, provider_name: str, config: BaseConfig = None) -> BasePipeline:
        """Create pipeline instance for provider"""
        pipeline_class = self.get_pipeline_class(provider_name)
        
        if config is None:
            config_class = self.get_config_class(provider_name)
            config = config_class()
            config.provider_name = provider_name
        
        return pipeline_class(config)


# Global registry instance
registry = PipelineRegistry()
