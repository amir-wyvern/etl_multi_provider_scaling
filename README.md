# 🚀 Multi-Provider ETL Framework

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Architecture](https://img.shields.io/badge/Architecture-Clean-brightgreen.svg)]()
[![ETL](https://img.shields.io/badge/ETL-Production%20Ready-orange.svg)]()

A professional, scalable ETL framework designed to handle multiple data providers with their own specialized pipelines. Built with clean architecture principles and industry best practices.

## 🌟 Overview

This framework provides a unified approach to ETL (Extract, Transform, Load) operations across multiple data providers. Each provider can have its own specialized pipeline while sharing common infrastructure and patterns.

**Transform your monolithic data processing scripts into enterprise-grade, scalable pipelines!**

### **Key Benefits:**
- 🎯 **Scalable**: Easy to add new providers without affecting existing ones
- 🔧 **Consistent**: Unified logging, metrics, and error handling across all providers
- 📊 **Flexible**: Each provider can implement custom business logic
- 🛡️ **Maintainable**: Clean separation of concerns and modular design
- 🚀 **Production-Ready**: Comprehensive monitoring, validation, and error handling

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Multi-Provider ETL Framework                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌──────────────┐ │
│  │   CORE FRAMEWORK│    │   PROVIDER A    │    │  PROVIDER B  │ │
│  ├─────────────────┤    │    (Filimo)     │    │  (Netflix)   │ │
│  │ • BasePipeline  │    ├─────────────────┤    ├──────────────┤ │
│  │ • BaseExtractor │    │ • Config        │    │ • Config     │ │
│  │ • BaseTransformer│   │ • Extractors    │    │ • Extractors │ │
│  │ • BaseLoader    │    │ • Transformers  │    │ • Transform. │ │
│  │ • BaseValidator │    │ • Loaders       │    │ • Loaders    │ │
│  │ • BaseMetrics   │    │ • Validators    │    │ • Validators │ │
│  │ • Registry      │    │ • Pipeline      │    │ • Pipeline   │ │
│  └─────────────────┘    └─────────────────┘    └──────────────┘ │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                      CLI Orchestrator                           │
│              • Provider Selection                               │
│              • Configuration Management                         │
│              • Multi-Provider Execution                         │
└─────────────────────────────────────────────────────────────────┘
```

### **Project Structure:**
```
Multi-Provider ETL Framework/
├── core/                           # Framework Core
│   ├── base_pipeline.py           # Abstract base classes & registry
│   └── __init__.py
├── providers/                      # Provider Implementations
│   ├── filimo/                    # Filimo Provider
│   │   ├── config.py              # Filimo-specific configuration
│   │   ├── extractors.py          # Data extraction components
│   │   ├── transformers.py        # Data transformation components
│   │   ├── loaders.py             # Data loading components
│   │   ├── validators.py          # Data validation components
│   │   ├── pipeline.py            # Filimo pipeline orchestrator
│   │   └── __init__.py
│   ├── netflix/                   # Netflix Provider (Example)
│   │   ├── config.py              # Netflix configuration
│   │   ├── pipeline.py            # Netflix pipeline (placeholder)
│   │   └── __init__.py
│   └── __init__.py
├── multi_provider_etl.py          # Main CLI orchestrator
├── demo_multi_provider.py         # Demo script
└── README.md                      # This file
```

## ✨ Features

### **🎯 Core Framework**
- **Abstract Base Classes**: Standardized interfaces for all components
- **Provider Registry**: Dynamic registration and discovery of providers
- **Unified Metrics**: Consistent metrics collection across all providers
- **Comprehensive Logging**: Multi-level logging with file and console output
- **Error Handling**: Robust error handling with continue-on-error options
- **Data Validation**: Built-in validation framework with custom validators

### **🔧 Configuration Management**
- **Environment-Based**: Support for development, production, testing presets
- **CLI Overrides**: Command-line configuration options
- **Validation**: Built-in configuration validation
- **Secrets Management**: Environment variable support for sensitive data

### **📊 Monitoring & Observability**
- **Phase Tracking**: Monitor Extract, Transform, Load phases separately
- **Performance Metrics**: Execution times, data volumes, file sizes
- **Quality Checks**: Data validation and business rule verification
- **Error Tracking**: Complete error and warning collection with context

## ⚡ Quick Start

### **Prerequisites**
- Python 3.8+
- Virtual environment (recommended)

### **Installation**
```bash
# Clone the repository
git clone <repository-url>
cd multi-provider-etl-framework

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### **Basic Usage**

#### **1. List Available Providers**
```bash
python multi_provider_etl.py --list-providers
```

#### **2. Run Filimo Provider**
```bash
# Default configuration
python multi_provider_etl.py --provider filimo

# Production preset
python multi_provider_etl.py --provider filimo --preset production

# Custom configuration
python multi_provider_etl.py --provider filimo \
  --output-dir /custom/path \
  --continue-on-error \
  --log-level DEBUG
```

#### **3. Dry Run**
```bash
python multi_provider_etl.py --provider filimo --preset production --dry-run
```

## 🔧 Configuration

### **Configuration Presets**

| Preset | Use Case | Features |
|--------|----------|----------|
| **Development** | Local development | Continue on errors, debug logging, no validation |
| **Production** | Production deployment | Stop on errors, full validation, API/SFTP enabled |
| **Testing** | Automated testing | Separate output, strict validation, no intermediate files |
| **Performance** | High-speed processing | Minimal logging, no validation, optimized for speed |

### **Environment Variables**
```env
# Provider-specific
FILIMO_SFTP_PASS=your_password
FILIMO_API_KEY=your_api_key

# Framework-wide
ETL_BASE_DIR=/custom/output
ETL_LOG_LEVEL=DEBUG
```

## 📚 Usage Examples

### **Command Line Interface**
```bash
# Basic usage
python multi_provider_etl.py --provider filimo

# Production run with all options
python multi_provider_etl.py \
  --provider filimo \
  --preset production \
  --output-dir /data/etl \
  --log-to-file \
  --log-level INFO

# Development run with error handling
python multi_provider_etl.py \
  --provider filimo \
  --preset development \
  --continue-on-error \
  --no-validate
```

### **Programmatic Usage**
```python
from multi_provider_etl import MultiProviderETLOrchestrator
from providers.filimo.config import FilimoConfigPresets

# Initialize orchestrator
orchestrator = MultiProviderETLOrchestrator()

# Run single provider
config = FilimoConfigPresets.production()
result = orchestrator.run_provider("filimo", config)

# Run multiple providers
provider_configs = {
    "filimo": FilimoConfigPresets.production(),
    "netflix": NetflixConfigPresets.production()
}
results = orchestrator.run_multiple_providers(provider_configs)
```

## 🔌 Adding New Providers

### **Step-by-Step Guide:**

#### **1. Create Provider Structure**
```
providers/your_provider/
├── __init__.py
├── config.py          # Provider configuration
├── extractors.py      # Data extraction components
├── transformers.py    # Data transformation components
├── loaders.py         # Data loading components
├── validators.py      # Data validation components
└── pipeline.py        # Main pipeline orchestrator
```

#### **2. Implement Configuration**
```python
# providers/your_provider/config.py
from dataclasses import dataclass
from core.base_pipeline import BaseConfig

@dataclass
class YourProviderConfig(BaseConfig):
    # Provider-specific configuration
    api_endpoint: str = "https://api.yourprovider.com"
    
    def __post_init__(self):
        super().__post_init__()
        self.provider_name = "your_provider"
        self.provider_version = "1.0.0"
```

#### **3. Implement Components**
```python
# providers/your_provider/extractors.py
from core.base_pipeline import BaseExtractor

class YourProviderExtractor(BaseExtractor):
    def extract(self, source_name: str, **kwargs) -> pd.DataFrame:
        # Implement your extraction logic
        pass
```

#### **4. Register Provider**
```python
# multi_provider_etl.py
from providers.your_provider.config import YourProviderConfig
from providers.your_provider.pipeline import YourProviderPipeline

class MultiProviderETLOrchestrator:
    def _register_providers(self):
        # Register existing providers
        self.registry.register_provider("filimo", FilimoPipeline, FilimoConfig)
        
        # Register your new provider
        self.registry.register_provider("your_provider", YourProviderPipeline, YourProviderConfig)
```

## 📈 Performance & Metrics

### **Real-World Performance**
- **Filimo Pipeline**: ~12-26 seconds for 106K+ input rows
- **Memory Usage**: Optimized pandas operations
- **Scalability**: Designed for large datasets

### **Sample Execution Results**
```
📊 Results:
   - Movies: 9
   - Series: 110
   - Duration: 27.42 seconds
📈 Metrics:
   - Errors: 0
   - Warnings: 0
   - Total Extracted: 200,397 rows
   - Total Loaded: 119 rows
```

### **Monitoring Capabilities**
- **Phase Tracking**: Extract, Transform, Load phases
- **Operation Metrics**: Data volumes, execution times, removal rates
- **Quality Metrics**: Validation results, error counts
- **File Metrics**: Output file sizes, formats

## 📖 API Reference

### **Core Classes**

#### **BasePipeline**
```python
class BasePipeline(ABC):
    def __init__(self, config: BaseConfig)
    def run(self) -> Dict[str, Any]
    
    @abstractmethod
    def extract_phase(self) -> Dict[str, pd.DataFrame]
    
    @abstractmethod
    def transform_phase(self, data) -> Dict[str, pd.DataFrame]
    
    @abstractmethod
    def load_phase(self, data)
```

#### **BaseExtractor**
```python
class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, source_name: str, **kwargs) -> pd.DataFrame
```

#### **BaseTransformer**
```python
class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame
```

#### **BaseLoader**
```python
class BaseLoader(ABC):
    @abstractmethod
    def load(self, df: pd.DataFrame, target_name: str, **kwargs)
```

## 🎯 Use Cases

- **Data Integration** from multiple APIs/databases
- **ETL Pipeline Modernization** from monolithic scripts
- **Multi-Tenant Data Processing** with provider isolation
- **Enterprise Data Workflows** with audit trails and monitoring

## 🔍 Troubleshooting

### **Common Issues**

#### **Provider Not Found**
```bash
❌ Error: Unknown provider 'xyz'
```
**Solution**: Check available providers with `--list-providers`

#### **Configuration Errors**
```bash
❌ Required source file not found
```
**Solution**: Verify file paths and permissions

#### **Memory Issues**
```bash
❌ Out of memory during processing
```
**Solution**: Use performance preset or implement chunking

### **Debug Mode**
```bash
python multi_provider_etl.py --provider filimo --log-level DEBUG --verbose
```

## 🚀 Architecture Evolution

This project evolved through multiple phases:

| Phase | Description | Approach |
|-------|-------------|----------|
| **Phase 1** | Original Script (697 lines) | Monolithic, hardcoded values |
| **Phase 2** | Clean Architecture (477 lines) | Object-oriented design |
| **Phase 3** | Professional ETL | Enterprise-grade with metrics |
| **Phase 4** | **Multi-Provider Framework** ⭐ | Scalable, extensible, production-ready |

### **Architecture Benefits**

| Aspect | Original Script | Multi-Provider Framework |
|--------|----------------|---------------------------|
| **Maintainability** | Difficult | Excellent |
| **Scalability** | Not scalable | Highly scalable |
| **Testing** | Hard to test | Easy unit/integration testing |
| **Monitoring** | Basic prints | Comprehensive metrics |
| **Configuration** | Hardcoded | Flexible, environment-based |
| **Multi-Provider** | Not supported | Core feature |
| **Production Readiness** | No | Yes |

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-provider`)
3. Commit your changes (`git commit -am 'Add new provider'`)
4. Push to the branch (`git push origin feature/new-provider`)
5. Create a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with clean architecture principles
- Inspired by enterprise ETL best practices
- Designed for scalability and maintainability

## 📞 Support

- **Documentation**: Complete guides and examples included
- **Issues**: Report bugs and request features via GitHub Issues
- **Architecture**: Designed for easy extension and modification

---

**🚀 Ready for production use and future expansion!**

*Transform your data processing workflows with professional-grade ETL capabilities.*
