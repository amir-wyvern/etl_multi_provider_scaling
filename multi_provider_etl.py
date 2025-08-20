#!/usr/bin/env python3
"""
Multi-Provider ETL Framework
Main orchestrator for managing multiple ETL providers.
"""

import argparse
import sys
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from core.base_pipeline import registry, BasePipeline, BaseConfig
from providers.filimo.config import FilimoConfig, FilimoConfigPresets
from providers.filimo.pipeline import FilimoPipeline


class MultiProviderETLOrchestrator:
    """Orchestrator for managing multiple ETL providers"""
    
    def __init__(self):
        self.registry = registry
        self._register_providers()
    
    def _register_providers(self):
        """Register all available providers"""
        # Register Filimo provider
        self.registry.register_provider("filimo", FilimoPipeline, FilimoConfig)
    
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


def create_config_from_preset(provider_name: str, preset_name: str) -> BaseConfig:
    """Create configuration from preset"""
    if provider_name == "filimo":
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
  python multi_provider_etl.py --provider filimo                    # Run Filimo with default config
  python multi_provider_etl.py --provider filimo --preset production  # Run with production preset
  python multi_provider_etl.py --list-providers                     # List all available providers
  python multi_provider_etl.py --provider filimo --output-dir /tmp  # Custom output directory
        """
    )
    
    # Provider selection
    parser.add_argument(
        "--provider",
        type=str,
        help="ETL provider to run (e.g., filimo)"
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
        
        # Run the ETL pipeline
        print(f"\n🚀 Starting {args.provider} ETL Pipeline...")
        result = orchestrator.run_provider(args.provider, config)
        
        # Display results
        if not args.quiet:
            print(f"\n🎉 {args.provider} pipeline completed successfully!")
            print(f"📊 Results:")
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
