#!/usr/bin/env python3
"""
Filimo Extractors
Data extraction components specific to Filimo provider.
"""

import pandas as pd
import requests
import io
from datetime import datetime
from pathlib import Path
try:
    import pysftp
except ImportError:
    pysftp = None
from core.base_pipeline import BaseExtractor, BaseMetrics
from .config import FilimoConfig


class FilimoCSVExtractor(BaseExtractor):
    """Extract data from CSV files for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config  # Type hint for IDE
    
    def extract(self, source_name: str, **kwargs) -> pd.DataFrame:
        """Extract data from CSV file"""
        source_paths = self.config.get_source_paths()
        
        if source_name not in source_paths:
            raise ValueError(f"Unknown Filimo source: {source_name}")
        
        file_path = source_paths[source_name]
        self.logger.info(f"Extracting {source_name} from: {file_path}")
        
        if not file_path.exists():
            raise FileNotFoundError(f"Filimo {source_name} file not found: {file_path}")
        
        start_time = datetime.now()
        df = pd.read_csv(file_path)
        duration = (datetime.now() - start_time).total_seconds()
        
        self._record_extraction(source_name, len(df), duration)
        self.logger.info(f"Extracted {len(df)} rows from {source_name}")
        
        return df


class FilimoSFTPExtractor(BaseExtractor):
    """Extract data from SFTP server for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
        self._setup_download_directory()
    
    def _setup_download_directory(self):
        """Ensure assets directory exists"""
        self.download_dir = self.config.base_dir
        self.download_dir.mkdir(exist_ok=True)
    
    def _download_from_sftp(self, source_name: str) -> Path:
        """Download the latest CSV file from SFTP server with retry logic"""
        import time
        
        if pysftp is None:
            raise ImportError("pysftp is required for SFTP extraction. Install with: pip install pysftp")
        
        # SFTP connection parameters
        host = self.config.source.sftp_host
        port = self.config.source.sftp_port
        username = self.config.source.sftp_username
        password = self.config.source.sftp_password
        remote_dir = self.config.source.sftp_remote_dir
        
        self.logger.info(f"🌐 Connecting to SFTP server: {username}@{host}:{port}")
        
        # Disable host key checking for simplicity (use with caution in production)
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    self.logger.info(f"🔄 Retry attempt {attempt + 1}/{max_retries} for SFTP download")
                    
                    # Add delay between retries (exponential backoff)
                    delay = 2 ** attempt  # 2, 4, 8 seconds
                    self.logger.info(f"⏳ Waiting {delay} seconds before retry...")
                    time.sleep(delay)
                
                with pysftp.Connection(host=host, port=port, username=username, 
                                     password=password, cnopts=cnopts) as sftp:
                    self.logger.info("✅ Connected to SFTP server")
                    
                    # Change to target directory
                    sftp.chdir(remote_dir)
                    self.logger.info(f"📂 Changed to directory: {remote_dir}")
                    
                    # List all CSV files
                    csv_files = [f for f in sftp.listdir() if f.lower().endswith(".csv")]
                    if not csv_files:
                        raise FileNotFoundError(f"No CSV files found in remote directory: {remote_dir}")
                    
                    # Get newest CSV file based on modification time
                    newest_csv = max(csv_files, key=lambda f: sftp.stat(f).st_mtime)
                    self.logger.info(f"📄 Latest CSV file found: {newest_csv}")
                    
                    # Create local filename with timestamp
                    today = datetime.now().strftime("%Y-%m-%d")
                    local_filename = f"{source_name}_{today}.csv"
                    local_path = self.download_dir / local_filename
                    
                    # Get file size for progress tracking
                    file_size = sftp.stat(newest_csv).st_size
                    self.logger.info(f"📦 File size: {file_size / (1024*1024):.1f} MB")
                    
                    # Download the file with progress tracking
                    start_time = datetime.now()
                    downloaded_bytes = [0]  # Use list to allow modification in callback
                    
                    # Track last update time to avoid too frequent updates
                    last_update_time = [0]
                    
                    def progress_callback(transferred, total):
                        """Callback function to show download progress on same line"""
                        downloaded_bytes[0] = transferred
                        current_time = datetime.now().timestamp()
                        
                        # Update progress every 0.5 seconds to avoid spam
                        if current_time - last_update_time[0] >= 0.5:
                            last_update_time[0] = current_time
                            
                            if total > 0:
                                percent = (transferred / total) * 100
                                mb_transferred = transferred / (1024*1024)
                                mb_total = total / (1024*1024)
                                
                                elapsed = (datetime.now() - start_time).total_seconds()
                                speed = mb_transferred / elapsed if elapsed > 0 else 0
                                eta = (mb_total - mb_transferred) / speed if speed > 0 else 0
                                
                                # Create progress bar
                                bar_length = 20
                                filled_length = int(bar_length * percent / 100)
                                bar = '█' * filled_length + '▒' * (bar_length - filled_length)
                                
                                # Print progress on same line using \r
                                progress_msg = f"\r⬇️ [{bar}] {percent:.1f}% ({mb_transferred:.1f}/{mb_total:.1f} MB) {speed:.1f} MB/s ETA: {eta:.0f}s"
                                print(progress_msg, end='', flush=True)
                    
                    self.logger.info(f"⬇️ Starting download of {newest_csv}...")
                    sftp.get(newest_csv, str(local_path), callback=progress_callback)
                    
                    # Print newline to finish progress line
                    print()  # Move to next line after progress
                    
                    download_duration = (datetime.now() - start_time).total_seconds()
                    
                    # Final download statistics
                    final_mb = downloaded_bytes[0] / (1024*1024)
                    download_speed = final_mb / download_duration if download_duration > 0 else 0
                    
                    self.logger.info(f"⬇️ File downloaded to: {local_path}")
                    self.logger.info(f"⏱️ Download completed in {download_duration:.2f} seconds ({download_speed:.1f} MB/s)")
                    
                    return local_path
                
            except Exception as e:
                error_msg = f"SFTP download failed for {source_name} (attempt {attempt + 1}/{max_retries}): {str(e)}"
                self.logger.warning(error_msg)
                if attempt == max_retries - 1:  # Last attempt
                    self.metrics.record_error(error_msg, "sftp_download")
                    raise
                continue
    
    def extract(self, source_name: str, **kwargs) -> pd.DataFrame:
        """Extract data from SFTP server or fallback to CSV"""
        try:
            # Try SFTP extraction first
            self.logger.info(f"🔄 Attempting SFTP extraction for {source_name}")
            
            # Download file from SFTP
            downloaded_file = self._download_from_sftp(source_name)
            
            # Load the downloaded CSV file
            start_time = datetime.now()
            df = pd.read_csv(downloaded_file)
            duration = (datetime.now() - start_time).total_seconds()
            
            self._record_extraction(source_name, len(df), duration)
            self.logger.info(f"✅ SFTP extraction successful: {len(df)} rows from {source_name}")
            
            return df
            
        except Exception as e:
            self.logger.warning(f"⚠️ SFTP extraction failed for {source_name}: {str(e)}")
            self.logger.info(f"🔄 Falling back to CSV extraction for {source_name}")
            
            # Fallback to CSV extraction
            csv_extractor = FilimoCSVExtractor(self.config, self.metrics)
            return csv_extractor.extract(source_name, **kwargs)


class FilimoAPIExtractor(BaseExtractor):
    """Extract data from Metabase API for Filimo"""
    
    def __init__(self, config: FilimoConfig, metrics: BaseMetrics):
        super().__init__(config, metrics)
        self.config = config
        self._setup_download_directory()
    
    def _setup_download_directory(self):
        """Ensure assets directory exists"""
        self.download_dir = self.config.base_dir
        self.download_dir.mkdir(exist_ok=True)
    
    def _download_from_api(self, source_name: str) -> pd.DataFrame:
        """Download data directly from Metabase API with retry logic"""
        import time
        
        api_url = self.config.source.metabase_api_url
        api_key = self.config.source.api_key
        
        self.logger.info(f"🔗 Connecting to Metabase API for {source_name}")
        
        headers = {
            "X-API-Key": api_key,
            "Content-Type": "application/json"
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    self.logger.info(f"🔄 Retry attempt {attempt + 1}/{max_retries} for {source_name}")
                    
                    # Add delay between retries (exponential backoff)
                    delay = 2 ** attempt  # 2, 4, 8 seconds
                    self.logger.info(f"⏳ Waiting {delay} seconds before retry...")
                    time.sleep(delay)
                
                start_time = datetime.now()
                self.logger.info(f"📡 Making API request to: {api_url}")
                
                # Make the API request with streaming for progress tracking
                response = requests.post(api_url, headers=headers, timeout=60, stream=True)
                response.raise_for_status()
                
                # Get total size if available
                total_size = response.headers.get('content-length')
                if total_size:
                    total_size = int(total_size)
                    total_mb = total_size / (1024*1024)
                    self.logger.info(f"📦 Expected response size: {total_mb:.1f} MB")
                else:
                    total_size = None
                    self.logger.info(f"📦 Response size unknown - downloading...")
                
                # Download with progress tracking
                downloaded_data = []
                downloaded_bytes = 0
                last_update_time = [0]
                
                def show_progress(current_bytes, total_bytes):
                    """Show download progress on same line"""
                    current_time = datetime.now().timestamp()
                    
                    # Update progress every 0.5 seconds
                    if current_time - last_update_time[0] >= 0.5:
                        last_update_time[0] = current_time
                        
                        elapsed = (datetime.now() - start_time).total_seconds()
                        mb_downloaded = current_bytes / (1024*1024)
                        speed = mb_downloaded / elapsed if elapsed > 0 else 0
                        
                        if total_bytes:
                            percent = (current_bytes / total_bytes) * 100
                            mb_total = total_bytes / (1024*1024)
                            eta = (mb_total - mb_downloaded) / speed if speed > 0 else 0
                            
                            # Create progress bar
                            bar_length = 20
                            filled_length = int(bar_length * percent / 100)
                            bar = '█' * filled_length + '▒' * (bar_length - filled_length)
                            
                            progress_msg = f"\r⬇️ [{bar}] {percent:.1f}% ({mb_downloaded:.1f}/{mb_total:.1f} MB) {speed:.1f} MB/s ETA: {eta:.0f}s"
                        else:
                            # Unknown total size - same format but no percentage
                            progress_msg = f"\r⬇️ Downloading... {mb_downloaded:.1f} MB @ {speed:.1f} MB/s"
                        
                        print(progress_msg, end='', flush=True)
                
                # Download data in chunks with progress
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        downloaded_data.append(chunk)
                        downloaded_bytes += len(chunk)
                        
                        # Show progress
                        show_progress(downloaded_bytes, total_size)
                
                # Print newline to finish progress line
                print()  # Move to next line after progress
                
                request_duration = (datetime.now() - start_time).total_seconds()
                
                # Combine all chunks
                full_content = b''.join(downloaded_data)
                content_mb = len(full_content) / (1024*1024)
                download_speed = content_mb / request_duration if request_duration > 0 else 0
                
                self.logger.info(f"✅ API response received: {content_mb:.1f} MB in {request_duration:.2f} seconds ({download_speed:.1f} MB/s)")
                
                # Parse CSV data directly from response
                csv_data = io.StringIO(full_content.decode('utf-8'))
                df = pd.read_csv(csv_data)
                
                # Optionally save a copy locally for backup
                if source_name == "imported_before":
                    backup_path = self.download_dir / f"{source_name}_api_backup.csv"
                    df.to_csv(backup_path, index=False)
                    self.logger.info(f"💾 API data backed up to: {backup_path}")
                
                self.logger.info(f"📊 Parsed {len(df)} rows from API response")
                return df
                
            except requests.exceptions.Timeout as e:
                error_msg = f"API request timeout for {source_name} (attempt {attempt + 1}/{max_retries})"
                self.logger.warning(error_msg)
                if attempt == max_retries - 1:  # Last attempt
                    self.metrics.record_error(error_msg, "api_download")
                    raise
                continue
                
            except requests.exceptions.RequestException as e:
                error_msg = f"API request failed for {source_name} (attempt {attempt + 1}/{max_retries}): {str(e)}"
                self.logger.warning(error_msg)
                if attempt == max_retries - 1:  # Last attempt
                    self.metrics.record_error(error_msg, "api_download")
                    raise
                continue
                
            except Exception as e:
                error_msg = f"API data parsing failed for {source_name}: {str(e)}"
                self.logger.error(error_msg)
                self.metrics.record_error(error_msg, "api_parsing")
                raise  # Don't retry parsing errors
    
    def extract(self, source_name: str, **kwargs) -> pd.DataFrame:
        """Extract data from Metabase API or fallback to CSV"""
        try:
            # Try API extraction first
            self.logger.info(f"🔄 Attempting API extraction for {source_name}")
            
            # Only use API for imported_before data
            if source_name == "imported_before":
                df = self._download_from_api(source_name)
                
                # Record extraction metrics
                self._record_extraction(source_name, len(df))
                self.logger.info(f"✅ API extraction successful: {len(df)} rows from {source_name}")
                
                return df
            else:
                # For other sources, fall back to CSV immediately
                self.logger.info(f"📁 Using CSV extractor for {source_name} (API only for imported_before)")
                csv_extractor = FilimoCSVExtractor(self.config, self.metrics)
                return csv_extractor.extract(source_name, **kwargs)
            
        except Exception as e:
            self.logger.warning(f"⚠️ API extraction failed for {source_name}: {str(e)}")
            self.logger.info(f"🔄 Falling back to CSV extraction for {source_name}")
            
            # Fallback to CSV extraction
            csv_extractor = FilimoCSVExtractor(self.config, self.metrics)
            return csv_extractor.extract(source_name, **kwargs)
