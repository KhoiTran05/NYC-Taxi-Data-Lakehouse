from pathlib import Path
import time
import json
from wsgiref import headers
from sqlalchemy import create_engine
import requests
import os
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataBackfillProccessor:
    def __init__(self, data_dir="data/taxi/backfill"):
        self.db_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/taxi_db")
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.db_engine = create_engine(self.db_url)
        
    def get_single_taxi_data(self, year, month):
        """Get NYC taxi data for a specific year and month"""
        base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
        url = f"{base_url}/{file_name}"
        
        local_file_path = self.data_dir / file_name
        if local_file_path.exists():
            logger.info(f"File {file_name} already exists locally.")
            return local_file_path
        
        try:
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Range": "bytes=0-"
            }

            response = requests.get(
                url,
                headers=headers,
                stream=True,
                timeout=60
            )
            response.raise_for_status()
            
            downloaded = 0
            with open(local_file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

            if downloaded == 0:
                raise ValueError(f"Downloaded file is empty: {file_name}")
            
            size_mb = downloaded / (1024 * 1024)
            logger.info(f"Downloaded {file_name}: {size_mb:.2f} MB")
            
            return local_file_path
        except requests.RequestException as e:
            logger.error(f"Failed to download {file_name}: {e}")
            if local_file_path.exists():
                local_file_path.unlink() 
            raise
        except Exception as e:
            logger.exception(f"Error during taxi data download: {e}")
            raise
        
    def get_multiple_taxi_data(self, start_year, start_month, end_year, end_month):
        """Get NYC taxi data for multiple months and years"""
        file_paths = []
        logger.info(f"Starting download taxi data from {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
        
        current_month = start_month
        current_year = start_year
        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            try:
                file_path = self.get_single_taxi_data(current_year, current_month)
                if file_path:
                    file_paths.append(file_path)
            except Exception as e:
                logger.warning(f"Skipping {current_year}-{current_month:02d} due to error: {e}")
                
            time.sleep(1)

            if current_month == 12:
                current_month = 1
                current_year += 1
            else:
                current_month += 1
                
        logger.info(f"Completed downloading taxi data from {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
        return file_paths
    
    def validate_file(self, file_path):
        """Validate the downloaded file"""
        if not file_path.exists():
            logger.error(f"File {file_path} does not exist.")
            return 
        
        try:
            df = pd.read_parquet(file_path)
            return {
                "file_name": file_path.name,
                "num_records": len(df),
                "num_columns": len(df.columns),
                "null_counts": df.isnull().sum().sum(),
                "columns": df.columns.tolist(),
                "status": "valid"
            }
        except Exception as e:
            logger.error(f"Failed to validate {file_path.name}: {e}")
            return {
                "file_name": file_path.name,
                "status": "invalid",
                "error": str(e)
            }
            
    def validate_all(self):
        """Validate multiple downloaded files"""
        files = sorted(self.data_dir.glob("*.parquet"))
        results = []
        
        for file_path in files:
            stats = self.validate_file(file_path)
            results.append(stats)
            
            if stats['status'] == 'valid':
                logger.info(f"{stats['file_name']}: {stats['num_records']:,} rows, {stats['num_columns']} columns")
            else:
                logger.info(f"{stats['file_name']}: {stats['status']}")
                
        return results
    
    def insert_file(self, file_path, table_name="taxi.trips", if_exists="append"):
        """Insert a single file into database"""
        if not self.db_engine:
            logger.error(f"Database connection not configured")
            return 
        
        logger.info(f"Inserting {file_path.name} to table {table_name}")
        try:
            df = pd.read_parquet(file_path)
            df.to_sql(
                name=table_name,
                con=self.db_engine,
                if_exists=if_exists,
                index=False,
                chunksize=10000,
                method="multi",
            )
            
            logger.info(f"Inserted {len(df)} rows")
            return True
        except Exception as e:
            logger.exception(f"Error during {file_path.name} insertion: {e}")
            raise
        
    def insert_all(self, table_name="taxi.trips", pattern="*.parquet"):
        """Load validated data into the database"""
        files = sorted(self.data_dir.glob(pattern))
        success_counts = 0
        file_counts = len(files)
        
        logger.info(f"Inserting {file_counts} files to {table_name}")
        
        for i, file_path in enumerate(files):
            if_exists = "replace" if i == 0 else "append"
            try:
                self.insert_file(file_path, table_name, if_exists)
                success_counts += 1
            except Exception as e:
                logger.warning(f"Skipping {file_path.name} due to insertion error: {e}")
            
        logger.info(f"Successfully inserted {success_counts}/{file_counts} files to {table_name}")
        
    def clean_up(self):
        """Deleting files locally after successfully insert"""
        to_delete_files = sorted(self.data_dir.glob("*.parquet"))
        
        for file_path in to_delete_files:
            file_path.unlink()
            logger.info(f"Deleted {file_path.name}")
            
        logger.info(f"Cleaned up {len(to_delete_files)} files")
        
def main():
    """Main function to process data backfill"""
    logger.info("Starting data backfill process")
    
    try:
        processor = DataBackfillProccessor(data_dir="data")
        file_paths = processor.get_multiple_taxi_data(2025, 1, 2025, 11)
        
        if not file_paths:
            logger.warning("No files downloaded, exiting process")
            return
        
        validation_results = processor.validate_all()
        logger.info(
            "Validation results:\n%s",
            json.dumps(validation_results, indent=2)
        )
        
        processor.insert_all(table_name="taxi.trips")
    except Exception as e:
        logger.exception(f"Data backfill process failed: {e}")
    finally:
        processor.clean_up()

if __name__ == "__main__":
    main()  