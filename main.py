#!/usr/bin/env python3
"""
Main execution script for pharmacy data scraping
Designed to run on AWS EC2 instances

This script:
1. Scrapes data from Veracruz and Farmaponte pharmacies
2. Consolidates results into a single DataFrame
3. Saves the consolidated data to S3
"""

import asyncio
import pandas as pd
import time
from datetime import datetime
import os
import sys
import logging
from pathlib import Path

# Import our scraper functions
from scrapper import VeraCruzScraper, FarmaponteScraper
from utils.save_to_s3 import upload_file_to_s3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def setup_environment():
    """Setup environment variables and validate configuration"""
    required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        raise ValueError(f"Missing environment variables: {missing_vars}")
    
    # Set S3 bucket name (can be overridden by environment variable)
    bucket_name = os.getenv('AWS_BUCKET_NAME', 'pharmacy-data-bucket')
    logger.info(f"Using S3 bucket: {bucket_name}")
    
    return bucket_name


def consolidate_dataframes(veracruz_df, farmaponte_df):
    """
    Consolidate both DataFrames into a single one with pharmacy identifier
    
    Args:
        veracruz_df (pd.DataFrame): Veracruz scraping results
        farmaponte_df (pd.DataFrame): Farmaponte scraping results
    
    Returns:
        pd.DataFrame: Consolidated DataFrame with 'Farmácia' column
    """
    logger.info("Consolidating DataFrames...")
    
    # Add pharmacy identifier column
    veracruz_df = veracruz_df.copy()
    veracruz_df['Farmácia'] = 'Vera Cruz'
    
    farmaponte_df = farmaponte_df.copy()
    farmaponte_df['Farmácia'] = 'Farmaponte'
    
    # Concatenate DataFrames
    consolidated_df = pd.concat([veracruz_df, farmaponte_df], ignore_index=True)
    
    # Reorder columns to put 'Farmácia' as first column
    cols = consolidated_df.columns.tolist()
    cols = ['Farmácia'] + [col for col in cols if col != 'Farmácia']
    consolidated_df = consolidated_df[cols]
    
    logger.info(f"Consolidated DataFrame created with {len(consolidated_df)} total records")
    logger.info(f"Veracruz records: {len(veracruz_df)}")
    logger.info(f"Farmaponte records: {len(farmaponte_df)}")
    
    return consolidated_df


def generate_filename(typ: str):
    """Generate timestamped filename for the extraction results"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{typ}_results_{timestamp}.csv"


async def main():
    """
    Main execution function
    """
    start_time = time.time()
    logger.info("🚀 Starting pharmacy data extraction process...")
    
    try:
        # Setup environment for BigQuery data extraction
        bucket_name = setup_environment()
        
        from google.cloud import bigquery
        bq_client = bigquery.Client()
        pharmacy_tables = {
            'Farmaponte': 'Historico_Vendas_Farma_Ponte',
            'Sao Joao': 'Historico_Vendas_Sao_Joao',
            'Sao Paulo': 'Historico_Vendas_Sao_Paulo',
            'Vera Cruz': 'Historico_Vendas_Vera_Cruz'
        }
        
        logger.info("Starting BigQuery data extraction...")
        df = None
        
        for name, table_id in pharmacy_tables.items():
            logger.info(f"Querying data from {name} ({table_id})...")
            query = f"""
            SELECT *
            FROM `Farmacias.{table_id}`
            """
            tmp_df = bq_client.query(query).to_dataframe()
            tmp_df['Farmácia'] = name
            df = tmp_df if df is None else pd.concat([df, tmp_df], ignore_index=True)
            logger.info(f"Extracted {len(tmp_df):,} records from {name}")
        
        logger.info(f"BigQuery extraction completed with {len(df):,} total records")
        
        # Generate filename with timestamp
        local_filename = generate_filename(typ='bigquery_extraction')
        
        # Save consolidated results locally
        logger.info(f"Saving BigQuery results to {local_filename}")
        df.to_csv(local_filename, index=False, encoding='utf-8')
        
        # Verify file was created and get file size
        if not Path(local_filename).exists():
            raise FileNotFoundError(f"Failed to create local file: {local_filename}")
            
        file_size_mb = Path(local_filename).stat().st_size / (1024 * 1024)
        logger.info(f"Local file created successfully: {file_size_mb:.2f} MB")
        
        # Upload to S3
        s3_key = f"data/{local_filename}"
        logger.info(f"Uploading BigQuery data to S3: s3://{bucket_name}/{s3_key}")
        
        upload_file_to_s3(
            local_file=local_filename,
            bucket_name=bucket_name,
            s3_file=s3_key
        )
        
        # Calculate execution time
        execution_time = time.time() - start_time
        
        # Log final statistics
        logger.info("🎉 BigQuery extraction process completed successfully!")
        logger.info(f"⏱️  Total execution time: {execution_time:.2f} seconds")
        logger.info(f"📊 Total records extracted: {len(df):,}")
        logger.info(f"📁 File saved to S3: s3://{bucket_name}/{s3_key}")
        
        # Display sample of consolidated data
        logger.info("\n📋 Sample of BigQuery data:")
        logger.info(f"\nColumns: {list(df.columns)}")
        logger.info(f"\nFirst few records:\n{df.head().to_string()}")
        
        # Display pharmacy distribution
        pharmacy_counts = df['Farmácia'].value_counts()
        logger.info(f"\n🏪 Records by pharmacy:")
        for pharmacy, count in pharmacy_counts.items():
            logger.info(f"  {pharmacy}: {count:,} records")
        
        # Optional: Clean up local file (comment out if you want to keep it)
        try:
            os.remove(local_filename)
            logger.info(f"Local file {local_filename} cleaned up")
        except Exception as e:
            logger.warning(f"Could not remove local file: {e}")
        
        return df

    except Exception as e:
        logger.warning(f"Could not connect to BigQuery: {e}")
        logger.info("Proceeding with web scraping instead...")

    try:
        # Setup environment
        bucket_name = setup_environment()
        veracruz = VeraCruzScraper()
        farmaponte = FarmaponteScraper()
        
        # Run both scrapers concurrently
        logger.info("Starting parallel scraping operations...")
        veracruz_task = asyncio.create_task(veracruz.scrape())
        farmaponte_task = asyncio.create_task(farmaponte.scrape())
        
        # Wait for both scrapers to complete
        logger.info("Waiting for scraping operations to complete...")
        veracruz_df, farmaponte_df = await asyncio.gather(
            veracruz_task, 
            farmaponte_task,
            return_exceptions=True
        )

        # Handle potential exceptions
        if isinstance(veracruz_df, Exception):
            logger.error(f"Veracruz scraping failed: {veracruz_df}")
            raise veracruz_df
            
        if isinstance(farmaponte_df, Exception):
            logger.error(f"Farmaponte scraping failed: {farmaponte_df}")
            raise farmaponte_df
        
        # Consolidate results
        consolidated_df = consolidate_dataframes(veracruz_df, farmaponte_df)
        
        # Generate filename with timestamp
        local_filename = generate_filename(typep='extraction')
        
        # Save consolidated results locally
        logger.info(f"Saving consolidated results to {local_filename}")
        consolidated_df.to_csv(local_filename, index=False, encoding='utf-8')
        
        # Verify file was created and get file size
        if not Path(local_filename).exists():
            raise FileNotFoundError(f"Failed to create local file: {local_filename}")
            
        file_size_mb = Path(local_filename).stat().st_size / (1024 * 1024)
        logger.info(f"Local file created successfully: {file_size_mb:.2f} MB")
        
        # Upload to S3
        s3_key = f"data/{local_filename}"
        logger.info(f"Uploading to S3: s3://{bucket_name}/{s3_key}")
        
        upload_file_to_s3(
            local_file=local_filename,
            bucket_name=bucket_name,
            s3_file=s3_key
        )
        
        # Calculate execution time
        execution_time = time.time() - start_time
        
        # Log final statistics
        logger.info("🎉 Extraction process completed successfully!")
        logger.info(f"⏱️  Total execution time: {execution_time:.2f} seconds")
        logger.info(f"📊 Total records extracted: {len(consolidated_df):,}")
        logger.info(f"📁 File saved to S3: s3://{bucket_name}/{s3_key}")
        
        # Display sample of consolidated data
        logger.info("\n📋 Sample of consolidated data:")
        logger.info(f"\nColumns: {list(consolidated_df.columns)}")
        logger.info(f"\nFirst few records:\n{consolidated_df.head().to_string()}")
        
        # Display pharmacy distribution
        pharmacy_counts = consolidated_df['Farmácia'].value_counts()
        logger.info(f"\n🏪 Records by pharmacy:")
        for pharmacy, count in pharmacy_counts.items():
            logger.info(f"  {pharmacy}: {count:,} products")
        
        # Optional: Clean up local file (comment out if you want to keep it)
        try:
            os.remove(local_filename)
            logger.info(f"Local file {local_filename} cleaned up")
        except Exception as e:
            logger.warning(f"Could not remove local file: {e}")
        
        return consolidated_df
        
    except Exception as e:
        logger.error(f"❌ Error during extraction process: {str(e)}")
        logger.exception("Full error details:")
        raise


def run_extraction():
    """
    Synchronous wrapper for the async main function
    Useful for calling from other scripts
    """
    return asyncio.run(main())


if __name__ == "__main__":
    """
    Entry point when script is run directly
    """
    try:
        # Set up signal handling for graceful shutdown (useful on EC2)
        import signal
        
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}. Shutting down gracefully...")
            sys.exit(0)
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        # Run the extraction
        result_df = run_extraction()
        
        logger.info("✅ Script execution completed successfully")
        
    except KeyboardInterrupt:
        logger.info("🛑 Script interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"💥 Fatal error: {str(e)}")
        sys.exit(1)