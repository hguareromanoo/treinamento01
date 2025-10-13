#!/usr/bin/env python3
"""
Main execution script for pharmacy data scraping
Designed to run on AWS EC2 instances

This script:
1. Scrapes data from Veracruz and Farmaponte pharmacies
2. Consolidates results into a single DataFrame
3. Saves the consolidated data to S3
4. Invokes a Lambda function to stop itself upon completion or failure
"""

import asyncio
import pandas as pd
import time
from datetime import datetime
import os
import sys
import logging
from pathlib import Path
import json
import boto3
import requests # Usado para obter metadados da instância

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
    # Para EC2 com IAM Role, as credenciais são obtidas automaticamente pelo boto3.
    # A validação de variáveis de ambiente para chaves não é mais estritamente necessária.
    aws_region = os.getenv('AWS_REGION', 'sa-east-1') # Default para sua região
    logger.info(f"AWS Region configured: {aws_region}")
    
    # Set S3 bucket name (can be overridden by environment variable)
    bucket_name = os.getenv('AWS_BUCKET_NAME', 'pharmacy-data-bucket')
    logger.info(f"Using S3 bucket: {bucket_name}")
    
    return bucket_name, aws_region


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
    if veracruz_df is not None:
        veracruz_df = veracruz_df.copy()
        veracruz_df['Farmácia'] = 'Vera Cruz'
    
    if farmaponte_df is not None:
        farmaponte_df = farmaponte_df.copy()
        farmaponte_df['Farmácia'] = 'Farmaponte'
    
    # Concatenate DataFrames
    consolidated_df = pd.concat([veracruz_df, farmaponte_df], ignore_index=True)
    
    # Reorder columns to put 'Farmácia' as first column
    cols = consolidated_df.columns.tolist()
    cols = ['Farmácia'] + [col for col in cols if col != 'Farmácia']
    consolidated_df = consolidated_df[cols]
    
    logger.info(f"Consolidated DataFrame created with {len(consolidated_df)} total records")
    if veracruz_df is not None:
        logger.info(f"Veracruz records: {len(veracruz_df)}")
    if farmaponte_df is not None:
        logger.info(f"Farmaponte records: {len(farmaponte_df)}")
    
    return consolidated_df


def generate_filename(type):
    """Generate timestamped filename for the extraction results"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{type}_results_{timestamp}.csv"


async def main():
    """
    Main execution function
    """
    start_time = time.time()
    logger.info("🚀 Starting pharmacy data extraction process...")
    
    # A função de parada será chamada no bloco 'finally' para garantir a execução
    try:
        # Tenta extrair do BigQuery primeiro
        try:
            # Setup environment for BigQuery data extraction
            bucket_name, _ = setup_environment()
            
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
                query = f"SELECT * FROM `Farmacias.{table_id}`"
                tmp_df = bq_client.query(query).to_dataframe()
                tmp_df['Farmácia'] = name
                df = tmp_df if df is None else pd.concat([df, tmp_df], ignore_index=True)
                logger.info(f"Extracted {len(tmp_df):,} records from {name}")
            
            logger.info(f"BigQuery extraction completed with {len(df):,} total records")
            
            local_filename = generate_filename(typ='bigquery_extraction')
            df.to_csv(local_filename, index=False, encoding='utf-8')
            
            s3_key = f"data/{local_filename}"
            upload_file_to_s3(local_file=local_filename, bucket_name=bucket_name, s3_file=s3_key)

        except Exception as e:
            logger.warning(f"Could not connect to BigQuery: {e}. Proceeding with web scraping.")
            # Se a extração do BigQuery falhar, executa o scraping
            bucket_name, _ = setup_environment()
            veracruz = VeraCruzScraper()
            farmaponte = FarmaponteScraper()
            
            logger.info("Starting parallel scraping operations...")
            veracruz_task = asyncio.create_task(veracruz.scrape())
            farmaponte_task = asyncio.create_task(farmaponte.scrape())
            
            logger.info("Waiting for scraping operations to complete...")
            veracruz_df, farmaponte_df = await asyncio.gather(veracruz_task, farmaponte_task, return_exceptions=True)

            if isinstance(veracruz_df, Exception):
                logger.error(f"Veracruz scraping failed: {veracruz_df}")
                veracruz_df = pd.DataFrame() # Cria um DF vazio para não quebrar a consolidação
                
            if isinstance(farmaponte_df, Exception):
                logger.error(f"Farmaponte scraping failed: {farmaponte_df}")
                farmaponte_df = pd.DataFrame() # Cria um DF vazio

            consolidated_df = consolidate_dataframes(veracruz_df, farmaponte_df)
            
            if not consolidated_df.empty:
                local_filename = generate_filename(typ='extraction')
                consolidated_df.to_csv(local_filename, index=False, encoding='utf-8')
                
                s3_key = f"data/{local_filename}"
                logger.info(f"Uploading to S3: s3://{bucket_name}/{s3_key}")
                upload_file_to_s3(local_file=local_filename, bucket_name=bucket_name, s3_file=s3_key)
            else:
                logger.warning("No data was scraped, skipping file generation and S3 upload.")

        execution_time = time.time() - start_time
        logger.info(f"🎉 Extraction process completed successfully in {execution_time:.2f} seconds!")

    except Exception as e:
        logger.error(f"❌ A fatal error occurred during the main process: {str(e)}")
        logger.exception("Full error details:")
    finally:
        # ESTE BLOCO SEMPRE SERÁ EXECUTADO
        logger.info("Process finished. Invoking Lambda to stop the instance.")
        invoke_stop_lambda()

def invoke_stop_lambda():
    """
    Invokes the AWS Lambda function to stop the current EC2 instance.
    """
    lambda_function_name = "G3-StartStopEC2Instances"
    lambda_region = "sa-east-1"
    
    try:
        # Obter o ID da instância a partir do serviço de metadados da EC2


        # Criar o cliente Lambda
        # A IAM Role da instância deve ter a permissão lambda:InvokeFunction
        lambda_client = boto3.client('lambda', region_name=lambda_region)
        
        # Preparar o payload para a Lambda
        payload = {
            "action": "stop",
        }
        
        logger.info(f"Invoking Lambda '{lambda_function_name}' with payload: {json.dumps(payload)}")
        
        # Invocar a Lambda
        lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType='Event',  # 'Event' para chamada assíncrona (não espera resposta)
            Payload=json.dumps(payload)
        )
        
        logger.info("Successfully invoked stop Lambda. The instance should stop shortly.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Could not get instance ID from metadata service: {e}")
    except Exception as e:
        logger.error(f"Failed to invoke Lambda function '{lambda_function_name}': {e}")


# Wrapper síncrono para a função async main
def run_extraction():
    return asyncio.run(main())


if __name__ == "__main__":
    try:
        run_extraction()
        logger.info("✅ Script execution logic completed.")
    except Exception as e:
        logger.error(f"💥 Fatal error in script execution: {str(e)}")
        sys.exit(1)