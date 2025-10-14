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

async def main():
    """
    Main execution function - Runs BOTH BigQuery AND Scraping
    Saves separate CSV files for each source
    """
    start_time = time.time()
    logger.info("🚀 Starting pharmacy data extraction process...")
    
    bucket_name, _ = setup_environment()
    bigquery_success = False
    scraping_success = False
    df_bq = None
    df_scraping = None
    
    try:
        # ========================================
        # BLOCO 1: BIGQUERY (sempre executar)
        # ========================================
        try:
            logger.info("\n" + "="*80)
            logger.info("📊 PHASE 1: BigQuery Extraction")
            logger.info("="*80)
            
            from google.cloud import bigquery
            bq_client = bigquery.Client()
            
            pharmacy_tables = {
                'Farmaponte': 'Historico_Vendas_Farma_Ponte',
                'Sao Joao': 'Historico_Vendas_Sao_Joao',
                'Sao Paulo': 'Historico_Vendas_Sao_Paulo',
                'Vera Cruz': 'Historico_Vendas_Vera_Cruz'
            }
            
            df_bq = None
            for name, table_id in pharmacy_tables.items():
                logger.info(f"Querying data from {name} ({table_id})...")
                query = f"SELECT * FROM `Farmacias.{table_id}`"
                tmp_df = bq_client.query(query).to_dataframe()
                tmp_df['Farmacia'] = name  # Coluna padronizada
                df_bq = tmp_df if df_bq is None else pd.concat([df_bq, tmp_df], ignore_index=True)
                logger.info(f"Extracted {len(tmp_df):,} records from {name}")
            
            if df_bq is not None and not df_bq.empty:
                logger.info(f"✅ BigQuery SUCCESS: {len(df_bq):,} total records")
                
                # SALVAR BIGQUERY SEPARADAMENTE
                bq_filename ='bigquery_extraction_results.csv'
                df_bq.to_csv(bq_filename, index=False, encoding='utf-8')
                
                s3_key_bq = f"data/{bq_filename}"
                logger.info(f"📤 Uploading BigQuery to S3: s3://{bucket_name}/{s3_key_bq}")
                upload_file_to_s3(local_file=bq_filename, bucket_name=bucket_name, s3_file=s3_key_bq)
                
                # Limpar arquivo local
                os.remove(bq_filename)
                logger.info(f"🗑️  Local file {bq_filename} cleaned up")
                
                bigquery_success = True
            else:
                logger.warning("⚠️  BigQuery returned no data")
                
        except Exception as e:
            logger.error(f"❌ BigQuery FAILED: {str(e)}")
            logger.exception("BigQuery error details:")
        
        # ========================================
        # BLOCO 2: WEB SCRAPING (sempre executar)
        # ========================================
        try:
            logger.info("\n" + "="*80)
            logger.info("🕷️  PHASE 2: Web Scraping")
            logger.info("="*80)
            
            veracruz = VeraCruzScraper()
            farmaponte = FarmaponteScraper()
            
            logger.info("Starting parallel scraping operations...")
            veracruz_task = asyncio.create_task(veracruz.scrape())
            farmaponte_task = asyncio.create_task(farmaponte.scrape())
            
            logger.info("Waiting for scraping operations to complete...")
            veracruz_df, farmaponte_df = await asyncio.gather(
                veracruz_task,
                farmaponte_task,
                return_exceptions=True
            )
            
            # Tratar exceções individuais
            if isinstance(veracruz_df, Exception):
                logger.error(f"Veracruz scraping failed: {veracruz_df}")
                veracruz_df = None
            
            if isinstance(farmaponte_df, Exception):
                logger.error(f"Farmaponte scraping failed: {farmaponte_df}")
                farmaponte_df = None
            
            # Consolidar scraping
            scraping_dfs = []
            if veracruz_df is not None and not veracruz_df.empty:
                veracruz_df = veracruz_df.copy()
                veracruz_df['Farmacia'] = 'Vera Cruz'  # Coluna padronizada
                scraping_dfs.append(veracruz_df)
                logger.info(f"Veracruz: {len(veracruz_df):,} records")
            
            if farmaponte_df is not None and not farmaponte_df.empty:
                farmaponte_df = farmaponte_df.copy()
                farmaponte_df['Farmacia'] = 'Farmaponte'  # Coluna padronizada
                scraping_dfs.append(farmaponte_df)
                logger.info(f"Farmaponte: {len(farmaponte_df):,} records")
            
            if scraping_dfs:
                df_scraping = pd.concat(scraping_dfs, ignore_index=True)
                logger.info(f"✅ Scraping SUCCESS: {len(df_scraping):,} total records")
                
                # SALVAR SCRAPING SEPARADAMENTE
                scraping_filename ='extraction_results.csv'
                df_scraping.to_csv(scraping_filename, index=False, encoding='utf-8')
                
                s3_key_scraping = f"data/{scraping_filename}"
                logger.info(f"📤 Uploading Scraping to S3: s3://{bucket_name}/{s3_key_scraping}")
                upload_file_to_s3(local_file=scraping_filename, bucket_name=bucket_name, s3_file=s3_key_scraping)
                
                # Limpar arquivo local
                os.remove(scraping_filename)
                logger.info(f"🗑️  Local file {scraping_filename} cleaned up")
                
                scraping_success = True
            else:
                logger.warning("⚠️  Scraping returned no data")
                
        except Exception as e:
            logger.error(f"❌ Scraping FAILED: {str(e)}")
            logger.exception("Scraping error details:")
        
        # ========================================
        # RESUMO FINAL
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("📊 FINAL RESULTS SUMMARY")
        logger.info("="*80)
        logger.info(f"BigQuery: {'✅ SUCCESS' if bigquery_success else '❌ FAILED'}")
        logger.info(f"Scraping: {'✅ SUCCESS' if scraping_success else '❌ FAILED'}")
        
        if bigquery_success:
            logger.info(f"  BigQuery records: {len(df_bq):,}")
        if scraping_success:
            logger.info(f"  Scraping records: {len(df_scraping):,}")
        
        if not bigquery_success and not scraping_success:
            logger.error("💥 FATAL: Both BigQuery and Scraping failed!")
            raise ValueError("No data extracted from any source!")
        
        execution_time = time.time() - start_time
        logger.info(f"\n🎉 Extraction process completed in {execution_time:.2f} seconds!")
        
        return df_bq, df_scraping
        
    except Exception as e:
        logger.error(f"❌ Fatal error in main: {str(e)}")
        logger.exception("Full error details:")
        raise
        
    finally:
        # SEMPRE CHAMAR LAMBDA, INDEPENDENTE DE SUCESSO OU ERRO
        logger.info("\n" + "="*80)
        logger.info("🛑 INVOKING LAMBDA TO STOP INSTANCE")
        logger.info("="*80)
        invoke_stop_lambda()
# Wrapper síncrono para a função async main
def run_extraction():
    return asyncio.run(main())


if __name__ == "__main__":
    try:
        run_extraction()
        logger.info("✅ Script execution logic completed.")
    except Exception as e:
        logger.error(f"💥 Fatal error in script execution: {str(e)}")
        sys.exit

