import boto3
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv
load_dotenv()


def upload_file_to_s3(local_file, bucket_name, s3_file):
    """Upload a file to an S3 bucket

    :param local_file: File to upload
    :param bucket_name: Bucket to upload to
    :param s3_file: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # Create an S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    # Upload arquivo para S3
    try:
        s3_client.upload_file(
            local_file,  # Arquivo local
            bucket_name,  # Nome do bucket
            s3_file       # Nome/caminho no S3
        )
        print(f"Upload realizado com sucesso: {local_file} -> s3://{bucket_name}/{s3_file}")
        return True
    except ClientError as e:
        print(f"Erro no upload: {e}")
        return False
    except FileNotFoundError as e:
        print(f"Arquivo não encontrado: {e}")
        return False
    except Exception as e:
        print(f"Erro inesperado: {e}")
        return False


def upload_and_cleanup(local_file, bucket_name, s3_file, delete_local=True):
    """
    Upload file to S3 and optionally delete local file after successful upload
    
    :param local_file: Local file path to upload
    :param bucket_name: S3 bucket name
    :param s3_file: S3 object key/path
    :param delete_local: Whether to delete local file after successful upload
    :return: True if successful, False otherwise
    """
    # First upload the file
    upload_success = upload_file_to_s3(local_file, bucket_name, s3_file)
    
    # If upload was successful and delete_local is True, delete the local file
    if upload_success and delete_local:
        try:
            os.remove(local_file)
            print(f"Local file deleted: {local_file}")
        except Exception as e:
            print(f"Warning: Could not delete local file {local_file}: {e}")
            # Don't return False here since upload was successful
    
    return upload_success



import os

## save main.py into s3 bucket
if __name__ == "__main__":
    # Example usage
    local_file = "/Users/henriqueromano/treinamento01/extraction_results_20251012_155748.csv"
    s3_file = "data/extraction_results.csv"
    bucket_name = os.getenv('AWS_BUCKET_NAME')
    upload_file_to_s3(local_file, bucket_name, s3_file)