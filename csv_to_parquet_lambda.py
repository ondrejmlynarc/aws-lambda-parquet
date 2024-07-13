import boto3
import awswrangler as wr
from urllib.parse import unquote_plus
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def parse_s3_event(event):
    """
    Get the bucket name and object key from the S3 event.
    """
    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        return bucket, key
    except (KeyError, IndexError) as e:
        logger.error(f"Error parsing S3 event: {e}")
        raise

def extract_db_and_table(key):
    """
    Get database and table names from the S3 object key.
    """
    key_parts = key.split("/")
    if len(key_parts) < 3:
        raise ValueError(f"Invalid key format: {key}")
    return key_parts[-3], key_parts[-2]

def ensure_database_exists(db_name):
    """
    Create the database if it doesn't exist.
    """
    current_databases = wr.catalog.databases()
    if db_name not in current_databases.values:
        logger.info(f"Creating database {db_name}")
        wr.catalog.create_database(db_name)
    else:
        logger.info(f"Database {db_name} already exists")

def lambda_handler(event, context):
    """
    Main function: Convert CSV from S3 to Parquet and update Glue catalog.
    """
    try:
        bucket, key = parse_s3_event(event)
        db_name, table_name = extract_db_and_table(key)

        logger.info(f"Bucket: {bucket}")
        logger.info(f"Key: {key}")
        logger.info(f"DB Name: {db_name}")
        logger.info(f"Table Name: {table_name}")

        input_path = f"s3://{bucket}/{key}"
        output_path = f"s3://dataeng-clean-zone-om77/{db_name}/{table_name}"
        
        logger.info(f"Input Path: {input_path}")
        logger.info(f"Output Path: {output_path}")

        input_df = wr.s3.read_csv([input_path])

        ensure_database_exists(db_name)

        result = wr.s3.to_parquet(
            df=input_df, 
            path=output_path, 
            dataset=True,
            database=db_name,
            table=table_name,
            mode="append"
        )

        logger.info(f"Result: {result}")
        return result

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise