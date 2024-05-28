import boto3
from datetime import datetime
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
ssm_client = boto3.client('ssm')
client = boto3.client('athena')

def get_ssm_parameter(param_name):
    try:
        # Attempt to retrieve the parameter
        response = ssm_client.get_parameter(Name=param_name, WithDecryption=True)
        return response['Parameter']['Value']
    except ssm_client.exceptions.ParameterNotFound as e:
        # Log the error and the parameter name if not found
        logger.error(f"Parameter retrieval failed for {param_name}: {str(e)}")
        sys.exit(f"Parameter retrieval failed for {param_name}: {str(e)}")
    except Exception as e:
        # Handle other possible exceptions
        logger.error(f"An error occurred while retrieving {param_name}: {str(e)}")
        sys.exit(f"An error occurred while retrieving {param_name}: {str(e)}")


DATABASE = get_ssm_parameter('raiderio_database')
TEMP_TABLE = get_ssm_parameter('raiderio_temp_table')
PARTITION_COLUMN = get_ssm_parameter('raiderio_partition_column')
PROD_TABLE = get_ssm_parameter('raiderio_prod_table')
TEMP_BUCKET = get_ssm_parameter('raiderio_temp_bucket')
QUERY_OUTPUT_BUCKET = get_ssm_parameter('raiderio_query_results_bucket')
PROD_BUCKET=get_ssm_parameter('raiderio_prod_bucket')

current_date = str(str(datetime.utcnow()).replace('-', '_').replace(' ', '_').replace(':', '_').replace('.', '_'))

# Refresh the table
query_string = f"""
    CREATE TABLE {PROD_TABLE} WITH
    (external_location='s3://{PROD_BUCKET}/{current_date}/',
    format='PARQUET',
    write_compression='SNAPPY',
    partitioned_by = ARRAY['{PARTITION_COLUMN}'])
    AS

    SELECT
        *
    FROM "{DATABASE}"."{TEMP_TABLE}"

    ;
    """
try:
    response = client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={'Database': DATABASE}, 
        ResultConfiguration={'OutputLocation': f's3://{QUERY_OUTPUT_BUCKET}'}
    )

    # Get the query execution ID
    query_execution_id = response['QueryExecutionId']

    # Check the status of the query execution
    query_status = client.get_query_execution(QueryExecutionId=query_execution_id)

    # Wait until query execution is finished
    while query_status['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)

    if query_status['QueryExecution']['Status']['State'] == 'FAILED':
        raise Exception(query_status['QueryExecution']['Status']['StateChangeReason'])

except Exception as e:
    logger.error(f"Failed to execute job: {str(e)}")
    sys.exit(1)  # Exit with error code 1 to indicate failure to AWS Glue


