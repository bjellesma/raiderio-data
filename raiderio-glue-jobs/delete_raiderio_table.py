import boto3
import sys
import logging

# Set up logging and AWS resources
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
ssm_client = boto3.client('ssm')
athena_client = boto3.client('athena')
s3_client = boto3.client('s3')

def get_ssm_parameter(param_name):
    """
    Retrieve a parameter from AWS Systems Manager Parameter Store.

    Parameters:
    param_name (str): The name of the parameter to retrieve.

    Returns:
    str: The value of the parameter.

    Raises:
    SystemExit: If the parameter cannot be retrieved due to it not existing or other AWS errors.
    """
    try:
        response = ssm_client.get_parameter(Name=param_name, WithDecryption=True)
        return response['Parameter']['Value']
    except ssm_client.exceptions.ParameterNotFound as e:
        logger.error(f"Parameter retrieval failed for {param_name}: {str(e)}")
        sys.exit(f"Parameter retrieval failed for {param_name}: {str(e)}")
    except Exception as e:
        logger.error(f"An error occurred while retrieving {param_name}: {str(e)}")
        sys.exit(f"An error occurred while retrieving {param_name}: {str(e)}")

def execute_query(query, database, output_bucket):
    """
    Execute an SQL query using Amazon Athena.

    Parameters:
    query (str): SQL query string to be executed.
    database (str): Database to execute the query against.
    output_bucket (str): S3 bucket to store query execution results.

    Returns:
    str: The Query Execution ID from Athena.

    Raises:
    SystemExit: If the query fails to execute.
    """
    try:
        response = athena_client.start_query_execution(
            QueryString=query['sql'],
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': f's3://{output_bucket}'}
        )
        query_execution_id = response['QueryExecutionId']
        logger.info(f"Query {query['name']} executed with ID: {query_execution_id}")
        return query_execution_id
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        sys.exit(1)

def empty_s3_bucket(bucket):
    """
    Empty all objects from a specified S3 bucket.

    Parameters:
    bucket (str): Name of the S3 bucket to be emptied.

    Raises:
    SystemExit: If unable to delete objects from the bucket.
    """
    try:
        while True:
            objects = s3_client.list_objects(Bucket=bucket)
            content = objects.get('Contents', [])
            if len(content) == 0:
                break
            for obj in content:
                s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
        logger.info("S3 bucket emptied successfully.")
    except Exception as e:
        logger.error(f"Failed to empty bucket {bucket}: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Retrieve parameters safely
    DATABASE = get_ssm_parameter('raiderio_database')
    TEMP_TABLE = get_ssm_parameter('raiderio_temp_table')
    PROD_TABLE = get_ssm_parameter('raiderio_prod_table')
    TEMP_BUCKET = get_ssm_parameter('raiderio_temp_bucket')
    QUERY_OUTPUT_BUCKET = get_ssm_parameter('raiderio_query_results_bucket')

    temp_table_query = {
        'name': 'Deletion of temporary source table',
        'sql': f'DROP TABLE IF EXISTS {DATABASE}.{TEMP_TABLE};'
    }
    prod_table_query = {
        'name': 'Deletion of production table',
        'sql': f'DROP TABLE IF EXISTS {DATABASE}.{PROD_TABLE};'
    }

    # Execute SQL statements to drop tables
    for query in [temp_table_query, prod_table_query]:
        execute_query(query, DATABASE, QUERY_OUTPUT_BUCKET)

    # Empty the bucket with the temp files and the query results bucket, we'll leave the prod bucket)
    empty_s3_bucket(TEMP_BUCKET)
    empty_s3_bucket(QUERY_OUTPUT_BUCKET)
