import boto3
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.ERROR)
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

# Retrieve parameters safely
DATABASE = get_ssm_parameter('raiderio_database')
TEMP_TABLE = get_ssm_parameter('temp_table')
FIREHOSE_BUCKET = get_ssm_parameter('raiderio_firehose_bucket')
FIREHOSE_TABLE = get_ssm_parameter('raiderio_firehose_table')
QUERY_OUTPUT_BUCKET = get_ssm_parameter('raiderio_query_results_bucket')


# delete all objects in the bucket
s3_client = boto3.client('s3')

while True:
    objects = s3_client.list_objects(Bucket=FIREHOSE_BUCKET)
    content = objects.get('Contents', [])
    if len(content) == 0:
        break
    for obj in content:
        s3_client.delete_object(Bucket=FIREHOSE_BUCKET, Key=obj['Key'])


# drop the table too
client = boto3.client('athena')

queryStart = client.start_query_execution(
    QueryString = f"""
    DROP TABLE IF EXISTS {DATABASE}.{TEMP_TABLE};
    """,
    QueryExecutionContext = {
        'Database': f'{DATABASE}'
    }, 
    ResultConfiguration = { 'OutputLocation': f's3://{QUERY_OUTPUT_BUCKET}'}
)
