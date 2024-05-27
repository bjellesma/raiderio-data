# boto3 is still needed to interact with the parameter store
import boto3
# use awswrangler over boto3 to more efficiently write a csv to s3
import awswrangler as wr
import datetime
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
ssm_client = boto3.client('ssm')

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
DATA_QUALITY_BUCKET = get_ssm_parameter('raiderio_data_quality_bucket')

current_date = datetime.date.today()

# Define the query to identify zeros in the percentile columns
RANGE_CHECK = f"""
SELECT 
    *,
     CASE WHEN p999 < 0 OR p999 > 5000 THEN 'Out of range' ELSE 'OK' END AS p999_check,
    CASE WHEN p990 < 0 OR p990 > 5000 THEN 'Out of range' ELSE 'OK' END AS p990_check,
    CASE WHEN p900 < 0 OR p900 > 5000 THEN 'Out of range' ELSE 'OK' END AS p900_check,
    CASE WHEN p750 < 0 OR p750 > 5000 THEN 'Out of range' ELSE 'OK' END AS p750_check,
    CASE WHEN p600 < 0 OR p600 > 5000 THEN 'Out of range' ELSE 'OK' END AS p600_check
FROM 
    {DATABASE}.{TEMP_TABLE}
"""
try:
    # Run the quality check query
    df = wr.athena.read_sql_query(sql=RANGE_CHECK, database=DATABASE)

    # Filter DataFrame to include only rows with 'Zero found'
    zero_rows = df[
        (df['p999_check'] == 'Out of range') |
        (df['p990_check'] == 'Out of range') |
        (df['p900_check'] == 'Out of range') |
        (df['p750_check'] == 'Out of range') |
        (df['p600_check'] == 'Out of range')
    ]

    # Check if any rows have zeros
    if not zero_rows.empty:
        # Write the problematic rows to an S3 bucket
        output_path = F's3://{DATA_QUALITY_BUCKET}/dq_check_{current_date}/range_check.csv'
        wr.s3.to_csv(df=zero_rows, path=output_path, index=False)
        logger.error(f'Zero value check failed.')
        sys.exit(1)
    else:
        logger.info('Zero value check passed. No issues found.')
except Exception as e:
    logger.error(f"SQL query execution failed: {str(e)}")
    # Parse the exception message to react differently based on error types
    if "syntax error" in str(e).lower():
        logger.error("There was a syntax error in your SQL query.")
    elif "ResourceNotFoundException" in str(e):
        logger.error("The specified table or database does not exist.")
    sys.exit(1)