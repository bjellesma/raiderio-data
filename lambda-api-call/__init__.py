import requests
import boto3

ssm_client = boto3.client('ssm')

FACTION='all'
FIREHOSE_BUCKET='raiderio-source-firehose-bucket-new-bill-jellesma'
PERCENTILES = ['p999', 'p990', 'p900', 'p750', 'p600']
FIREHOSE_NAME = ssm_client.get_parameter(Name='raiderio_firehose_name', WithDecryption=True)['Parameter']['Value']
EXPANSIONS = [
    {
        'title' : 'Dragonflight',
        'slug' : 'df',
        'seasons': [
            {
                'expansion_season': 1,
                'full_season': 5
            },
            {
                'expansion_season': 2,
                'full_season': 6
            },
            {
                'expansion_season': 3,
                'full_season': 7
            },
            {
                'expansion_season': 4,
                'full_season': 8
            }
        ]
    },
    {
        'title' : 'Shadowlands',
        'slug' : 'sl',
        'seasons': [
            {
                'expansion_season': 1,
                'full_season': 1
            },
            {
                'expansion_season': 2,
                'full_season': 2
            },
            {
                'expansion_season': 3,
                'full_season': 3
            },
            {
                'expansion_season': 4,
                'full_season': 4
            }
        ]
    },
]
def lambda_handler(event,context):
    # delete all objects in the bucket
    s3_client = boto3.client('s3')
    while True:
        objects = s3_client.list_objects(Bucket=FIREHOSE_BUCKET)
        content = objects.get('Contents', [])
        if len(content) == 0:
            break
        for obj in content:
            s3_client.delete_object(Bucket=FIREHOSE_BUCKET, Key=obj['Key'])
    # connect to firehose and put records from raiderio
    fh = boto3.client('firehose')
    cutoff_table = ''
    for expansion in EXPANSIONS:
        for season in expansion['seasons']:
            cutoff_row = {}
            cutoff_row['expansion'] = expansion['title']
            cutoff_row['season'] = season['expansion_season']
            cutoff_row['full_season'] = season['full_season']
            url = f"https://raider.io/api/v1/mythic-plus/season-cutoffs?season=season-{expansion['slug']}-{season['expansion_season']}&region=us"
            response = requests.get(url)
            # if we can't successfully get the data
            if response.status_code != 200:
                for percentile in PERCENTILES:
                    cutoff_row[percentile] = 0
            else:
                data = response.json()
                cutoff = data.get('cutoffs')
                for percentile in PERCENTILES:
                    cutoff_row[percentile] = cutoff[percentile][FACTION]["quantileMinValue"]
            cutoff_table += str(cutoff_row) + '\n'

    # put the record onto the firehose
    reply = fh.put_record(
            DeliveryStreamName=FIREHOSE_NAME,
            Record = {
                    'Data': cutoff_table
                    }
        )
    print(f'reply: {reply}')
    response = 'All was successful! Logs are located in CloudWatch.'
    
    return {
        'statusCode': reply['ResponseMetadata']['HTTPStatusCode'],
        'body': response
    }
