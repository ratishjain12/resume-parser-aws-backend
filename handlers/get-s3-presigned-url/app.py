import json
import boto3
import os

def lambda_handler(event, context):
    # Extract parameters from the event
    bucket_name = os.environ['BUCKET_NAME']
    object_name = event['queryStringParameters']['object_name']

    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Generate the pre-signed URL
    try:
        url = s3_client.generate_presigned_url('put_object',
                                               Params={'Bucket': bucket_name,
                                                       'Key': object_name},
                                               ExpiresIn=3600)  # URL expires in 1 hour
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
            'headers': {
                'Access-Control-Allow-Origin': '*',
            }
        }

    # Return the pre-signed URL
    return {
        'statusCode': 200,
        'body': json.dumps({'url': url}),
        'headers': {
            'Access-Control-Allow-Origin': '*',
        }
    }
