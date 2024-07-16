import json
import boto3
import csv
import io
from datetime import datetime

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
glue_client = boto3.client('glue')
table = dynamodb.Table('S3_Audit_1')  # Replace with your actual DynamoDB table name

def lambda_handler(event, context):
    bucket_name = 'your-source-bucket'  # Replace with your actual S3 bucket name
    source_prefix = 'source/'  # Replace with your actual source prefix
    processed_prefix = 'processed/'  # Replace with your actual processed prefix
    
    # Get the list of files in the source folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)
    if 'Contents' not in response:
        return {'statusCode': 200, 'body': json.dumps('No files to process')}

    for obj in response['Contents']:
        file_key = obj['Key']
        
        # Skip if the file is not directly under the source prefix
        if not file_key.startswith(source_prefix) or file_key == source_prefix:
            continue
        
        # Log processing start
        file_id = file_key.split('/')[-1]
        table.put_item(
            Item={
                'FileID': file_id,
                'FileName': file_key,
                'UploadTimestamp': obj['LastModified'].isoformat(),
                'ProcessingStartTimestamp': str(datetime.utcnow()),
                'Status': 'processing'
            }
        )

        # Get the file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')

        # Process the file (e.g., CSV)
        reader = csv.DictReader(io.StringIO(content))
        fieldnames = reader.fieldnames
        processed_data = []
        
        for row in reader:
            # Data Integrity Checks
            if not row['CustomerID']:
                row['CustomerID'] = 'N/A'  # Handle missing CustomerID
            if not is_valid_date(row['TransactionDate']):
                row['TransactionDate'] = str(datetime.utcnow().date())  # Handle invalid date
            
            # No check for negative TransactionAmount

            processed_data.append(row)
        
        if not processed_data:
            print("No valid data found after processing")
            return {'statusCode': 200, 'body': json.dumps('No valid data found after processing')}
        
        # Upload processed data to S3 with headers
        processed_file_key = f"{processed_prefix}{file_id}"
        with io.StringIO() as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed_data)
            processed_content = output.getvalue()
        
        s3_client.put_object(Bucket=bucket_name, Key=processed_file_key, Body=processed_content)

        # Trigger AWS Glue job
        glue_response = glue_client.start_job_run(
            JobName='your-glue-job-name',  # Replace with your actual Glue job name
            Arguments={
                '--source_bucket': bucket_name,
                '--source_key': processed_file_key
            }
        )

        # Log processing end
        table.update_item(
            Key={'FileID': file_id},
            UpdateExpression='SET ProcessingEndTimestamp = :val1, #st = :val2, GlueJobID = :val3',
            ExpressionAttributeValues={
                ':val1': str(datetime.utcnow()),
                ':val2': 'completed',
                ':val3': glue_response['JobRunId']
            },
            ExpressionAttributeNames={
                '#st': 'Status'
            }
        )

    return {'statusCode': 200, 'body': json.dumps('Batch processing and Glue job initiation completed')}

def is_valid_date(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False
