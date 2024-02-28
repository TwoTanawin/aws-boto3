import pandas as pd
import boto3
from io import StringIO
from datetime import datetime

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'tanawin-bucket-lab6'
    destination_bucket = 'tanawin-load-bucket'

    # List all objects in the bucket
    response = s3.list_objects_v2(Bucket=bucket_name)

    # Get the CSV file names
    csv_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]

    if csv_files:
        # Sort the CSV files by modification time
        latest_csv_file = sorted(csv_files, key=lambda x: s3.get_object(Bucket=bucket_name, Key=x)['LastModified'])[-1]

        # Read the content of the last CSV file into a DataFrame
        csv_object = s3.get_object(Bucket=bucket_name, Key=latest_csv_file)
        csv_content = csv_object['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))

        # Drop the "Marathon" column if it exists
        # if "Marathon" in df.columns:
        # df.drop(columns=["Marathon"], inplace=True)
        # df.drop('3', axis=1, inplace=True)
        df[['city', 'year']] = df['3'].str.split(', ', expand=True)

        df.drop(columns=['3'], inplace=True)
        
        df.columns = df.columns.str.replace("city", "3")
        df.columns = df.columns.str.replace("Marathon", "City")
        df.columns = df.columns.str.replace("year", "4")
        df.loc[0, '3'] = 'City'
        df.loc[0, '4'] = 'Year'

        # Process the DataFrame (e.g., perform analysis, manipulation, etc.)
        print(df)  # Print the first few rows of the DataFrame for verification
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d-%H-%M")

        # Save the DataFrame to a CSV file
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=destination_bucket, Key=f'processed_{formatted_datetime}.csv', Body=csv_buffer.getvalue())

        return {
            'statusCode': 200,
            'body': 'Last file processed and uploaded successfully'
        }
        
    else:
        print("No CSV files found in the bucket")

    return {
            'statusCode': 404,
            'body': 'No files found in the source bucket'
        }
