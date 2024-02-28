import json
import pandas as pd 
import boto3
from datetime import datetime
from io import StringIO

def put_file_s3(bucket, formatted_datetime, my_data):
    s3_source = boto3.resource('s3')
    s3_source.Object(bucket, f'transform_{formatted_datetime}.csv').put(Body=my_data)

def list_lastKey(s3, response, source_bucket):
    response = s3.list_objects_v2(Bucket=source_bucket)
    
    # print(response)
    obj = []
    for item in response['Contents']:
        obj.append(item['Key'])
        
    print(obj[-1])
    lastKey = obj[-1]
    
    return lastKey

def get_csv_content(s3, source_bucket, lastKey):
    csv_object = s3.get_object(Bucket=source_bucket, Key=lastKey)
    csv_content = csv_object['Body'].read().decode('utf-8')
    
    return csv_content

def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')
    
    source_bucket = 'tanawin-source-etl'
    dst_bucket = 'tanawin-dst-etl'
    
    response = s3.list_objects_v2(Bucket=source_bucket)
    
    # # print(response)
    # obj = []
    # for item in response['Contents']:
    #     obj.append(item['Key'])
        
    # print(obj[-1])
    # lastKey = obj[-1]
    
    lastKey = list_lastKey(s3, response, source_bucket)
    
    # csv_object = s3.get_object(Bucket=source_bucket, Key=lastKey)
    # csv_content = csv_object['Body'].read().decode('utf-8')
    
    csv_content = get_csv_content(s3, source_bucket, lastKey)
    
    # print(csv_content)
    
    df = pd.read_csv(StringIO(csv_content))
    # print(df)
    df[['city', 'year']] = df['3'].str.split(', ', expand=True)
    
    df.columns = df.columns.str.replace("city", "3")
    df.columns = df.columns.str.replace("Marathon", "City")
    df.columns = df.columns.str.replace("year", "4")
    df.loc[0, '3'] = 'City'
    df.loc[0, '4'] = 'Year'
    
    # print(df)
    
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d-%H-%M")
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    my_data = csv_buffer.getvalue()
    
    put_file_s3(dst_bucket, formatted_datetime, my_data)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
