import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def get_csv(url):
    df = pd.read_html(url)
    
    result_df = df[0]
    csv_buffer = StringIO()
    result_df.to_csv(csv_buffer)
    
    my_data = csv_buffer.getvalue()
    return my_data

def put_file_s3(bucket, formatted_datetime, my_data):
    s3_source = boto3.resource('s3')
    s3_source.Object(bucket, f'source_{formatted_datetime}.csv').put(Body=my_data)
    

def lambda_handler(event, context):
    # TODO 
    url = 'https://www.runnersworld.com/races-places/a20823734/these-are-the-worlds-fastest-marathoners-and-marathon-courses/'
    # df = pd.read_html(url)
    # result_df = df[0]

    # csv_buffer = StringIO()
    
    # result_df.to_csv(csv_buffer)
    
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d-%H-%M")
    
    my_data = get_csv(url)
    
    bucket = 'tanawin-source-etl'
    # s3_source = boto3.resource('s3')
    # s3_source.Object(bucket, f'source_{formatted_datetime}.scv').put(Body=my_data)
    
    put_file_s3(bucket, formatted_datetime, my_data)
    
    # print(my_data)
    print('Done!')
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
