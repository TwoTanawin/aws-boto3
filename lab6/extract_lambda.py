import json
import pandas as pd
import boto3
from io import StringIO

def lambda_handler(event, context):
    # TODO implement
    # ser = pd.Series([1,2,3,4])
    url = 'https://www.runnersworld.com/races-places/a20823734/these-are-the-worlds-fastest-marathoners-and-marathon-courses/'
    list_of_df = pd.read_html(url)
    result_df1 = list_of_df[0]
    bucket = 'tanawin-bucket-lab6' #s3 name
    csv_buffer = StringIO()
    result_df1.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, 'df.csv').put(Body=csv_buffer.getvalue())
    
    # print(ser)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
