import boto3
from pprint import pprint

aws_management_console = boto3.Session(profile_name="default")

iam_console = aws_management_console.client(service_name="iam")

result = iam_console.list_users()

for each_user in result['Users']:
    pprint(each_user['UserName'])
