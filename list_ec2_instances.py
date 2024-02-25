import boto3

aws_management_console = boto3.Session(profile_name='default')

ec2_console = aws_management_console.client(service_name='ec2')

result = ec2_console.describe_instances()['Reservations']

# pprint(result)

for each_instance in result:
    # print(each_instance['InstanceId'])
    for val in each_instance['Instances']:
        print(val['InstanceId'])
        print(val['Placement'])