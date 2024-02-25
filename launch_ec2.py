import boto3

aws_management_console = boto3.Session(profile_name='default')

ec2_console = aws_management_console.client(service_name='ec2')

response =ec2_console.run_instances(
    ImageId='ami-0440d3b780d96b29d', # Images ID
    InstanceType='t2.micro',
    MaxCount=1,
    MinCount=1,
)

print('Done!')