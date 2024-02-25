import boto3

aws_management_console = boto3.Session(profile_name="default")

ec2_console = aws_management_console.client(service_name='ec2')

# response = ec2_console.stop_instances( # stop
#     InstanceIds=[
#         'i-0e5738011582828a6',
#     ],   
# )

# response = ec2_console.start_instances( # start
#     InstanceIds=[
#         'i-0e5738011582828a6',
#     ],   
# )

response = ec2_console.terminate_instances( # terminate
    InstanceIds=[
        'i-0e5738011582828a6',
    ],   
)

print('Done!')