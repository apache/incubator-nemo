import boto3
import os
import zipfile
from botocore.config import Config
import sys


my_config = Config(
    region_name = 'ap-northeast-1',
)


client = boto3.client('lambda', config=my_config)
BUCKET_NAME="nemo-lambda"


num_lambda = int(sys.argv[1])
name="nemo-dev-lambda-executor"

for i in range(1,num_lambda + 1):
    print("Creating ", name + str(i))
    client.delete_function(FunctionName=name + str(i))
