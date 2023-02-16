import boto3
import os, time
import zipfile
from botocore.config import Config
import sys


my_config = Config(
    region_name = 'ap-northeast-1',
)

client = boto3.client('lambda', config=my_config)
BUCKET_NAME="nemo-lambda"
BUCKET_KEY="lambda.zip"

def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file),
                    os.path.relpath(os.path.join("/".join(root.split("/")[1:]), file),
                                       os.path.join(path, '..')))


zipf = zipfile.ZipFile('lambda.zip', 'w', zipfile.ZIP_DEFLATED)
zipdir('lambda_zip/', zipf)
zipf.close()

# create zip file done
print("Created zip file ..., update it to S3")

# upload to S3 
s3client = boto3.client('s3')
response = s3client.delete_object(
    Bucket=BUCKET_NAME,
    Key=BUCKET_KEY
)

s3 = boto3.resource('s3')
s3.meta.client.upload_file('lambda.zip', BUCKET_NAME, BUCKET_KEY)
print("Zip file uploaded to S3")


num_lambda = int(sys.argv[1])
name="lambda-dev-11-lambda-executor"

for i in range(1,num_lambda + 1):
    print("Creating ", name + str(i))
    # client.delete_function(FunctionName=name + str(i))
    response = client.create_function(
            FunctionName=name + str(i),
            Runtime="java11",
            Role="arn:aws:iam::835596193924:role/nemo-dev-ap-northeast-1-lambdaRole",
            SnapStart={
                'ApplyOn': 'PublishedVersions'
            },
            Handler="org.apache.nemo.runtime.lambdaexecutor.LambdaWorker",
            Code={
                "S3Bucket": BUCKET_NAME,
                "S3Key": BUCKET_KEY
                },
            Timeout=600,
            MemorySize=1500,
            Publish=True,
            VpcConfig={
                'SubnetIds': [
                    'subnet-5bcae603',
                    ],
                'SecurityGroupIds': [
                    "sg-388f4641",
                    ]
                },
            Layers=["arn:aws:lambda:ap-northeast-1:835596193924:layer:sponge-layer:1"],
            )
    print(response)
    time.sleep(6)
