#!/bin/bash

rm serverless.yml
cp serverless1.yml serverless.yml

for((i=1;i<=$1;i++))
do
  echo "  lambda-executor${i}:" >> serverless.yml
  echo "    handler: org.apache.nemo.runtime.lambdaexecutor.LambdaWorker" >> serverless.yml
done
