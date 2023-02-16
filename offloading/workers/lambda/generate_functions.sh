#!/bin/bash

rm serverless.yml
cp serverless1.yml serverless.yml

MEM=`cat mem_size`

echo "  memorySize: $MEM" >> serverless.yml
echo "package:" >> serverless.yml
echo "   artifact: build/distributions/lambda.zip" >> serverless.yml
echo "functions:" >> serverless.yml

for((i=1;i<=$1;i++))
do
  echo "  lambda-executor${i}:" >> serverless.yml
  echo "    handler: org.apache.nemo.runtime.lambdaexecutor.LambdaWorker" >> serverless.yml
done
