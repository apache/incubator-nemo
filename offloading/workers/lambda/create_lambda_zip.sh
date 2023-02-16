#!/bin/bash

rm -rf lambda_zip
rm lambda.zip

mkdir lambda_zip
mkdir lambda_zip/lib

cp build/resources/main/log4j2.properties lambda_zip/
cp jars/* lambda_zip/lib/

cp aws-lambda-java-log4j-1.0.0.jar lambda_zip/lib/
cp ~/.m2/repository/com/amazonaws/aws-lambda-java-log4j2/1.0.0/aws-lambda-java-log4j2-1.0.0.jar lambda_zip/lib/
cp ~/.m2/repository/org/apache/logging/log4j/log4j-core/2.8.2/log4j-core-2.8.2.jar lambda_zip/lib/
cp ~/.m2/repository/com/amazonaws/aws-lambda-java-core/1.1.0/aws-lambda-java-core-1.1.0.jar lambda_zip/lib/
cp ~/.m2/repository/org/apache/logging/log4j/log4j-api/2.8.2/log4j-api-2.8.2.jar lambda_zip/lib/
cp ~/.m2/repository/log4j/log4j/1.2.17/log4j-1.2.17.jar lambda_zip/lib/

