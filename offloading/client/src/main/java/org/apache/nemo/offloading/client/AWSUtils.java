/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.offloading.client;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AWSUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AWSUtils.class.getName());

  //public static final AmazonS3 AWS_S3;
  public static final AWSLambda AWS_LAMBDA;
  public static final String S3_BUCKET_NAME = "nemo-serverless";
  public static final String SIDEINPUT_LAMBDA_NAME = "nemo-dev-hello";
  public static final String SIDEINPUT_LAMBDA_NAME2 = "nemo-dev-tg-erverless-worker";

  static {
    //AWS_S3 = AmazonS3ClientBuilder.standard().build();
    AWS_LAMBDA = AWSLambdaClientBuilder.standard().build();
  }
}
