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
package org.apache.nemo.runtime.executor.datatransfer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class LambdaWarmer {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaWarmer.class.getName());

  public static final AtomicInteger TICKET = new AtomicInteger(0);

  // TODO: remove
  private final AWSLambda awsLambda;

  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final int numOfInvocation = 30;


  /**
   * Constructor of the output collector.
   */
  public LambdaWarmer() {
    this.awsLambda = AWSLambdaClientBuilder.standard().
      withClientConfiguration(new ClientConfiguration().withMaxConnections(150)).build();
  }

  public void warmup() {
    warmup("{}");
  }

  public void warmup(String payload) {
    LOG.info("Warmup");
    for (int i = 0; i < numOfInvocation; i++) {
      // Trigger lambdas
      executorService.submit(() -> {
        final InvokeRequest request = new InvokeRequest()
          .withFunctionName(AWSUtils.SIDEINPUT_LAMBDA_NAME2)
          .withPayload(payload);
        awsLambda.invoke(request);
      });
    }
  }

  public void start() {
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      warmup();
    }, 1, 60 * 2, TimeUnit.SECONDS);
  }
}
