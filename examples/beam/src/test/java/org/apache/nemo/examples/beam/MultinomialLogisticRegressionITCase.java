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
package org.apache.nemo.examples.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.test.ArgBuilder;
import org.apache.nemo.common.test.ExampleTestArgs;
import org.apache.nemo.compiler.optimizer.policy.DefaultPolicy;
import org.apache.nemo.examples.beam.policy.DefaultPolicyParallelismFive;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Testing Multinomial Logistic Regressions with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class MultinomialLogisticRegressionITCase {
  private static ArgBuilder builder = new ArgBuilder();
  private static final String executorResourceFileName = ExampleTestArgs.getFileBasePath() + "executors/beam_test_executor_resources.json";

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder();
  }

  @Test (timeout = ExampleTestArgs.TIMEOUT)
  public void test() throws Exception {
    final String input = ExampleTestArgs.getFileBasePath() + "inputs/test_input_mlr";
    final String numFeatures = "100";
    final String numClasses = "5";
    final String numIteration = "3";

    JobLauncher.main(builder
        .addJobId(MultinomialLogisticRegressionITCase.class.getSimpleName())
        .addUserMain(MultinomialLogisticRegression.class.getCanonicalName())
        .addUserArgs(input, numFeatures, numClasses, numIteration)
        .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
        .addResourceJson(executorResourceFileName)
        .build());
  }
}
