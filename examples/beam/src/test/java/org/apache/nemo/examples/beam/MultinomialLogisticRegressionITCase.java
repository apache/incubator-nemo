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
import org.apache.nemo.compiler.optimizer.policy.DynamicTaskSizingPolicy;
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
  private static final String input = ExampleTestArgs.getFileBasePath() + "inputs/test_input_mlr";
  private static final String numFeatures = "100";
  private static final String numClasses = "5";
  private static final String numIteration = "3";

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
      .addUserMain(MultinomialLogisticRegression.class.getCanonicalName())
      .addUserArgs(input, numFeatures, numClasses, numIteration);
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT, expected = Test.None.class)
  public void testDefault() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(MultinomialLogisticRegressionITCase.class.getSimpleName() + "_default")
      .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
      .build());
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT, expected = Test.None.class)
  public void testDTS() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(MultinomialLogisticRegressionITCase.class.getSimpleName() + "_dts")
      .addOptimizationPolicy(DynamicTaskSizingPolicy.class.getCanonicalName())
      .build());
  }

  // TODO #453: Add test methods related to Dynamic Task Sizing in Nemo.
}
