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
import org.apache.nemo.common.test.ExampleTestUtil;
import org.apache.nemo.compiler.optimizer.policy.DynamicTaskSizingPolicy;
import org.apache.nemo.compiler.optimizer.policy.TransientResourcePolicy;
import org.apache.nemo.examples.beam.policy.DefaultPolicyParallelismFive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test Alternating Least Square program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class AlternatingLeastSquareITCase {
  private static ArgBuilder builder;
  private static final String input = ExampleTestArgs.getFileBasePath() + "inputs/test_input_als";
  private static final String outputFileName = "test_output_als";
  private static final String output = ExampleTestArgs.getFileBasePath() + outputFileName;
  private static final String expectedOutputFileName = "outputs/expected_output_als";
  private static final String noPoisonResources = ExampleTestArgs.getFileBasePath() + "executors/beam_test_executor_resources.json";
  private static final String poisonedResource = ExampleTestArgs.getFileBasePath() + "executors/beam_test_poisoned_executor_resources.json";
  private static final String numFeatures = "10";
  private static final String numIteration = "3";
  private static final String lambda = "0.05";

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
      .addUserMain(AlternatingLeastSquare.class.getCanonicalName())
      .addUserArgs(input, numFeatures, numIteration, lambda, output);
  }

  @After
  public void tearDown() throws Exception {
    try {
      ExampleTestUtil.ensureALSOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
    } finally {
      ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    }
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testDefault() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(noPoisonResources)
      .addJobId(AlternatingLeastSquareITCase.class.getSimpleName() + "_default")
      .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
      .build());
  }

  @Test(timeout = 2400000)
  public void testDTS() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(noPoisonResources)
      .addJobId(AlternatingLeastSquareITCase.class.getSimpleName() + "_dts")
      .addOptimizationPolicy(DynamicTaskSizingPolicy.class.getCanonicalName())
      .build());
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testTransient() throws Exception {
    JobLauncher.main(builder
      .addResourceJson(noPoisonResources)
      .addJobId(AlternatingLeastSquareITCase.class.getSimpleName() + "_transient")
      .addOptimizationPolicy(TransientResourcePolicy.class.getCanonicalName())
      .build());
  }

  // TODO #137: Retry parent task(s) upon task INPUT_READ_FAILURE
  // @Test (timeout = TIMEOUT)
  // public void testTransientResourceWithPoison() throws Exception {
  //   JobLauncher.main(builder
  //       .addResourceJson(poisonedResource)
  //       .addJobId(AlternatingLeastSquareITCase.class.getSimpleName() + "_transient_poisoned")
  //       .addMaxTaskAttempt(Integer.MAX_VALUE)
  //       .addOptimizationPolicy(TransientResourcePolicyParallelismTen.class.getCanonicalName())
  //       .build());
  // }

  // TODO #453: Add test methods related to Dynamic Task Sizing in Nemo.
}
