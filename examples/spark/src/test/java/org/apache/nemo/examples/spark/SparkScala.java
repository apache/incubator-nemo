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
package org.apache.nemo.examples.spark;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.test.ArgBuilder;
import org.apache.nemo.common.test.ExampleTestArgs;
import org.apache.nemo.common.test.ExampleTestUtil;
import org.apache.nemo.compiler.optimizer.policy.DefaultPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test Spark programs with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
@PowerMockIgnore("javax.management.*")
public final class SparkScala {
  private static ArgBuilder builder;
  private static final String executorResourceFileName = ExampleTestArgs.getFileBasePath() + "executors/spark_test_executor_resources.json";

  @Before
  public void setUp() {
    builder = new ArgBuilder()
        .addResourceJson(executorResourceFileName);
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testPi() throws Exception {
    final String numParallelism = "3";

    JobLauncher.main(builder
        .addJobId(SparkPi.class.getSimpleName() + "_test")
        .addUserMain(SparkPi.class.getCanonicalName())
        .addUserArgs(numParallelism)
        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
        .build());
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testWordCount() throws Exception {
    final String inputFileName = "inputs/test_input_wordcount_spark";
    final String outputFileName = "inputs/test_output_wordcount_spark";
    final String expectedOutputFilename = "/outputs/expected_output_wordcount_spark";
    final String inputFilePath = ExampleTestArgs.getFileBasePath() + inputFileName;
    final String outputFilePath = ExampleTestArgs.getFileBasePath() + outputFileName;

    JobLauncher.main(builder
        .addJobId(SparkWordCount.class.getSimpleName() + "_test")
        .addUserMain(SparkWordCount.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath)
        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
        .build());

    try {
      ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFilename);
    } finally {
      ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    }
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testCachingWordCount() throws Exception {
    final String inputFileName = "inputs/test_input_wordcount_spark";
    final String outputFileName1 = "test_output_wordcount_spark";
    final String outputFileName2 = "test_output_reversed_wordcount_spark";
    final String expectedOutputFilename1 = "outputs/expected_output_wordcount_spark";
    final String expectedOutputFilename2 = "outputs/expected_output_reversed_wordcount_spark";
    final String inputFilePath = ExampleTestArgs.getFileBasePath() + inputFileName;
    final String outputFilePath1 = ExampleTestArgs.getFileBasePath() + outputFileName1;
    final String outputFilePath2 = ExampleTestArgs.getFileBasePath() + outputFileName2;

    JobLauncher.main(builder
        .addJobId(SparkCachingWordCount.class.getSimpleName() + "_test")
        .addUserMain(SparkCachingWordCount.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath1, outputFilePath2)
        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
        .build());

    try {
      ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName1, expectedOutputFilename1);
      ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName2, expectedOutputFilename2);
    } finally {
      ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName1);
      ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName2);
    }
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testALS() throws Exception {
    JobLauncher.main(builder
      .addJobId(SparkALS.class.getSimpleName() + "_test")
      .addUserMain(SparkALS.class.getCanonicalName())
      .addUserArgs("100")
      .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
      .build());
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testALSEmptyUserArgs() throws Exception {
    JobLauncher.main(builder
      .addJobId(SparkALS.class.getSimpleName() + "_test")
      .addUserMain(SparkALS.class.getCanonicalName())
      .addUserArgs("")
      .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
      .build());
  }
}
