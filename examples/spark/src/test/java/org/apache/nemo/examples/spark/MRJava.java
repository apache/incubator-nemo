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
 * Test MR Spark programs with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
@PowerMockIgnore("javax.management.*")
public final class MRJava {
  private static ArgBuilder builder;
  private static final String executorResourceFileName = ExampleTestArgs.getFileBasePath() + "/executors/spark_test_executor_resources.json";

  @Before
  public void setUp() {
    builder = new ArgBuilder()
        .addResourceJson(executorResourceFileName);
  }

  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testSparkWordCount() throws Exception {
    final String inputFileName = "/inputs/test_input_wordcount_spark";
    final String outputFileName = "test_output_wordcount_spark";
    final String expectedOutputFilename = "/outputs/expected_output_wordcount_spark";
    final String inputFilePath = ExampleTestArgs.getFileBasePath() + inputFileName;
    final String outputFilePath = ExampleTestArgs.getFileBasePath() + outputFileName;

    JobLauncher.main(builder
        .addJobId(JavaWordCount.class.getSimpleName() + "_test")
        .addUserMain(JavaWordCount.class.getCanonicalName())
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
  public void testSparkWordAndLineCount() throws Exception {
    final String inputFileName = "/inputs/test_input_wordcount_spark";
    final String outputFileName = "test_output_word_and_line_count";
    final String expectedOutputFilename = "/outputs/expected_output_word_and_line_count";
    final String inputFilePath = ExampleTestArgs.getFileBasePath() + inputFileName;
    final String outputFilePath = ExampleTestArgs.getFileBasePath() + outputFileName;

    JobLauncher.main(builder
        .addJobId(JavaWordAndLineCount.class.getSimpleName() + "_test")
        .addUserMain(JavaWordAndLineCount.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath)
        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
        .build());

    try {
      ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFilename);
    } finally {
      ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    }
  }
}
