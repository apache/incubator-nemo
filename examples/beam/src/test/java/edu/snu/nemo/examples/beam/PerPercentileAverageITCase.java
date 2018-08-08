/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.examples.beam;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.test.ArgBuilder;
import edu.snu.nemo.common.test.ExampleTestUtil;
import edu.snu.nemo.examples.beam.policy.DefaultPolicyParallelismFive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test PerPercentile Average program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class PerPercentileAverageITCase {
  private static final int TIMEOUT = 120000;
  private static ArgBuilder builder;
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";

  private static final String inputFileName = "test_input_partition";
  private static final String outputFileName = "test_output_partition";
  private static final String expectedOutputFileName = "expected_output_partition";
  private static final String executorResourceFileName = fileBasePath + "beam_test_executor_resources.json";
  private static final String inputFilePath =  fileBasePath + inputFileName;
  private static final String outputFilePath =  fileBasePath + outputFileName;

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
      .addResourceJson(executorResourceFileName)
      .addUserMain(PerPercentileAverage.class.getCanonicalName())
      .addUserArgs(inputFilePath, outputFilePath);
  }

  @After
  public void tearDown() throws Exception {
    try {
      for (int i = 0; i < 10; i++) {
        ExampleTestUtil.ensureOutputValidity(fileBasePath,
            outputFileName + "_" + i,
            expectedOutputFileName + "_" + i);
      }
    } finally {
      ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName);
    }
  }

  @Test (timeout = TIMEOUT)
  public void test() throws Exception {
    JobLauncher.main(builder
      .addJobId(PerPercentileAverage.class.getSimpleName())
      .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
      .build());
  }
}
