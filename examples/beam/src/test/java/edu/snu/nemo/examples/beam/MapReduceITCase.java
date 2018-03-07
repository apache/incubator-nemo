/*
 * Copyright (C) 2017 Seoul National University
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
import edu.snu.nemo.examples.beam.policy.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Optional;

/**
 * Test MapReduce program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class MapReduceITCase {
  private static final int TIMEOUT = 120000;
  private static ArgBuilder builder = new ArgBuilder();
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";

  private static final String inputFileName = "sample_input_mr";
  private static final String outputFileName = "sample_output_mr";
  private static final String testResourceFileName = "test_output_mr";
  private static final String inputFilePath =  fileBasePath + inputFileName;
  private static final String outputFilePath =  fileBasePath + outputFileName;


  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder();
  }

  @After
  public void tearDown() throws Exception {
    final Optional<String> errorMsg =
        ExampleTestUtil.ensureOutputValidity(fileBasePath, outputFileName, testResourceFileName);
    ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName);
    if (errorMsg.isPresent()) {
      throw new RuntimeException(errorMsg.get());
    }
  }

  @Test (timeout = TIMEOUT)
  public void test() throws Exception {
    JobLauncher.main(builder
        .addJobId(MapReduceITCase.class.getSimpleName())
        .addUserMain(MapReduce.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath)
        .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testSailfish() throws Exception {
    JobLauncher.main(builder
        .addJobId(MapReduceITCase.class.getSimpleName() + "_sailfish")
        .addUserMain(MapReduce.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath)
        .addOptimizationPolicy(SailfishPolicyParallelsimFive.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testDisagg() throws Exception {
    JobLauncher.main(builder
        .addJobId(MapReduceITCase.class.getSimpleName() + "_disagg")
        .addUserMain(MapReduce.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath)
        .addOptimizationPolicy(DisaggregationPolicyParallelsimFive.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testPado() throws Exception {
    JobLauncher.main(builder
        .addJobId(MapReduceITCase.class.getSimpleName() + "_pado")
        .addUserMain(MapReduce.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath)
        .addOptimizationPolicy(PadoPolicyParallelsimFive.class.getCanonicalName())
        .build());
  }

  /**
   * Testing data skew dynamic optimization.
   * @throws Exception exception on the way.
   */
  @Test (timeout = TIMEOUT)
  public void testDataSkew() throws Exception {
    JobLauncher.main(builder
        .addJobId(MapReduceITCase.class.getSimpleName() + "_dataskew")
        .addUserMain(MapReduce.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath)
        .addOptimizationPolicy(DataSkewPolicyParallelsimFive.class.getCanonicalName())
        .build());
  }
}
