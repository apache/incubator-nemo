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
package edu.snu.onyx.tests.examples.beam;

import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.examples.beam.MapReduce;
import edu.snu.onyx.tests.compiler.CompilerTestUtil;
import edu.snu.onyx.tests.examples.ExampleTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test MapReduce program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class MapReduceITCase {
  private static final int TIMEOUT = 60000;
  private static final String inputFileName = "sample_input_mr";
  private static final String outputFileName = "sample_output_mr";
  private static final String testResourceFileName = "test_output_mr_test";
  private static final String fileBasePath = CompilerTestUtil.rootDir + "/../examples/src/main/resources/";
  private static final String inputFilePath =  fileBasePath + inputFileName;
  private static final String outputFilePath =  fileBasePath + outputFileName;

  public static ArgBuilder builder = new ArgBuilder()
      .addJobId(MapReduceITCase.class.getSimpleName())
      .addUserMain(MapReduce.class.getCanonicalName())
      .addUserArgs(inputFilePath, outputFilePath);

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
        .addUserMain(MapReduce.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath);
  }

  @Test (timeout = TIMEOUT)
  public void test() throws Exception {
    JobLauncher.main(builder
        .addJobId(MapReduceITCase.class.getSimpleName())
        .addOptimizationPolicy(CompilerTestUtil.defaultPolicy)
        .build());

    ExampleTestUtil.ensureOutputValid(fileBasePath, outputFileName, testResourceFileName);
  }

  @Test (timeout = TIMEOUT)
  public void testSailfish() throws Exception {
    JobLauncher.main(builder
        .addJobId(MapReduceITCase.class.getSimpleName() + "_sailfish")
        .addOptimizationPolicy(CompilerTestUtil.sailfishPolicy)
        .build());

    ExampleTestUtil.ensureOutputValid(fileBasePath, outputFileName, testResourceFileName);
  }

  @Test (timeout = TIMEOUT)
  public void testDisagg() throws Exception {
    JobLauncher.main(builder
        .addJobId(MapReduceITCase.class.getSimpleName() + "_disagg")
        .addOptimizationPolicy(CompilerTestUtil.disaggPolicy)
        .build());

    ExampleTestUtil.ensureOutputValid(fileBasePath, outputFileName, testResourceFileName);
  }

  @Test (timeout = TIMEOUT)
  public void testPado() throws Exception {
    JobLauncher.main(builder
        .addJobId(MapReduceITCase.class.getSimpleName() + "_pado")
        .addOptimizationPolicy(CompilerTestUtil.padoPolicy)
        .build());

    ExampleTestUtil.ensureOutputValid(fileBasePath, outputFileName, testResourceFileName);
  }

  /**
   * Testing data skew dynamic optimization.
   * @throws Exception exception on the way.
   */
  @Test (timeout = TIMEOUT)
  public void testDataSkew() throws Exception {
    JobLauncher.main(builder
        .addJobId(MapReduceITCase.class.getSimpleName() + "_dataskew")
        .addOptimizationPolicy(CompilerTestUtil.dataSkewPolicy)
        .build());

    ExampleTestUtil.ensureOutputValid(fileBasePath, outputFileName, testResourceFileName);
  }
}
