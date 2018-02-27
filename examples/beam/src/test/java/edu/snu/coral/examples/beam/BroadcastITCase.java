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
package edu.snu.coral.examples.beam;

import edu.snu.coral.client.JobLauncher;
import edu.snu.coral.common.test.ArgBuilder;
import edu.snu.coral.common.test.ExampleTestUtil;
import edu.snu.coral.examples.beam.policy.DefaultPolicyParallelismFive;
import edu.snu.coral.examples.beam.policy.PadoPolicyParallelismFive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test Broadcast program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class BroadcastITCase {
  private static final int TIMEOUT = 180000;
  private static final String inputFileName = "sample_input_mr";
  private static final String outputFileName = "sample_output_broadcast";
  private static final String testResourceFileName = "test_output_broadcast";
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";
  private static final String inputFilePath =  fileBasePath + inputFileName;
  private static final String outputFilePath =  fileBasePath + outputFileName;

  private static ArgBuilder builder = new ArgBuilder()
      .addJobId(BroadcastITCase.class.getSimpleName())
      .addUserMain(Broadcast.class.getCanonicalName())
      .addUserArgs(inputFilePath, outputFilePath);

  @Before
  public void setUp() {
    builder = new ArgBuilder()
        .addUserMain(Broadcast.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath);
  }

  @After
  public void tearDown() throws Exception {
    ExampleTestUtil.ensureOutputValidity(fileBasePath, outputFileName, testResourceFileName);
    ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName);
  }

  @Test (timeout = TIMEOUT)
  public void test() throws Exception {
    JobLauncher.main(builder
        .addJobId(BroadcastITCase.class.getSimpleName())
        .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testPado() throws Exception {
    JobLauncher.main(builder
        .addJobId(BroadcastITCase.class.getSimpleName() + "_pado")
        .addOptimizationPolicy(PadoPolicyParallelismFive.class.getCanonicalName())
        .build());
  }
}
