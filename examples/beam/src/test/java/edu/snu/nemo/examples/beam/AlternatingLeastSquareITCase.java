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
import edu.snu.nemo.examples.beam.policy.DefaultPolicyParallelismFive;
import edu.snu.nemo.examples.beam.policy.PadoPolicyParallelsimFive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Optional;

/**
 * Test Alternating Least Square program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class AlternatingLeastSquareITCase {
  private static final int TIMEOUT = 240000;
  private static ArgBuilder builder = new ArgBuilder();
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";

  private static final String input = fileBasePath + "sample_input_als";
  private static final String outputFileName = "sample_output_als";
  private static final String output = fileBasePath + outputFileName;
  private static final String testResourceFileName = "test_output_als";
  private static final String numFeatures = "10";
  private static final String numIteration = "3";
  private static final String lambda = "0.05";

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder();
  }

  @After
  public void tearDown() throws Exception {
    final Optional<String> errorMsg =
        ExampleTestUtil.ensureALSOutputValidity(fileBasePath, outputFileName, testResourceFileName);
    ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName);
    if (errorMsg.isPresent()) {
      throw new RuntimeException(errorMsg.get());
    }
  }

  @Test (timeout = TIMEOUT)
  public void test() throws Exception {
    JobLauncher.main(builder
        .addJobId(AlternatingLeastSquareITCase.class.getSimpleName())
        .addUserMain(AlternatingLeastSquare.class.getCanonicalName())
        .addUserArgs(input, numFeatures, numIteration, lambda, output)
        .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testPado() throws Exception {
    JobLauncher.main(builder
        .addJobId(AlternatingLeastSquareITCase.class.getSimpleName() + "_pado")
        .addUserMain(AlternatingLeastSquare.class.getCanonicalName())
        .addUserArgs(input, numFeatures, numIteration, lambda, output)
        .addOptimizationPolicy(PadoPolicyParallelsimFive.class.getCanonicalName())
        .build());
  }
}
