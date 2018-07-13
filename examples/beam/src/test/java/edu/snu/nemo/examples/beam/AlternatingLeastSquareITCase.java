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
import edu.snu.nemo.compiler.optimizer.policy.DefaultPolicy;
import edu.snu.nemo.examples.beam.policy.PadoPolicyParallelismFive;
import edu.snu.nemo.examples.beam.policy.PadoPolicyParallelismTen;
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
  private static final int TIMEOUT = 240 * 1000;
  private static ArgBuilder builder;
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";

  private static final String input = fileBasePath + "sample_input_als";
  private static final String outputFileName = "sample_output_als";
  private static final String output = fileBasePath + outputFileName;
  private static final String testResourceFileName = "test_output_als";
  private static final String noPoisonResources = fileBasePath + "beam_sample_executor_resources.json";
  private static final String poisonedResource = fileBasePath + "beam_sample_poisoned_executor_resources.json";
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
      ExampleTestUtil.ensureALSOutputValidity(fileBasePath, outputFileName, testResourceFileName);
    } finally {
      ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName);
    }
  }

  @Test (timeout = TIMEOUT)
  public void testDefault() throws Exception {
    JobLauncher.main(builder
        .addResourceJson(noPoisonResources)
        .addJobId(AlternatingLeastSquareITCase.class.getSimpleName() + "_default")
        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testPadoWithPoison() throws Exception {
    JobLauncher.main(builder
        .addResourceJson(poisonedResource)
        .addJobId(AlternatingLeastSquareITCase.class.getSimpleName() + "_pado_poisoned")
        .addMaxTaskAttempt(Integer.MAX_VALUE)
        .addOptimizationPolicy(PadoPolicyParallelismTen.class.getCanonicalName())
        .build());
  }
}
