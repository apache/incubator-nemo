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
package edu.snu.vortex.examples.beam;

import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.compiler.TestUtil;
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
  private static final String mapReduce = "edu.snu.vortex.examples.beam.MapReduce";
  private static final String input = TestUtil.rootDir + "/src/main/resources/sample_input_mr";
  private static final String output = TestUtil.rootDir + "/src/main/resources/sample_output";
  private static final String dagDirectory = "./dag";

  public static ArgBuilder builder = new ArgBuilder()
      .addJobId(MapReduceITCase.class.getSimpleName())
      .addUserMain(mapReduce)
      .addUserArgs(input, output)
      .addDAGDirectory(dagDirectory);

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
        .addUserMain(mapReduce)
        .addUserArgs(input, output)
        .addDAGDirectory(dagDirectory);
  }

  @Test (timeout = TIMEOUT)
  public void test() throws Exception {
    JobLauncher.main(builder
        .addJobId(MapReduceITCase.class.getSimpleName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testDisaggregation() throws Exception {
    JobLauncher.main(builder
        .addJobId(MapReduceITCase.class.getSimpleName() + "_disaggregation")
        .addOptimizationPolicy("disaggregation")
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testPado() throws Exception {
    JobLauncher.main(builder
        .addJobId(MapReduceITCase.class.getSimpleName() + "_pado")
        .addOptimizationPolicy("pado")
        .build());
  }
}
