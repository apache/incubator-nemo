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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test MapReduce program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class MapReduceDisaggregationTest {
  private final String mapReduce = "edu.snu.vortex.examples.beam.MapReduce";
  private final String optimizationPolicy = "disaggregation";
  private final String input = "./src/main/resources/sample_input_mr";
  private final String output = "./src/main/resources/sample_output";
  private final String dagDirectory = "./target/dag/mr_disaggregation";

  @Test
  public void test() throws Exception {
    final ArgBuilder builder = new ArgBuilder()
        .addUserMain(mapReduce)
        .addOptimizationPolicy(optimizationPolicy)
        .addUserArgs(input, output)
        .addDAGDirectory(dagDirectory);
    JobLauncher.main(builder.build());
  }
}
