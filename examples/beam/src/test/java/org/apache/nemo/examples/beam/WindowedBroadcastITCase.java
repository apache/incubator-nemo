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
package org.apache.nemo.examples.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.test.ArgBuilder;
import org.apache.nemo.common.test.ExampleTestUtil;
import org.apache.nemo.examples.beam.policy.StreamingPolicyParallelismFive;
import org.apache.nemo.runtime.master.scheduler.StreamingScheduler;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test Windowed word count program with JobLauncher.
 * TODO #291: ITCase for Empty PCollectionViews
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class WindowedBroadcastITCase {

  private static final int TIMEOUT = 120000;
  private static ArgBuilder builder;
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";

  private static final String outputFileName = "test_output_windowed_broadcast";
  private static final String expectedOutputFileName = "expected_output_windowed_broadcast";
  private static final String expectedSlidingWindowOutputFileName = "expected_output_sliding_windowed_broadcast";
  private static final String executorResourceFileName = fileBasePath + "beam_test_executor_resources.json";
  private static final String outputFilePath = fileBasePath + outputFileName;

  // TODO #271: We currently disable this test because we cannot force close Nemo
  // @Test (timeout = TIMEOUT)
  public void testUnboundedSlidingWindow() throws Exception {
    builder = new ArgBuilder()
      .addScheduler(StreamingScheduler.class.getCanonicalName())
      .addUserMain(WindowedBroadcast.class.getCanonicalName())
      .addUserArgs(outputFilePath);

    JobLauncher.main(builder
      .addResourceJson(executorResourceFileName)
      .addJobId(WindowedBroadcastITCase.class.getSimpleName())
      .addOptimizationPolicy(StreamingPolicyParallelismFive.class.getCanonicalName())
      .build());

    try {
      ExampleTestUtil.ensureOutputValidity(fileBasePath, outputFileName, expectedSlidingWindowOutputFileName);
    } finally {
    }
  }
}
