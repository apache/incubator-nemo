package org.apache.nemo.examples.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.test.ArgBuilder;
import org.apache.nemo.common.test.ExampleTestUtil;
import org.apache.nemo.examples.beam.policy.DefaultPolicyParallelismFive;
import org.apache.nemo.examples.beam.policy.TransientResourcePolicyParallelismFive;
import org.apache.nemo.examples.beam.policy.LargeShufflePolicyParallelismFive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class NetworkTraceAnalysisITCase {
  private static final int TIMEOUT = 120000;
  private static ArgBuilder builder;
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";

  private static final String inputFileName0 = "test_input_network0";
  private static final String inputFileName1 = "test_input_network1";
  private static final String outputFileName = "test_output_network";
  private static final String expectedOutputFileName = "expected_output_network";
  private static final String executorResourceFileName = fileBasePath + "beam_test_executor_resources.json";
  private static final String inputFilePath0 =  fileBasePath + inputFileName0;
  private static final String inputFilePath1 =  fileBasePath + inputFileName1;
  private static final String outputFilePath =  fileBasePath + outputFileName;

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
        .addResourceJson(executorResourceFileName)
        .addUserMain(NetworkTraceAnalysis.class.getCanonicalName())
        .addUserArgs(inputFilePath0, inputFilePath1, outputFilePath);
  }

  @After
  public void tearDown() throws Exception {
    try {
      ExampleTestUtil.ensureOutputValidity(fileBasePath, outputFileName, expectedOutputFileName);
    } finally {
      ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName);
    }
  }

  @Test(timeout = TIMEOUT)
  public void test() throws Exception {
    JobLauncher.main(builder
        .addJobId(NetworkTraceAnalysisITCase.class.getSimpleName())
        .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testLargeShuffle() throws Exception {
    JobLauncher.main(builder
        .addJobId(NetworkTraceAnalysisITCase.class.getSimpleName() + "_largeshuffle")
        .addOptimizationPolicy(LargeShufflePolicyParallelismFive.class.getCanonicalName())
        .build());
  }

  @Test (timeout = TIMEOUT)
  public void testTransientResource() throws Exception {
    JobLauncher.main(builder
        .addJobId(NetworkTraceAnalysisITCase.class.getSimpleName() + "_transient")
        .addOptimizationPolicy(TransientResourcePolicyParallelismFive.class.getCanonicalName())
        .build());
  }
}
