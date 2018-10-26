package org.apache.nemo.examples.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.test.ArgBuilder;
import org.apache.nemo.common.test.ExampleTestUtil;
import org.apache.nemo.examples.beam.policy.DefaultPolicyParallelismFive;
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
