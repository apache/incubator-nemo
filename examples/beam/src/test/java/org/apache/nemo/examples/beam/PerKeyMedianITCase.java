package org.apache.nemo.examples.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.test.ArgBuilder;
import org.apache.nemo.common.test.ExampleTestUtil;
import org.apache.nemo.examples.beam.policy.DataSkewPolicyParallelismFive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test PerKeyMedian program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class PerKeyMedianITCase {
  private static final int TIMEOUT = 60 * 1000;
  private static ArgBuilder builder;
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";

  private static final String inputFileName = "test_input_median";
  private static final String outputFileName = "test_output_median";
  private static final String expectedOutputFileName = "expected_output_median";
  private static final String executorResourceFileName = fileBasePath + "beam_test_executor_resources.json";
  private static final String inputFilePath =  fileBasePath + inputFileName;
  private static final String outputFilePath =  fileBasePath + outputFileName;

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
        .addResourceJson(executorResourceFileName)
        .addUserMain(PerKeyMedian.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath);
  }

  @After
  public void tearDown() throws Exception {
    try {
      ExampleTestUtil.ensureOutputValidity(fileBasePath, outputFileName, expectedOutputFileName);
    } finally {
      ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName);
    }
  }

  /**
   * Testing data skew dynamic optimization.
   * @throws Exception exception on the way.
   */
  @Test (timeout = TIMEOUT)
  public void testDataSkew() throws Exception {
    JobLauncher.main(builder
        .addJobId(PerKeyMedianITCase.class.getSimpleName())
        .addOptimizationPolicy(DataSkewPolicyParallelismFive.class.getCanonicalName())
        .build());
  }
}
