package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.runtime.executor.lambda.LambdaWarmer;
import org.junit.Test;

public final class LambdaWarmerTest {

  @Test
  public void test() throws InterruptedException {
    final LambdaWarmer warmer = new LambdaWarmer();
    //warmer.start();
    warmer.warmup();

    Thread.sleep(10000);
    //Thread.sleep(1000 * 60 * 100);
  }
}
