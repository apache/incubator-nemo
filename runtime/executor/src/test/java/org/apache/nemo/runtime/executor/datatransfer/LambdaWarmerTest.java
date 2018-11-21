package org.apache.nemo.runtime.executor.datatransfer;

import org.junit.Test;

public final class LambdaWarmerTest {

  @Test
  public void test() throws InterruptedException {
    final LambdaWarmer warmer = new LambdaWarmer();
    warmer.start();

    Thread.sleep(1000 * 60 * 100);
  }
}
