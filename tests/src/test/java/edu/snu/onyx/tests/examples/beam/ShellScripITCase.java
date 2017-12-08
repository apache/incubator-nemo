package edu.snu.onyx.tests.examples.beam;

import org.junit.Test;

public final class ShellScripITCase {
  private static final int TIMEOUT = 120000;

  @Test(timeout = TIMEOUT)
  public void test() throws Exception {
    Runtime.getRuntime().exec("../bin/run.sh");
  }

}
