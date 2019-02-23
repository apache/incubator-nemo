package org.apache.nemo.runtime.executor;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

public class MyTest {

  @Test
  public void cpuProfileTest() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final SystemLoadProfiler slp = injector.getInstance(SystemLoadProfiler.class);
    System.out.println("Load: " + slp.getCpuLoad());

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    System.out.println("Load: " + slp.getCpuLoad());
  }
}
