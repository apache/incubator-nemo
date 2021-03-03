package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CpuInfoExtractor {

  private static final Logger LOG = Logger.getLogger(CpuInfoExtractor.class.getName());

  public static void printCpuSpec() {
    try {

      Process p = Runtime.getRuntime().exec(
        "cat /proc/cpuinfo");
      //   "cpulimit -l " + cpulimit + " java -cp " + path + " org.apache.nemo.offloading.workers.vm.VMWorker " + myPort + " " + 10000000);

      String line;
      BufferedReader in = new BufferedReader(
        new InputStreamReader(p.getInputStream()) );

      BufferedReader stdError = new BufferedReader(new
        InputStreamReader(p.getErrorStream()));

      while (in.ready() && (line = in.readLine()) != null) {
        LOG.info(line);
      }
      // in.close();
      // LOG.info("End of read line !!!!!!!!!!!!!!!!!!!!");

      while (stdError.ready() && (line = stdError.readLine()) != null) {
        LOG.info(line);
      }
      // stdError.close();

      p.destroy();

      in.close();
      stdError.close();

    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
