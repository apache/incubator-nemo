package org.apache.nemo.runtime.executor.common;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CpuInfoExtractor {

  private static final Logger LOG = Logger.getLogger("LOG");

  public static void printSpecs(int requestId) {
    System.out.println(printCommand("cat /proc/cpuinfo", requestId));
    System.out.println(printCommand("cat /proc/meminfo", requestId));
  }

  public static void printNetworkStat(int requestId) {
    LOG.info(printCommand("cat /proc/net/dev", requestId));
  }

  public static String printCommand(String cmd, int requestId) {
    try {

      Process p = Runtime.getRuntime().exec(
        cmd);
      //   "cpulimit -l " + cpulimit + " java -cp " + path + " org.apache.nemo.offloading.workers.vm.VMWorker " + myPort + " " + 10000000);

      StringBuilder sb = new StringBuilder("--------- Start printing command " + cmd +
        " worker id " + requestId + " ---------\n");

      String line;
      BufferedReader in = new BufferedReader(
        new InputStreamReader(p.getInputStream()) );

      BufferedReader stdError = new BufferedReader(new
        InputStreamReader(p.getErrorStream()));

      long st = System.currentTimeMillis();
      while (!in.ready() && !stdError.ready() && System.currentTimeMillis() - st < 3000) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      while (in.ready() && (line = in.readLine()) != null) {
        sb.append(line);
        sb.append("\n");
      }
      // in.close();
      // LOG.info("End of read line !!!!!!!!!!!!!!!!!!!!");

      while (stdError.ready() && (line = stdError.readLine()) != null) {
        sb.append(line);
        sb.append("\n");
      }
      // stdError.close();

      p.destroy();

      in.close();
      stdError.close();

      sb.append("--------- End printing command " + cmd + " worker id "
        + requestId + " ---------\n");
      return sb.toString();
    } catch (IOException e) {
      e.printStackTrace();
      return "";
    }
  }
}
