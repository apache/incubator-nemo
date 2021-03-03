package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CpuInfoExtractor {

  private static final Logger LOG = Logger.getLogger(CpuInfoExtractor.class.getName());

  public static void printCpuSpec(int requestId) {
    System.out.println("----------- Start cpu info worker " + requestId + "----------");
    printCommand("cat /proc/cpuinfo");
    System.out.println("----------- End cpu info worker " + requestId + "----------");
  }

  public static void printMemSpec(int requestId) {
    System.out.println("----------- Start mem info worker " + requestId + "----------");
    printCommand("cat /proc/meminfo");
    System.out.println("----------- End mem info worker " + requestId + "----------");
  }

  public static void printNetworkStat(int requestId) {
     System.out.println("----------- Start nstat " + requestId + "----------");
    printCommand("nstat");
    System.out.println("----------- End nstat " + requestId + "----------");

    System.out.println("----------- Start /sbin/ifconfig " + requestId + "----------");
    printCommand("/sbin/ifconfig");
    System.out.println("----------- End /sbin/ifconfig " + requestId + "----------");

    System.out.println("----------- Start ip -s link " + requestId + "----------");
    printCommand("ip -s link");
    System.out.println("----------- End ip -s link " + requestId + "----------");
  }

  public static void printCommand(String cmd) {
    try {
      System.out.println("Printing command " + cmd);

      Process p = Runtime.getRuntime().exec(
        cmd);
      //   "cpulimit -l " + cpulimit + " java -cp " + path + " org.apache.nemo.offloading.workers.vm.VMWorker " + myPort + " " + 10000000);

      String line;
      BufferedReader in = new BufferedReader(
        new InputStreamReader(p.getInputStream()) );

      BufferedReader stdError = new BufferedReader(new
        InputStreamReader(p.getErrorStream()));

      while (in.ready() && (line = in.readLine()) != null) {
        System.out.println(line);
      }
      // in.close();
      // LOG.info("End of read line !!!!!!!!!!!!!!!!!!!!");

      while (stdError.ready() && (line = stdError.readLine()) != null) {
        System.out.println(line);
      }
      // stdError.close();

      p.destroy();

      in.close();
      stdError.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
