package org.apache.nemo.runtime.executor;

import org.apache.nemo.runtime.executor.offloading.LocalExecutorOffloadingRequester;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class VMWorkerExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(VMWorkerExecutor.class.getName());

  @NamedParameter
  public static final class VMWorkerPort implements Name<Integer> {}


  private final ExecutorService waitingExecutor = Executors.newCachedThreadPool();

  private transient boolean finished = false;

  @Inject
  private VMWorkerExecutor(
    @Parameter(VMWorkerPort.class) final int port) {
    final String nemo_home = "/home/taegeonum/incubator-nemo";
    LOG.info("Creating VM worker with port " + port);
    final String path = nemo_home + "/offloading/workers/vm/target/offloading-vm-0.2-SNAPSHOT-shaded.jar";

    LOG.info("java -cp " + path + " org.apache.nemo.offloading.workers.vm.VMWorker " + port + " " + 10000000);
    waitingExecutor.execute(() -> {
      try {

        //LOG.info("cpulimit -l " + cpulimit + " java -cp " + path + " org.apache.nemo.offloading.workers.vm.VMWorker " + myPort + " " + 10000000);
        LOG.info("java -verbosegc -cp " + path + " org.apache.nemo.offloading.workers.vm.VMWorker " + port + " " + 10000000);
        Process p = Runtime.getRuntime().exec(
          "java -verbosegc -cp " + path + " org.apache.nemo.offloading.workers.vm.VMWorker " + port + " " + 10000000);
        //   "cpulimit -l " + cpulimit + " java -cp " + path + " org.apache.nemo.offloading.workers.vm.VMWorker " + myPort + " " + 10000000);

        String line;
        BufferedReader in = new BufferedReader(
          new InputStreamReader(p.getInputStream()) );

        BufferedReader stdError = new BufferedReader(new
          InputStreamReader(p.getErrorStream()));


        while (!finished) {
          while (in.ready() && (line = in.readLine()) != null) {
            LOG.info("[VMWworker " + port + "]: " + line);
          }
          // in.close();
          // LOG.info("End of read line !!!!!!!!!!!!!!!!!!!!");

          while (stdError.ready() && (line = stdError.readLine()) != null) {
            LOG.info("[VMWworker " + port + "]: " + line);
          }
          // stdError.close();

          try {
            Thread.sleep(300);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        p.destroy();

        in.close();
        stdError.close();

      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
  }

  public void terminate() {
    finished = true;
  }
}
