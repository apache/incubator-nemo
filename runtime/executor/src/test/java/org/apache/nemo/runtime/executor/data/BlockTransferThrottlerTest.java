package org.apache.nemo.runtime.executor.data;

import org.apache.nemo.conf.JobConf;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import static org.junit.Assert.assertFalse;

public final class BlockTransferThrottlerTest {
  private static final String THREAD_NAME = BlockTransferThrottler.class.getSimpleName() + "-TestThread";
  private static final String RUNTIME_EDGE_0 = "RuntimeEdge0";
  private static final int WAIT_TIME = 1000;
  /**
   * Creates {@link BlockTransferThrottler} for testing.
   * @param maxNum value for {@link JobConf.MaxNumDownloadsForARuntimeEdge} parameter.
   * @return {@link BlockTransferThrottler} object created.
   */
  private final BlockTransferThrottler getQueue(final int maxNum) {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(JobConf.MaxNumDownloadsForARuntimeEdge.class, String.valueOf(maxNum))
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    try {
      return injector.getInstance(BlockTransferThrottler.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Test(timeout = WAIT_TIME * 2)
  public void test() throws InterruptedException, ExecutionException {
    final ExecutorService executorService = Executors.newSingleThreadExecutor(
        runnable -> new Thread(runnable, THREAD_NAME));
    final BlockTransferThrottler queue = getQueue(3);
    final Future executorServiceFuture = executorService.submit(() -> {
      try {
        queue.requestTransferPermission(RUNTIME_EDGE_0).get();
        queue.requestTransferPermission(RUNTIME_EDGE_0).get();
        queue.requestTransferPermission(RUNTIME_EDGE_0).get();
        queue.requestTransferPermission(RUNTIME_EDGE_0).get();
      } catch (final InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    Thread.sleep(WAIT_TIME);
    // We must have one pending connection request.
    assertFalse(executorServiceFuture.isDone());
    queue.onTransferFinished(RUNTIME_EDGE_0);
    // The remaining request should be accepted before test timeout.
    executorServiceFuture.get();
  }
}
