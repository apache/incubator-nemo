/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.executor.data;

import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.executor.data.BlockTransferConnectionQueue;
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

public final class BlockTransferConnectionQueueTest {
  private static final String THREAD_NAME = BlockTransferConnectionQueue.class.getSimpleName() + "-TestThread";
  private static final String RUNTIME_EDGE_0 = "RuntimeEdge0";
  private static final int WAIT_TIME = 1000;
  /**
   * Creates {@link BlockTransferConnectionQueue} for testing.
   * @param maxNum value for {@link JobConf.MaxNumDownloadsForARuntimeEdge} parameter.
   * @return {@link BlockTransferConnectionQueue} object created.
   */
  private final BlockTransferConnectionQueue getQueue(final int maxNum) {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(JobConf.MaxNumDownloadsForARuntimeEdge.class, String.valueOf(maxNum))
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    try {
      return injector.getInstance(BlockTransferConnectionQueue.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Test(timeout = WAIT_TIME * 2)
  public void test() throws InterruptedException, ExecutionException {
    final ExecutorService executorService = Executors.newSingleThreadExecutor(
        runnable -> new Thread(runnable, THREAD_NAME));
    final BlockTransferConnectionQueue queue = getQueue(3);
    final Future executorServiceFuture = executorService.submit(() -> {
      try {
        queue.requestConnectPermission(RUNTIME_EDGE_0).get();
        queue.requestConnectPermission(RUNTIME_EDGE_0).get();
        queue.requestConnectPermission(RUNTIME_EDGE_0).get();
        queue.requestConnectPermission(RUNTIME_EDGE_0).get();
      } catch (final InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    Thread.sleep(WAIT_TIME);
    // We must have one pending connection request.
    assertFalse(executorServiceFuture.isDone());
    queue.onConnectionFinished(RUNTIME_EDGE_0);
    // The remaining request should be accepted before test timeout.
    executorServiceFuture.get();
  }
}
