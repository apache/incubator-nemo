/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.runtime.master.scheduler;

import edu.snu.onyx.runtime.common.grpc.CommonMessage;
import edu.snu.onyx.runtime.executor.Executor;
import edu.snu.onyx.runtime.master.resource.ExecutorRepresenter;
import edu.snu.onyx.runtime.master.resource.ResourceSpecification;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.reef.driver.context.ActiveContext;
import org.mockito.Mockito;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

class MockedExecutorRepresenters implements Closeable {
  private final Server inProcessServer;
  private final String serviceName;

  public MockedExecutorRepresenters(final String serviceName) throws IOException {
    this.serviceName = serviceName;
    final Executor.ExecutorSchedulerMessageService executorSchedulerService =
        mock(Executor.ExecutorSchedulerMessageService.class);
    Mockito.doAnswer((invocation) -> {
      final StreamObserver observer = invocation.getArgumentAt(1, StreamObserver.class);
      observer.onNext(CommonMessage.Empty.newBuilder().build());
      observer.onCompleted();
      return null;
    }).when(executorSchedulerService).executeTaskGroup(any(), any());
    inProcessServer = InProcessServerBuilder.forName(serviceName).addService(executorSchedulerService).build().start();
  }

  public ExecutorRepresenter newExecutorRepresenter(final String executorId,
                                                    final ResourceSpecification resourceSpecification) {
    return new ExecutorRepresenter(
        executorId,
        resourceSpecification,
        InProcessChannelBuilder.forName(serviceName).build(),
        mock(ActiveContext.class));
  }

  @Override
  public void close() {
    inProcessServer.shutdown();
    try {
      inProcessServer.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
