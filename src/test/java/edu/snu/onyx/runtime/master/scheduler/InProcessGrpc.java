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

import edu.snu.onyx.runtime.common.grpc.Common;
import edu.snu.onyx.runtime.executor.Executor;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.mockito.Mockito;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

class InProcessGrpc {
  private final String serviceName;

  public InProcessGrpc(final String serviceName) {
    this.serviceName = serviceName;
  }

  public Server getInProcessExecutorSchedulerServer() {
    final Executor.ExecutorSchedulerService executorSchedulerService = mock(Executor.ExecutorSchedulerService.class);
    Mockito.doAnswer((invocation) -> {
      final StreamObserver observer = invocation.getArgumentAt(1, StreamObserver.class);
      observer.onNext(Common.Empty.newBuilder().build());
      observer.onCompleted();
      return null;
    }).when(executorSchedulerService).executeTaskGroup(any(), any());
    return InProcessServerBuilder.forName(serviceName).addService(executorSchedulerService).build();
  }

  public ManagedChannel getInProcessChannelToExecutorScheduler() {
    return InProcessChannelBuilder.forName(serviceName).build();
  }
}
