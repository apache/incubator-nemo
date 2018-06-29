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
package edu.snu.nemo.runtime.master;

import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageContext;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageListener;
import edu.snu.nemo.runtime.common.message.MessageSender;
import edu.snu.nemo.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.nemo.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.nemo.runtime.executor.MetricManagerWorker;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import edu.snu.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Ensures metrics collected by {@link edu.snu.nemo.runtime.executor.MetricManagerWorker} are properly sent to master
 * before the job finishes.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExecutorRepresenter.class, ExecutorRegistry.class})
public final class MetricCollectionTest {
  private static final Tang TANG = Tang.Factory.getTang();
  private static final String MASTER = "MASTER";
  private static final String WORKER = "WORKER";

  @Test(timeout = 3000)
  public void test() throws InjectionException, ExecutionException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);

    final LocalMessageDispatcher localMessagedispatcher = new LocalMessageDispatcher();

    final Configuration configuration = TANG.newConfigurationBuilder()
        .build();
    final Injector injector = TANG.newInjector(configuration);

    final Injector masterInjector = injector.forkInjector();
    final Injector workerInjector = injector.forkInjector();

    final LocalMessageEnvironment masterMessageEnvironment = new LocalMessageEnvironment(MASTER,
        localMessagedispatcher);
    masterInjector.bindVolatileInstance(MessageEnvironment.class, masterMessageEnvironment);

    final LocalMessageEnvironment workerMessageEnvironment = new LocalMessageEnvironment(WORKER,
        localMessagedispatcher);
    workerInjector.bindVolatileInstance(MessageEnvironment.class, workerMessageEnvironment);


    final MessageSender masterToWorkerSender = masterMessageEnvironment
        .asyncConnect(WORKER, MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID).get();
    
    final ExecutorRepresenter workerRepresenter = mock(ExecutorRepresenter.class);
    doAnswer((Answer<Void>) invocationOnMock -> {
      final ControlMessage.Message msg = (ControlMessage.Message) invocationOnMock.getArguments()[0];
      masterToWorkerSender.send(msg);
      return null;
    }).when(workerRepresenter).sendControlMessage(any());

    final ExecutorRegistry executorRegistry = mock(ExecutorRegistry.class);
    doAnswer((Answer<Void>) invocationOnMock -> {
      final Consumer<Set<ExecutorRepresenter>> consumer = (Consumer) invocationOnMock.getArguments()[0];
      consumer.accept(Collections.singleton(workerRepresenter));
      return null;
    }).when(executorRegistry).viewExecutors(any());

    masterInjector.bindVolatileInstance(ExecutorRegistry.class, executorRegistry);

    final MetricManagerMaster metricManagerMaster = masterInjector.getInstance(MetricManagerMaster.class);
    final MetricManagerWorker metricManagerWorker = workerInjector.getInstance(MetricManagerWorker.class);

    masterMessageEnvironment.setupListener(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID,
        new MessageListener<Object>() {
        @Override
        public void onMessage(Object message) {
          System.out.println(message);
          latch.countDown();
        }

        @Override
        public void onMessageWithContext(Object message, MessageContext messageContext) {
        }
    });

    workerMessageEnvironment.setupListener(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID,
        new MessageListener<Object>() {
          @Override
          public void onMessage(Object message) {
            metricManagerWorker.flush();
          }

          @Override
          public void onMessageWithContext(Object message, MessageContext messageContext) {
          }
        });

    metricManagerWorker.send("HEHE", "HOHO");
    metricManagerMaster.sendMetricFlushRequest();

    latch.await();
  }
}
