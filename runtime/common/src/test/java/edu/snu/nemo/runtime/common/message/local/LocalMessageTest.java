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
package edu.snu.nemo.runtime.common.message.local;

import edu.snu.nemo.runtime.common.message.MessageContext;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageListener;
import edu.snu.nemo.runtime.common.message.MessageSender;
import edu.snu.nemo.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.nemo.runtime.common.message.local.LocalMessageEnvironment;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests local messaging components.
 */
public class LocalMessageTest {
  private final LocalMessageDispatcher localMessageDispatcher = new LocalMessageDispatcher();

  @Test
  public void testLocalMessages() throws Exception {
    final String driverNodeId = "DRIVER_NODE";
    final String executorOneNodeId = "EXECUTOR_ONE_NODE";
    final String executorTwoNodeId = "EXECUTOR_TWO_NODE";

    final String listenerIdToDriver = "ToDriver";
    final String secondListenerIdToDriver = "SecondToDriver";
    final String listenerIdBetweenExecutors = "BetweenExecutors";

    final MessageEnvironment driverEnv = new LocalMessageEnvironment(driverNodeId, localMessageDispatcher);
    final MessageEnvironment executorOneEnv = new LocalMessageEnvironment(executorOneNodeId, localMessageDispatcher);
    final MessageEnvironment executorTwoEnv = new LocalMessageEnvironment(executorTwoNodeId, localMessageDispatcher);

    final AtomicInteger toDriverMessageUsingSend = new AtomicInteger();

    driverEnv.setupListener(listenerIdToDriver, new MessageListener<ToDriver>() {
      @Override
      public void onMessage(final ToDriver message) {
        toDriverMessageUsingSend.incrementAndGet();
      }

      @Override
      public void onMessageWithContext(final ToDriver message, final MessageContext messageContext) {
        messageContext.reply(true);
      }
    });

    // Setup multiple listeners.
    driverEnv.setupListener(secondListenerIdToDriver, new MessageListener<SecondToDriver>() {
      @Override
      public void onMessage(SecondToDriver message) {
      }

      @Override
      public void onMessageWithContext(SecondToDriver message, MessageContext messageContext) {
      }
    });

    // Test sending message from executors to the driver.

    final Future<MessageSender<ToDriver>> messageSenderFuture1 = executorOneEnv.asyncConnect(
        driverNodeId, listenerIdToDriver);
    Assert.assertTrue(messageSenderFuture1.isDone());
    final MessageSender<ToDriver> messageSender1 = messageSenderFuture1.get();

    final Future<MessageSender<ToDriver>> messageSenderFuture2 = executorTwoEnv.asyncConnect(
        driverNodeId, listenerIdToDriver);
    Assert.assertTrue(messageSenderFuture2.isDone());
    final MessageSender<ToDriver> messageSender2 = messageSenderFuture2.get();

    messageSender1.send(new ExecutorStarted());
    messageSender2.send(new ExecutorStarted());

    Assert.assertEquals(2, toDriverMessageUsingSend.get());
    Assert.assertTrue(messageSender1.<Boolean>request(new ExecutorStarted()).get());
    Assert.assertTrue(messageSender2.<Boolean>request(new ExecutorStarted()).get());

    // Test exchanging messages between executors.

    final AtomicInteger executorOneMessageCount = new AtomicInteger();
    final AtomicInteger executorTwoMessageCount = new AtomicInteger();

    executorOneEnv.setupListener(listenerIdBetweenExecutors, new SimpleMessageListener(executorOneMessageCount));
    executorTwoEnv.setupListener(listenerIdBetweenExecutors, new SimpleMessageListener(executorTwoMessageCount));

    final MessageSender<BetweenExecutors> oneToTwo = executorOneEnv.<BetweenExecutors>asyncConnect(
        executorTwoNodeId, listenerIdBetweenExecutors).get();
    final MessageSender<BetweenExecutors> twoToOne = executorTwoEnv.<BetweenExecutors>asyncConnect(
        executorOneNodeId, listenerIdBetweenExecutors).get();

    Assert.assertEquals("oneToTwo", oneToTwo.<String>request(new SimpleMessage("oneToTwo")).get());
    Assert.assertEquals("twoToOne", twoToOne.<String>request(new SimpleMessage("twoToOne")).get());
    Assert.assertEquals(1, executorOneMessageCount.get());
    Assert.assertEquals(1, executorTwoMessageCount.get());

    // Test deletion and re-setting of listener.

    final AtomicInteger newExecutorOneMessageCount = new AtomicInteger();
    final AtomicInteger newExecutorTwoMessageCount = new AtomicInteger();

    executorOneEnv.removeListener(listenerIdBetweenExecutors);
    executorTwoEnv.removeListener(listenerIdBetweenExecutors);

    executorOneEnv.setupListener(listenerIdBetweenExecutors, new SimpleMessageListener(newExecutorOneMessageCount));
    executorTwoEnv.setupListener(listenerIdBetweenExecutors, new SimpleMessageListener(newExecutorTwoMessageCount));

    final MessageSender<BetweenExecutors> newOneToTwo = executorOneEnv.<BetweenExecutors>asyncConnect(
        executorTwoNodeId, listenerIdBetweenExecutors).get();
    final MessageSender<BetweenExecutors> newTwoToOne = executorTwoEnv.<BetweenExecutors>asyncConnect(
        executorOneNodeId, listenerIdBetweenExecutors).get();

    Assert.assertEquals("newOneToTwo", newOneToTwo.<String>request(new SimpleMessage("newOneToTwo")).get());
    Assert.assertEquals("newTwoToOne", newTwoToOne.<String>request(new SimpleMessage("newTwoToOne")).get());
    Assert.assertEquals(1, executorOneMessageCount.get());
    Assert.assertEquals(1, executorTwoMessageCount.get());
    Assert.assertEquals(1, newExecutorOneMessageCount.get());
    Assert.assertEquals(1, newExecutorTwoMessageCount.get());
  }

  final class SimpleMessageListener implements MessageListener<SimpleMessage> {
    private final AtomicInteger messageCount;

    private SimpleMessageListener(final AtomicInteger messageCount) {
      this.messageCount = messageCount;
    }

    @Override
    public void onMessage(final SimpleMessage message) {
      // Expected not reached here.
      throw new RuntimeException();
    }

    @Override
    public void onMessageWithContext(final SimpleMessage message, final MessageContext messageContext) {
      messageCount.getAndIncrement();
      messageContext.reply(message.getData());
    }
  }

  interface ToDriver extends Serializable {
  }

  final class ExecutorStarted implements ToDriver {
  }

  interface SecondToDriver extends Serializable {
  }

  interface BetweenExecutors extends Serializable {
  }

  final class SimpleMessage implements BetweenExecutors {
    private final String data;
    SimpleMessage(final String data) {
      this.data = data;
    }

    public String getData() {
      return data;
    }
  }
}
