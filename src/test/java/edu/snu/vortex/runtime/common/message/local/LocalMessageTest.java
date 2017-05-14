package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.*;
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

    final MessageEnvironment driverEnv = new LocalMessageEnvironment(driverNodeId, localMessageDispatcher);
    final MessageEnvironment executorOneEnv = new LocalMessageEnvironment(executorOneNodeId, localMessageDispatcher);
    final MessageEnvironment executorTwoEnv = new LocalMessageEnvironment(executorTwoNodeId, localMessageDispatcher);

    final AtomicInteger toDriverMessageUsingSend = new AtomicInteger();

    driverEnv.setupListener("ToDriver", new MessageListener<ToDriver>() {
      @Override
      public void onMessage(final ToDriver message) {
        toDriverMessageUsingSend.incrementAndGet();
      }

      @Override
      public void onMessageWithContext(final ToDriver message, final MessageContext messageContext) {
        if (message instanceof ExecutorStarted) {
          messageContext.reply(true);
        } else if (message instanceof MakeException) {
          messageContext.replyThrowable(new RuntimeException());
        }
      }
    });

    // Setup multiple listeners.
    driverEnv.setupListener("SecondToDriver", new MessageListener<SecondToDriver>() {
      @Override
      public void onMessage(SecondToDriver message) {
      }

      @Override
      public void onMessageWithContext(SecondToDriver message, MessageContext messageContext) {
      }
    });

    // Test sending message from executors to the driver.

    final Future<MessageSender<ToDriver>> messageSenderFuture1 = executorOneEnv.asyncConnect(
        driverNodeId, "ToDriver");
    Assert.assertTrue(messageSenderFuture1.isDone());
    final MessageSender<ToDriver> messageSender1 = messageSenderFuture1.get();

    final Future<MessageSender<ToDriver>> messageSenderFuture2 = executorTwoEnv.asyncConnect(
        driverNodeId, "ToDriver");
    Assert.assertTrue(messageSenderFuture2.isDone());
    final MessageSender<ToDriver> messageSender2 = messageSenderFuture2.get();

    messageSender1.send(new ExecutorStarted());
    messageSender2.send(new ExecutorStarted());

    Assert.assertEquals(2, toDriverMessageUsingSend.get());
    Assert.assertTrue(messageSender1.<Boolean>request(new ExecutorStarted()).get());
    Assert.assertTrue(messageSender2.<Boolean>request(new ExecutorStarted()).get());
    try {
      messageSender1.<Boolean>request(new MakeException()).get();
      throw new RuntimeException(); // Expected not reached here.
    } catch (final Exception e) {
    }

    // Test exchanging messages between executors.

    executorOneEnv.setupListener("BetweenExecutors", new SimpleMessageListener());
    executorTwoEnv.setupListener("BetweenExecutors", new SimpleMessageListener());

    final MessageSender<BetweenExecutors> oneToTwo = executorOneEnv.<BetweenExecutors>asyncConnect(
        executorTwoNodeId, "BetweenExecutors").get();
    final MessageSender<BetweenExecutors> twoToOne = executorOneEnv.<BetweenExecutors>asyncConnect(
        executorOneNodeId, "BetweenExecutors").get();

    Assert.assertEquals("oneToTwo", oneToTwo.<String>request(new SimpleMessage("oneToTwo")).get());
    Assert.assertEquals("twoToOne", twoToOne.<String>request(new SimpleMessage("twoToOne")).get());
  }

  final class SimpleMessageListener implements MessageListener<SimpleMessage> {

    @Override
    public void onMessage(final SimpleMessage message) {
      // Expected not reached here.
      throw new RuntimeException();
    }

    @Override
    public void onMessageWithContext(final SimpleMessage message, final MessageContext messageContext) {
      messageContext.reply(message.getData());
    }
  }

  interface ToDriver extends Serializable {
  }

  final class ExecutorStarted implements ToDriver {
  }
  final class MakeException implements ToDriver {
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
