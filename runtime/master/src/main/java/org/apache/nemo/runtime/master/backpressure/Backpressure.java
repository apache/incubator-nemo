package org.apache.nemo.runtime.master.backpressure;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.reef.tang.annotations.DefaultImplementation;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.EXECUTOR_MESSAGE_LISTENER_ID;

@DefaultImplementation(InputAndQueueSizeBasedBackpressure.class)
public interface Backpressure {

  void addSourceEvent(final long sourceEvent);
  void addCurrentInput(final long rate);

  default void sendBackpressure(final ExecutorRegistry executorRegistry,
                                final long rate) {
    executorRegistry.viewExecutors(executors -> {
      executors.forEach(executor -> {
        if (executor.getContainerType().equals(ResourcePriorityProperty.SOURCE)) {
          executor.sendControlMessage(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.ThrottleSource)
            .setSetNum(rate)
            .build());
        }
      });
    });
  }
}

