package org.apache.nemo.runtime.master.scaler;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.master.backpressure.InputAndQueueSizeBasedBackpressure;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.reef.tang.annotations.DefaultImplementation;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.EXECUTOR_MESSAGE_LISTENER_ID;

@DefaultImplementation(InputAndCpuBasedScaler.class)
public interface Scaler {

  void addSourceEvent(final long sourceEvent);
  void addCurrentInput(final long rate);
}

