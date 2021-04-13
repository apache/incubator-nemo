package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.controlmessages.DefaultControlEventHandlerImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(DefaultControlEventHandlerImpl.class)
public interface ControlEventHandler {
  void handleControlEvent(TaskHandlingEvent event);
}
