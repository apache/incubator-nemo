package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.runtime.executor.TinyTaskWorker;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.lambdaexecutor.StateOutput;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;

import java.util.*;

public interface Offloader {


  public TaskExecutor.PendingState getPendingStatus();

  public void handleOffloadingOutput(final KafkaOffloadingOutput output);

  void handleStateOutput(final StateOutput output);

  void offloadingData(final Object event,
                      final List<String> nextOperatorIds,
                      final long wm,
                      final String edgeId);

  public void handleEndOffloadingEvent(boolean moveToVmScaling);

  public void handleStartOffloadingEvent(TinyTaskWorker worker);

  public default void callTaskOffloadingDone() {
    // do nothing
  }

}
