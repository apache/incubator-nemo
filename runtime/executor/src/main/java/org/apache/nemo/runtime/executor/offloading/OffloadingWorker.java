package org.apache.nemo.runtime.executor.offloading;

import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.executor.common.ExecutorMetrics;
import org.apache.nemo.runtime.executor.common.Serializer;

import java.util.Optional;


public interface OffloadingWorker<I, O> {

  public double getLoad();

  String getId();

  int getRequestId();

  void addReadyTask(String taskId);
  void removeDoneTask(String taskId);
  boolean hasReadyTask(String taskId);
  boolean isInputAccepted(String taskId);

  double getProcessingRate();

  void writeControl(ControlMessage.Message controlMessage);
  void writeData(int pipeIndex, TaskHandlingEvent event);
  void writeSourceData(int pipeIndex,
                       Serializer serializer,
                       Object event);

  void setMetric(ExecutorMetrics executorMetrics);

  Optional<ExecutorMetrics> getExecutorMetrics();

  boolean isReady();

  boolean isFinished();

  Channel getChannel();

}
