package org.apache.nemo.runtime.executor.offloading;

import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.OffloadingEvent;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.ExecutorMetrics;
import org.apache.nemo.runtime.executor.common.Serializer;

import java.util.Optional;


public interface OffloadingWorker<I, O> {

  public double getLoad();

  String getId();

  void addReadyTask(String taskId);
  void removeDoneTask(String taskId);
  boolean hasReadyTask(String taskId);

  double getProcessingRate();

  void writeControl(OffloadingEvent offloadingEvent);
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
