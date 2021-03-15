package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Task;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.offloading.common.Pair;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.nemo.runtime.executor.common.TaskExecutorUtil.getDecoderFactory;
import static org.apache.nemo.runtime.executor.common.TaskExecutorUtil.getEncoderFactory;

public final class StreamVertexSerializerManager {

  private final Map<String, EncoderFactory> encoderMap;
  private final Map<String, DecoderFactory> decoderMap;

  @Inject
  private StreamVertexSerializerManager() {
    this.encoderMap = new ConcurrentHashMap<>();
    this.decoderMap = new ConcurrentHashMap<>();
  }

  public void register(Task task) {
    if (task.isStreamVertex) {
      task.getTaskIncomingEdges().forEach(e ->
        encoderMap.put(task.getTaskId(), getEncoderFactory(e.getPropertyValue(EncoderProperty.class).get())));

      task.getTaskOutgoingEdges().forEach(e ->
        decoderMap.put(task.getTaskId(), getDecoderFactory(e.getPropertyValue(DecoderProperty.class).get())));
    }
  }

  public boolean isStreamVertexTask(final String taskId) {
    return encoderMap.containsKey(taskId);
  }

  public EncoderFactory getInputEncoderFactory(final String taskId) {
    return encoderMap.get(taskId);
  }

  public DecoderFactory getOutputDecoderFactory(final String taskId) {
    return decoderMap.get(taskId);
  }
}
