package org.apache.nemo.runtime.executor.common.controlmessages;

import org.apache.commons.lang3.SerializationUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

public final class TaskStopSignalByDownstreamTask {

  public final String srcTaskId;
  public final String edgeId;
  public final String dstTaskId;

  public TaskStopSignalByDownstreamTask(final String srcTaskId,
                                        final String edgeId,
                                        final String dstTaskId) {
    this.srcTaskId = srcTaskId;
    this.edgeId = edgeId;
    this.dstTaskId = dstTaskId;
  }

  public void encode(final OutputStream bos) {
    SerializationUtils.serialize(srcTaskId, bos);
    SerializationUtils.serialize(edgeId, bos);
    SerializationUtils.serialize(dstTaskId, bos);
  }

  public static TaskStopSignalByDownstreamTask decode(final InputStream is) {
    return new TaskStopSignalByDownstreamTask(
      SerializationUtils.deserialize(is),
      SerializationUtils.deserialize(is),
      SerializationUtils.deserialize(is)
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TaskStopSignalByDownstreamTask that = (TaskStopSignalByDownstreamTask) o;
    return Objects.equals(srcTaskId, that.srcTaskId) &&
      Objects.equals(edgeId, that.edgeId) &&
      Objects.equals(dstTaskId, that.dstTaskId);
  }

  @Override
  public int hashCode() {

    return Objects.hash(srcTaskId, edgeId, dstTaskId);
  }
}
