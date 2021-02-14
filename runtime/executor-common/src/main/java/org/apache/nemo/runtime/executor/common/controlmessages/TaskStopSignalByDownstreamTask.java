package org.apache.nemo.runtime.executor.common.controlmessages;

import java.io.DataInputStream;
import java.io.DataOutputStream;
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
    final DataOutputStream dos = new DataOutputStream(bos);
    try {
      dos.writeUTF(srcTaskId);
      dos.writeUTF(edgeId);
      dos.writeUTF(dstTaskId);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static TaskStopSignalByDownstreamTask decode(final InputStream is) {
    try {
      final DataInputStream dis = new DataInputStream(is);
      final String stid = dis.readUTF();
      final String eid = dis.readUTF();
      final String dtid = dis.readUTF();

      return new TaskStopSignalByDownstreamTask(
        stid, eid, dtid);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
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
