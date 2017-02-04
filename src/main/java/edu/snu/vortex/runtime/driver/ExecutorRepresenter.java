package edu.snu.vortex.runtime.driver;

import edu.snu.vortex.runtime.TaskGroup;
import edu.snu.vortex.runtime.VortexMessage;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.driver.task.RunningTask;

class ExecutorRepresenter {

  private final RunningTask runningTask;

  ExecutorRepresenter(final RunningTask runningTask) {
    this.runningTask = runningTask;
  }

  String getId() {
    return runningTask.getId();
  }

  @Override
  public String toString() {
    return getId();
  }

  void sendExecuteCachedTaskGroup(final String id) {
    final VortexMessage message = new VortexMessage(
        getId(), VortexMessage.Type.ExecutedCachedTaskGroup, id);
    sendMessage(message);
  }

  void sendExecuteTaskGroup(final TaskGroup taskGroup) {
    final VortexMessage message = new VortexMessage(
        getId(), VortexMessage.Type.ExecuteTaskGroup, taskGroup);
    sendMessage(message);
  }

  void sendReadRequest(final String targetExecutorId, final String chanId) {
    System.out.println("sendreadrequest");
    System.out.println(targetExecutorId);
    System.out.println(chanId);
    final VortexMessage message = new VortexMessage(
        getId(), targetExecutorId, VortexMessage.Type.ReadRequest, chanId);
    sendMessage(message);
  }

  void sendNotReadyResponse(final String chanId) {
    final VortexMessage message = new VortexMessage(
        getId(), VortexMessage.Type.ChannelNotReady, chanId);
    sendMessage(message);
  }

  private void sendMessage(final VortexMessage vortexMessage) {
    runningTask.send(SerializationUtils.serialize(vortexMessage));
  }
}
