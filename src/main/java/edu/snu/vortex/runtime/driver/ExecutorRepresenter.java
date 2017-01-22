package edu.snu.vortex.runtime.driver;

import edu.snu.vortex.runtime.TaskGroup;
import edu.snu.vortex.runtime.VortexMessage;
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

  void sendExecuteTaskGroup(final TaskGroup taskGroup) {
    final VortexMessage message = new VortexMessage(
        getId(), VortexMessage.Type.ExecuteTaskGroup, taskGroup);
    sendMessage(message);
  }

  void sendReadRequest(final String chanId) {
    final VortexMessage message = new VortexMessage(
        getId(), VortexMessage.Type.ReadRequest, chanId);
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
