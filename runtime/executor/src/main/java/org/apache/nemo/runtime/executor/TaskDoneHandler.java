package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import static org.apache.nemo.runtime.common.message.MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID;

public final class TaskDoneHandler implements EventHandler<String> {
  private static final Logger LOG = LoggerFactory.getLogger(TaskDoneHandler.class.getName());

  private TaskExecutorMapWrapper taskExecutorMapWrapper;
  private final PersistentConnectionToMasterMap toMaster;
  private final String executorId;


  @Inject
  private TaskDoneHandler(final TaskExecutorMapWrapper taskExecutorMapWrapper,
                          final PersistentConnectionToMasterMap toMaster,
                          @Parameter(JobConf.ExecutorId.class) final String executorId) {
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.toMaster = toMaster;
    this.executorId = executorId;
  }

  @Override
  public void onNext(String taskId) {
    LOG.info("Handling complete deletion of task " + taskId);
    // taskExecutorMapWrapper.removeTask(taskId);
    // Send stop done signal to master
    toMaster.getMessageSender(SCALE_DECISION_MESSAGE_LISTENER_ID)
      .send(ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.StopTaskDone)
        .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
        .setExecutorId(executorId)
        .setTaskId(taskId)
        .build())
        .build());
  }
}
