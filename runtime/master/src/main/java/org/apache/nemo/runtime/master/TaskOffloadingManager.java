package org.apache.nemo.runtime.master;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.ir.edge.Stage;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ThreadSafe
@DriverSide
public final class TaskOffloadingManager {

  private DAG<Stage, StageEdge> stageDAG;

  private enum Status {
    PENDING,
    RUNNING
  }

  private final Map<String, Status> stageStatusMap;
  private final Map<String, Stage> stageIdMap;

  private static final Logger LOG = LoggerFactory.getLogger(TransferIndexMaster.class.getName());

  @Inject
  private TaskOffloadingManager(final MessageEnvironment masterMessageEnvironment) {
    masterMessageEnvironment.setupListener(MessageEnvironment.TASK_OFFLOADING_LISTENER_ID,
      new TaskOffloadingReceiver());
    this.stageStatusMap = new HashMap<>();
    this.stageIdMap = new HashMap<>();
  }

  public void setStageDAG(DAG<Stage, StageEdge> dag) {
    this.stageDAG = dag;
    for (Stage stage : stageDAG.getVertices()) {
      stageIdMap.put(stage.getId(), stage);
      stageStatusMap.put(stage.getId(), Status.RUNNING);
    }
  }

  private List<String> getDependencies(final Stage stage) {
    final List<StageEdge> outgoing = stageDAG.getOutgoingEdgesOf(stage);
    final List<StageEdge> incoming = stageDAG.getIncomingEdgesOf(stage);

    final List<String> dependencies = new ArrayList<>(outgoing.size() + incoming.size());

    outgoing.forEach(edge -> {
      dependencies.add(edge.getDst().getId());
    });

    incoming.forEach(edge -> {
      dependencies.add(edge.getSrc().getId());
    });

    return dependencies;
  }

  private boolean hasPendingDependencies(final List<String> dependencies) {
    for (final String stageId : dependencies) {
      if (stageStatusMap.get(stageId).equals(Status.PENDING)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Handler for control messages received.
   */
  public final class TaskOffloadingReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case RequestTaskOffloadingDone: {
          final ControlMessage.RequestTaskOffloadingDoneMessage offloadingMessage =
            message.getRequestTaskOffloadingDoneMsg();
          final String taskId = offloadingMessage.getTaskId();
          final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
          stageStatusMap.put(stageId, Status.RUNNING);
          LOG.info("Receive TaskOffloadingDone {}, map: {}", stageId, stageStatusMap);
          break;
        }
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }

    @Override
    public synchronized void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        case RequestTaskOffloading: {
          final ControlMessage.RequestTaskOffloadingMessage offloadingMessage =
            message.getRequestTaskOffloadingMsg();
          final String taskId = offloadingMessage.getTaskId();
          final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
          final Stage stage = stageIdMap.get(stageId);


          final List<String> dependencies = getDependencies(stage);

          LOG.info("Receive RequestTaskOffloading {}, dependncies: {}, map: {}", taskId, dependencies,
            stageStatusMap);

          if (hasPendingDependencies(dependencies)) {
            messageContext.reply(
              ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(MessageEnvironment.TASK_OFFLOADING_LISTENER_ID)
                .setType(ControlMessage.MessageType.TaskOffloadingInfo)
                .setTaskOffloadingInfoMsg(ControlMessage.TaskOffloadingInfoMessage.newBuilder()
                  .setRequestId(message.getId())
                  .setCanOffloading(false)
                  .build())
                .build());
          } else {
            stageStatusMap.put(stageId, Status.PENDING);
            messageContext.reply(
              ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(MessageEnvironment.TASK_OFFLOADING_LISTENER_ID)
                .setType(ControlMessage.MessageType.TaskOffloadingInfo)
                .setTaskOffloadingInfoMsg(ControlMessage.TaskOffloadingInfoMessage.newBuilder()
                  .setRequestId(message.getId())
                  .setCanOffloading(true)
                  .build())
                .build());
          }
          break;
        }

        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }
  }
}
