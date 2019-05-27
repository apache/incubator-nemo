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

import javax.inject.Inject;

public final class TaskOffloadingManager {

  private DAG<Stage, StageEdge> stageDAG;

  @Inject
  private TaskOffloadingManager(final MessageEnvironment masterMessageEnvironment) {
    masterMessageEnvironment.setupListener(MessageEnvironment.TASK_OFFLOADING_LISTENER_ID,
      new TaskOffloadingReceiver());
  }

  public void setStageDAG(DAG<Stage, StageEdge> stageDAG) {
    this.stageDAG = stageDAG;
  }

  /**
   * Handler for control messages received.
   */
  public final class TaskOffloadingReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      throw new RuntimeException("Exception " + message);
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        case RequestTransferIndex:
          final ControlMessage.RequestTransferIndexMessage requestIndexMessage = message.getRequestTransferIndexMsg();
          final int isInputContext = (int) requestIndexMessage.getIsInputContext();

          //final int index = isInputContext == 1 ? inputContextIndex.getAndIncrement() : outputContextIndex.getAndIncrement();
          final int index = contextIndex.getAndIncrement();

          LOG.info("Send input/output ({}) context index {}", isInputContext, index);

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.TRANSFER_INDEX_LISTENER_ID)
              .setType(ControlMessage.MessageType.TransferIndexInfo)
              .setTransferIndexInfoMsg(ControlMessage.TransferIndexInfoMessage.newBuilder()
                .setRequestId(message.getId())
                .setIndex(index)
                .build())
              .build());

          break;
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }
  }
}
