package edu.snu.onyx.runtime.executor;

import edu.snu.onyx.runtime.common.grpc.GrpcClient;
import edu.snu.onyx.runtime.exception.NodeConnectionException;
import edu.snu.onyx.runtime.master.grpc.MasterRemotePartitionServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterSchedulerServiceGrpc;

import javax.inject.Inject;
import java.util.concurrent.ExecutionException;

public final class MasterRPC {
  private final GrpcClient

  @Inject
  public MasterRPC() {
  }

  public MasterSchedulerServiceGrpc.MasterSchedulerServiceBlockingStub getSchedulerBlockingStub() {
    return MasterSchedulerServiceGrpc.newFutureStub()
  }

  public MasterSchedulerServiceGrpc.MasterSchedulerServiceFutureStub getSchedulerFutureStub() {
    final MasterSchedulerServiceGrpc.MasterSchedulerServiceFutureStub futureStub;
    futureStub.taskGroupStateChanged()
  }

  public GrpcClient getPartitionBlockingStub() {
  }

  public GrpcClient getPartitionFutureStub() {
  }

  public MasterRemotePartitionServiceGrpc.MasterRemotePartitionServiceBlockingStub getRemoteBlockBlockingStub() {
  }


  synchronized MessageSender<ControlMessage.Message> getMessageSender(final String listenerId) {
    final MessageSender<ControlMessage.Message> messageSender = messageSenders.get(listenerId);
    if (messageSender != null) {
      return messageSender;
    } else { // Unknown message listener.
      final MessageSender<ControlMessage.Message> createdMessageSender;
      try {
        createdMessageSender = messageEnvironment.<ControlMessage.Message>asyncConnect(
            MessageEnvironment.MASTER_COMMUNICATION_ID, listenerId).get();
        messageSenders.put(listenerId, createdMessageSender);
      } catch (InterruptedException | ExecutionException e) {
        throw new NodeConnectionException(e);
      }
      return createdMessageSender;
    }
  }
}
