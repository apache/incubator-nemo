package edu.snu.onyx.runtime.executor;

import edu.snu.onyx.runtime.common.grpc.GrpcClient;
import edu.snu.onyx.runtime.common.grpc.GrpcServer;
import edu.snu.onyx.runtime.common.grpc.MasterMetricServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterPartitionServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterRemoteBlockServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterSchedulerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import net.jcip.annotations.ThreadSafe;

import javax.inject.Inject;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Helps making RPCs to the master.
 */
@ThreadSafe
public class MasterRPC implements Closeable {
  private final ManagedChannel channelToMaster; // thread-safe

  @Inject
  public MasterRPC(final GrpcClient grpcClient) {
    try {
      channelToMaster = grpcClient.openChannel(GrpcServer.MASTER_GRPC_SERVER_ID);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Scheduler.
  public MasterSchedulerServiceGrpc.MasterSchedulerServiceBlockingStub newSchedulerBlockingStub() {
    return MasterSchedulerServiceGrpc.newBlockingStub(channelToMaster);
  }

  // Partition.
  public MasterPartitionServiceGrpc.MasterPartitionServiceBlockingStub newPartitionBlockingStub() {
    return MasterPartitionServiceGrpc.newBlockingStub(channelToMaster);
  }
  public MasterPartitionServiceGrpc.MasterPartitionServiceStub newPartitionAsyncStub() {
    return MasterPartitionServiceGrpc.newStub(channelToMaster);
  }

  // RemoteBlock.
  public MasterRemoteBlockServiceGrpc.MasterRemoteBlockServiceBlockingStub newRemoteBlockBlockingStub() {
    return MasterRemoteBlockServiceGrpc.newBlockingStub(channelToMaster);
  }

  // Metrics.
  public MasterMetricServiceGrpc.MasterMetricServiceBlockingStub newMetricBlockingStub() {
    return MasterMetricServiceGrpc.newBlockingStub(channelToMaster);
  }

  // Utility methods.
  public <ResponseType> StreamObserver<ResponseType> createObserverFromCompletableFuture(
      final CompletableFuture<ResponseType> completableFuture) {
    return new StreamObserver<ResponseType>() {
      @Override
      public void onNext(final ResponseType responseMessage) {
        completableFuture.complete(responseMessage);
      }

      @Override
      public void onError(final Throwable e) {
        completableFuture.completeExceptionally(e);
      }

      @Override
      public void onCompleted() {
      }
    };
  }

  @Override
  public void close() {
    channelToMaster.shutdown();
  }
}
