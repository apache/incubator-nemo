package edu.snu.onyx.runtime.executor;

import edu.snu.onyx.runtime.common.grpc.GrpcClient;
import edu.snu.onyx.runtime.common.grpc.GrpcServer;
import edu.snu.onyx.runtime.common.grpc.MasterMetricMessageServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterPartitionMessageServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterRemoteBlockMessageServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterSchedulerMessageServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.inject.Inject;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Helps making RPCs to the master.
 */
@ThreadSafe
@EvaluatorSide
public final class RpcToMaster implements Closeable {
  private final ManagedChannel channelToMaster; // thread-safe

  @Inject
  public RpcToMaster(final GrpcClient grpcClient) {
    try {
      channelToMaster = grpcClient.openChannel(GrpcServer.MASTER_GRPC_SERVER_ID);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Scheduler.
  public MasterSchedulerMessageServiceGrpc.MasterSchedulerMessageServiceBlockingStub newSchedulerSyncStub() {
    return MasterSchedulerMessageServiceGrpc.newBlockingStub(channelToMaster);
  }

  // Partition.
  public MasterPartitionMessageServiceGrpc.MasterPartitionMessageServiceBlockingStub newPartitionSyncStub() {
    return MasterPartitionMessageServiceGrpc.newBlockingStub(channelToMaster);
  }
  public MasterPartitionMessageServiceGrpc.MasterPartitionMessageServiceStub newPartitionAsyncStub() {
    return MasterPartitionMessageServiceGrpc.newStub(channelToMaster);
  }

  // RemoteBlock.
  public MasterRemoteBlockMessageServiceGrpc.MasterRemoteBlockMessageServiceBlockingStub newRemoteBlockSyncStub() {
    return MasterRemoteBlockMessageServiceGrpc.newBlockingStub(channelToMaster);
  }

  // MetricsMessage.
  public MasterMetricMessageServiceGrpc.MasterMetricMessageServiceBlockingStub newMetricSyncStub() {
    return MasterMetricMessageServiceGrpc.newBlockingStub(channelToMaster);
  }

  // Utility methods.
  /**
   * Creates an observer that completes the completableFuture on response/error.
   * Use this only when the observer#onNext is expected to be called exactly once.
   * Otherwise, completableFuture might never complete.
   * @param completableFuture to create an observer for.
   * @param <ResponseType> of the observer.
   * @return an observer for the completableFuture.
   */
  public <ResponseType> StreamObserver<ResponseType> createObserverForCompletableFuture(
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
        // Ignore, since onNext() is expected to be called exactly once.
      }
    };
  }

  @Override
  public void close() {
    channelToMaster.shutdown();
  }
}
