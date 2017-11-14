package edu.snu.onyx.runtime.executor;

import edu.snu.onyx.client.JobConf;
import edu.snu.onyx.runtime.common.grpc.GrpcClient;
import edu.snu.onyx.runtime.common.grpc.GrpcServer;
import edu.snu.onyx.runtime.common.grpc.MasterMetricServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterPartitionServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterRemoteBlockServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterSchedulerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import net.jcip.annotations.ThreadSafe;
import org.apache.reef.tang.annotations.Parameter;

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
  public MasterRPC(final GrpcClient grpcClient,
                    @Parameter(JobConf.ExecutorId.class) final String executorId) {
    try {
      channelToMaster = grpcClient.openChannel(GrpcServer.MASTER_GRPC_SERVER_ID);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Scheduler.
  public MasterSchedulerServiceGrpc.MasterSchedulerServiceBlockingStub getSchedulerBlockingStub() {
    return MasterSchedulerServiceGrpc.newBlockingStub(channelToMaster);
  }

  // Partition.
  public MasterPartitionServiceGrpc.MasterPartitionServiceBlockingStub getPartitionBlockingStub() {
    return MasterPartitionServiceGrpc.newBlockingStub(channelToMaster);
  }
  public MasterPartitionServiceGrpc.MasterPartitionServiceStub getPartitionAsyncStub() {
    return MasterPartitionServiceGrpc.newStub(channelToMaster);
  }

  // RemoteBlock.
  public MasterRemoteBlockServiceGrpc.MasterRemoteBlockServiceBlockingStub getRemoteBlockBlockingStub() {
    return MasterRemoteBlockServiceGrpc.newBlockingStub(channelToMaster);
  }

  // Metrics.
  public MasterMetricServiceGrpc.MasterMetricServiceBlockingStub getMetricBlockingStub() {
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
