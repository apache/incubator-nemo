package edu.snu.onyx.runtime.executor;

import edu.snu.onyx.client.JobConf;
import edu.snu.onyx.runtime.common.grpc.GrpcUtil;
import edu.snu.onyx.runtime.master.grpc.MasterPartitionServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterRemoteBlockServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterSchedulerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import net.jcip.annotations.ThreadSafe;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Helps making RPCs to the master.
 */
@ThreadSafe
public final class MasterRPC implements Closeable {
  private final ManagedChannel channelToMaster; // thread-safe

  @Inject
  private MasterRPC(final NameResolver nameResolver,
                    final IdentifierFactory idFactory,
                    @Parameter(JobConf.ExecutorId.class) final String executorId) {
    try {
      channelToMaster = GrpcUtil.buildChannel(nameResolver, idFactory, executorId);
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
