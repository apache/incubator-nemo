package edu.snu.onyx.runtime.executor;

import edu.snu.onyx.runtime.common.grpc.GrpcClient;
import edu.snu.onyx.runtime.master.grpc.MasterPartitionServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterRemotePartitionServiceGrpc;
import edu.snu.onyx.runtime.master.grpc.MasterSchedulerServiceGrpc;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;

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

  public MasterPartitionServiceGrpc.MasterPartitionServiceBlockingStub getPartitionBlockingStub() {
  }

  public MasterPartitionServiceGrpc.MasterPartitionServiceStub getPartitionAsyncStub() {
  }

  public MasterRemotePartitionServiceGrpc.MasterRemotePartitionServiceBlockingStub getRemoteBlockBlockingStub() {
  }

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
}
