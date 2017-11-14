/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.onyx.runtime.master;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.snu.onyx.client.JobConf;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.composite.DataSkewCompositePass;
import edu.snu.onyx.runtime.common.grpc.*;
import edu.snu.onyx.runtime.common.metric.MetricMessageHandler;
import edu.snu.onyx.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.onyx.runtime.common.state.PartitionState;
import edu.snu.onyx.runtime.common.state.TaskGroupState;
import edu.snu.onyx.runtime.exception.ContainerException;
import edu.snu.onyx.runtime.exception.IllegalMessageException;
import edu.snu.onyx.runtime.exception.UnknownExecutionStateException;
import edu.snu.onyx.runtime.exception.UnknownFailureCauseException;
import edu.snu.onyx.runtime.master.grpc.MasterScheduler;
import edu.snu.onyx.runtime.master.grpc.MasterSchedulerServiceGrpc;
import edu.snu.onyx.runtime.master.resource.ContainerManager;
import edu.snu.onyx.runtime.master.scheduler.PendingTaskGroupQueue;
import edu.snu.onyx.runtime.master.resource.ResourceSpecification;
import edu.snu.onyx.runtime.master.scheduler.Scheduler;
import io.grpc.stub.StreamObserver;
import edu.snu.onyx.runtime.master.scheduler.SchedulerRunner;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static edu.snu.onyx.runtime.common.state.TaskGroupState.State.COMPLETE;
import static edu.snu.onyx.runtime.common.state.TaskGroupState.State.ON_HOLD;

/**
 * Runtime Master is the central controller of Runtime.
 * Compiler submits an {@link PhysicalPlan} to Runtime Master to execute a job.
 * Runtime Master handles:
 *    a) Scheduling the job with {@link Scheduler}, {@link SchedulerRunner}, {@link PendingTaskGroupQueue}.
 *    b) Managing resources with {@link ContainerManager}.
 *    c) Managing partitions with {@link PartitionManagerMaster}.
 *    d) Receiving rpcs with {@link GrpcServer}.
 *    e) Metric using {@link MetricMessageHandler}.
 */
@DriverSide
public final class RuntimeMaster {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMaster.class.getName());
  private static final int DAG_LOGGING_PERIOD = 3000;

  private final Scheduler scheduler;
  private final SchedulerRunner schedulerRunner;
  private final PendingTaskGroupQueue pendingTaskGroupQueue;
  private final ContainerManager containerManager;
  private final PartitionManagerMaster partitionManagerMaster;
  private final MetricMessageHandler metricMessageHandler;
  private final GrpcServer grpcServer;

  // For converting json data. This is a thread safe.
  // TODO #420: Create a Singleton ObjectMapper
  private final ObjectMapper objectMapper;

  private final String dagDirectory;
  private final Set<IRVertex> irVertices;

  @Inject
  public RuntimeMaster(final Scheduler scheduler,
                       final SchedulerRunner schedulerRunner,
                       final PendingTaskGroupQueue pendingTaskGroupQueue,
                       final ContainerManager containerManager,
                       final PartitionManagerMaster partitionManagerMaster,
                       final GrpcServer grpcServer,
                       final MetricMessageHandler metricMessageHandler,
                       @Parameter(JobConf.DAGDirectory.class) final String dagDirectory) {
    this.scheduler = scheduler;
    this.schedulerRunner = schedulerRunner;
    this.pendingTaskGroupQueue = pendingTaskGroupQueue;
    this.containerManager = containerManager;
    this.partitionManagerMaster = partitionManagerMaster;
    try {
      grpcServer.start(GrpcUtil.MASTER_GRPC_SERVER_ID,
          new MasterSchedulerService(),
          partitionManagerMaster.new MasterPartitionService(),
          partitionManagerMaster.new MasterRemoteBlockService(),
          new MasterMetricService());
      this.grpcServer = grpcServer;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    this.metricMessageHandler = metricMessageHandler;
    this.dagDirectory = dagDirectory;
    this.irVertices = new HashSet<>();
    this.objectMapper = new ObjectMapper();
  }

  /**
   * Submits the {@link PhysicalPlan} to Runtime.
   * @param plan to execute.
   * @param maxScheduleAttempt the max number of times this plan/sub-part of the plan should be attempted.
   */
  public void execute(final PhysicalPlan plan,
                      final int maxScheduleAttempt) {
    this.irVertices.addAll(plan.getTaskIRVertexMap().values());
    try {
      final JobStateManager jobStateManager =
          new JobStateManager(plan, partitionManagerMaster, metricMessageHandler, maxScheduleAttempt);

      scheduler.scheduleJob(plan, jobStateManager);

      // Schedule dag logging thread
      final ScheduledExecutorService dagLoggingExecutor = scheduleDagLogging(jobStateManager);

      // Wait for the job to finish and stop logging
      jobStateManager.waitUntilFinish();
      dagLoggingExecutor.shutdown();

      jobStateManager.storeJSON(dagDirectory, "final");
      LOG.info("{} is complete!", plan.getId());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void terminate() {
    try {
      scheduler.terminate();
      schedulerRunner.terminate();
      pendingTaskGroupQueue.close();
      partitionManagerMaster.terminate();
      grpcServer.close();
      final Future<Boolean> allExecutorsClosed = containerManager.terminate();

      if (allExecutorsClosed.get()) {
        LOG.info("All executors were closed successfully!");
      } else {
        LOG.error("Failed to shutdown all executors. See log exceptions for details. Terminating RuntimeMaster.");
      }
    } catch (Exception e) {
      new ContainerException(new Throwable("An exception occurred while trying to terminate ContainerManager"));
      e.printStackTrace();
    }
  }

  public void requestContainer(final String resourceSpecificationString) {
    try {
      final TreeNode jsonRootNode = objectMapper.readTree(resourceSpecificationString);

      for (int i = 0; i < jsonRootNode.size(); i++) {
        final TreeNode resourceNode = jsonRootNode.get(i);
        final ResourceSpecification.Builder builder = ResourceSpecification.newBuilder();
        builder.setContainerType(resourceNode.get("type").traverse().nextTextValue());
        builder.setMemory(resourceNode.get("memory_mb").traverse().getIntValue());
        builder.setCapacity(resourceNode.get("capacity").traverse().getIntValue());
        final int executorNum = resourceNode.path("num").traverse().nextIntValue(1);
        containerManager.requestContainer(executorNum, builder.build());
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Called when a container is allocated for this runtime.
   * A wrapper function for {@link ContainerManager}.
   * @param executorId to use for the executor to be launched on this container.
   * @param allocatedEvaluator to be used as the container.
   * @param executorConfiguration to use for the executor to be launched on this container.
   */
  public void onContainerAllocated(final String executorId,
                                  final AllocatedEvaluator allocatedEvaluator,
                                  final Configuration executorConfiguration) {
    containerManager.onContainerAllocated(executorId, allocatedEvaluator, executorConfiguration);
  }

  /**
   * Called when an executor is launched on a container for this runtime.
   * @param activeContext of the launched executor.
   */
  public void onExecutorLaunched(final ActiveContext activeContext) {
    containerManager.onExecutorLaunched(activeContext);
    scheduler.onExecutorAdded(activeContext.getId());
  }

  /**
   * Called when an executor fails due to container failure on this runtime.
   * @param failedExecutorId of the failed executor.
   */
  public void onExecutorFailed(final String failedExecutorId) {
    containerManager.onExecutorRemoved(failedExecutorId);
    scheduler.onExecutorRemoved(failedExecutorId);
  }

  /**
   * Accumulates the metric data for a barrier vertex.
   * TODO #511: Refactor metric aggregation for (general) run-rime optimization.
   * TODO #513: Replace MetricCollectionBarrierVertex with a Customizable IRVertex.
   *
   * @param blockSizeInfo the block size info to accumulate.
   * @param srcVertexId   the ID of the source vertex.
   * @param partitionId   the ID of the partition.
   */
  public void accumulateBarrierMetric(final List<Long> blockSizeInfo,
                                      final String srcVertexId,
                                      final String partitionId) {
    final IRVertex vertexToSendMetricDataTo = irVertices.stream()
        .filter(irVertex -> irVertex.getId().equals(srcVertexId)).findFirst()
        .orElseThrow(() -> new RuntimeException(srcVertexId + " doesn't exist in the submitted Physical Plan"));

    if (vertexToSendMetricDataTo instanceof MetricCollectionBarrierVertex) {
      final MetricCollectionBarrierVertex<Long> metricCollectionBarrierVertex =
          (MetricCollectionBarrierVertex) vertexToSendMetricDataTo;
      metricCollectionBarrierVertex.accumulateMetric(partitionId, blockSizeInfo);
    } else {
      throw new RuntimeException("Something wrong happened at " + DataSkewCompositePass.class.getSimpleName() + ". ");
    }
  }

  /**
   * Grpc master scheduler service.
   */
  private class MasterSchedulerService extends MasterSchedulerServiceGrpc.MasterSchedulerServiceImplBase {
    private final Common.Empty empty = Common.Empty.newBuilder().build();

    @Override
    public void taskGroupStateChanged(final MasterScheduler.NewTaskGroupState newState,
                                      final StreamObserver<Common.Empty> observer) {
      scheduler.onTaskGroupStateChanged(newState.getExecutorId(),
          newState.getTaskGroupId(),
          convertTaskGroupState(newState.getState()),
          newState.getAttemptIdx(),
          newState.getTasksPutOnHoldIdsList(),
          convertFailureCause(newState.getFailureCause()));
      observer.onNext(empty);
      observer.onCompleted();
    }

    @Override
    public void executorFailed(final MasterScheduler.FailedExecutor failedExecutor,
                               final StreamObserver<Common.Empty> observer) {
      final String failedExecutorId = failedExecutor.getExecutorId();
      final Exception exception = SerializationUtils.deserialize(failedExecutor.getException().toByteArray());
      LOG.error(failedExecutorId + " failed, Stack Trace: ", exception);
      containerManager.onExecutorRemoved(failedExecutorId);
      observer.onNext(empty);
      observer.onCompleted();
      throw new RuntimeException(exception);
    }
  }

  /**
   * Grpc master metric service.
   */
  private class MasterMetricService extends MasterMetricServiceGrpc.MasterMetricServiceImplBase {
    private final Common.Empty empty = Common.Empty.newBuilder().build();

    @Override
    public void reportDataSizeMetric(final Metrics.DataSizeMetric metric,
                                     final StreamObserver<Common.Empty> observer) {
      // TODO #511: Refactor metric aggregation for (general) run-rime optimization.
      accumulateBarrierMetric(metric.getBlockSizeInfoList(), metric.getSrcIRVertexId(), metric.getPartitionId());
      observer.onNext(empty);
      observer.onCompleted();
    }

    @Override
    public void reportMetrics(final Metrics.MetricList metricList,
                              final StreamObserver<Common.Empty> observer) {
      metricList.getMetricList().forEach(metric ->
          metricMessageHandler.onMetricMessageReceived(metric.getMetricKey(), metric.getMetricValue()));
      observer.onNext(empty);
      observer.onCompleted();
    }
  }

  // TODO #164: Cleanup Protobuf Usage
  private static TaskGroupState.State convertTaskGroupState(final MasterScheduler.TaskGroupStateFromExecutor state) {
    switch (state) {
      case READY:
        return TaskGroupState.State.READY;
      case EXECUTING:
        return TaskGroupState.State.EXECUTING;
      case COMPLETE:
        return COMPLETE;
      case FAILED_RECOVERABLE:
        return TaskGroupState.State.FAILED_RECOVERABLE;
      case FAILED_UNRECOVERABLE:
        return TaskGroupState.State.FAILED_UNRECOVERABLE;
      case ON_HOLD:
        return ON_HOLD;
      default:
        throw new UnknownExecutionStateException(new Exception("This TaskGroupState is unknown: " + state));
    }
  }

  // TODO #164: Cleanup Protobuf Usage
  public static PartitionState.State convertPartitionState(final Common.PartitionState state) {
    switch (state) {
    case PARTITION_READY:
      return PartitionState.State.READY;
    case SCHEDULED:
      return PartitionState.State.SCHEDULED;
    case COMMITTED:
      return PartitionState.State.COMMITTED;
    case LOST_BEFORE_COMMIT:
      return PartitionState.State.LOST_BEFORE_COMMIT;
    case LOST:
      return PartitionState.State.LOST;
    case REMOVED:
      return PartitionState.State.REMOVED;
    default:
      throw new UnknownExecutionStateException(new Exception("This PartitionState is unknown: " + state));
    }
  }

  // TODO #164: Cleanup Protobuf Usage
  public static Common.PartitionState convertPartitionState(final PartitionState.State state) {
    switch (state) {
      case READY:
        return Common.PartitionState.PARTITION_READY;
      case SCHEDULED:
        return Common.PartitionState.SCHEDULED;
      case COMMITTED:
        return Common.PartitionState.COMMITTED;
      case LOST_BEFORE_COMMIT:
        return Common.PartitionState.LOST_BEFORE_COMMIT;
      case LOST:
        return Common.PartitionState.LOST;
      case REMOVED:
        return Common.PartitionState.REMOVED;
      default:
        throw new UnknownExecutionStateException(new Exception("This PartitionState is unknown: " + state));
    }
  }

  // TODO #164: Cleanup Protobuf Usage
  private TaskGroupState.RecoverableFailureCause convertFailureCause(
      final MasterScheduler.RecoverableFailureCause cause) {
    switch (cause) {
      case InputReadFailure:
        return TaskGroupState.RecoverableFailureCause.INPUT_READ_FAILURE;
      case OutputWriteFailure:
        return TaskGroupState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE;
      default:
        throw new UnknownFailureCauseException(
            new Throwable("The failure cause for the recoverable failure is unknown"));
    }
  }

  /**
   * Schedules a periodic DAG logging thread.
   * TODO #58: Web UI (Real-time visualization)
   * @param jobStateManager for the job the DAG should be logged.
   *
   * @return the scheduled executor service.
   */
  private ScheduledExecutorService scheduleDagLogging(final JobStateManager jobStateManager) {
    final ScheduledExecutorService dagLoggingExecutor = Executors.newSingleThreadScheduledExecutor();
    dagLoggingExecutor.scheduleAtFixedRate(new Runnable() {
      private int dagLogFileIndex = 0;

      public void run() {
        jobStateManager.storeJSON(dagDirectory, String.valueOf(dagLogFileIndex++));
      }
    }, DAG_LOGGING_PERIOD, DAG_LOGGING_PERIOD, TimeUnit.MILLISECONDS);

    return dagLoggingExecutor;
  }

  /**
   * Map function that converts a json string to a java map object.
   */
  final class JsonStringToMapFunction implements Function<String, Map<String, Object>> {
    @Override
    public Map<String, Object> apply(final String s) {
      try {
        return objectMapper.readValue(s, new TypeReference<Map<String, String>>() { });
      } catch (final IOException e) {
        e.printStackTrace();
        throw new IllegalMessageException(
            new Exception("The metric message format is incorrect. It should be in Json format: " + s));
      }
    }
  }
}
