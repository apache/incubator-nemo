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
package edu.snu.nemo.runtime.master;

import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.common.exception.*;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.MetricCollectionBarrierVertex;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageContext;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageListener;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.nemo.runtime.common.state.BlockState;
import edu.snu.nemo.runtime.common.state.TaskGroupState;
import edu.snu.nemo.runtime.master.resource.ContainerManager;
import edu.snu.nemo.runtime.master.scheduler.PendingTaskGroupQueue;
import edu.snu.nemo.runtime.master.resource.ResourceSpecification;
import edu.snu.nemo.runtime.master.scheduler.Scheduler;
import edu.snu.nemo.runtime.master.scheduler.SchedulerRunner;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static edu.snu.nemo.runtime.common.state.TaskGroupState.State.COMPLETE;
import static edu.snu.nemo.runtime.common.state.TaskGroupState.State.ON_HOLD;

/**
 * Runtime Master is the central controller of Runtime.
 * Compiler submits an {@link PhysicalPlan} to Runtime Master to execute a job.
 * Runtime Master handles:
 *    a) Scheduling the job with {@link Scheduler}, {@link SchedulerRunner}, {@link PendingTaskGroupQueue}.
 *    b) Managing resources with {@link ContainerManager}.
 *    c) Managing blocks with {@link BlockManagerMaster}.
 *    d) Receiving and sending control messages with {@link MessageEnvironment}.
 *    e) Metric using {@link MetricMessageHandler}.
 */
@DriverSide
public final class RuntimeMaster {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMaster.class.getName());
  private static final int DAG_LOGGING_PERIOD = 3000;

  private final ExecutorService masterControlEventExecutor;

  private final Scheduler scheduler;
  private final SchedulerRunner schedulerRunner;
  private final PendingTaskGroupQueue pendingTaskGroupQueue;
  private final ContainerManager containerManager;
  private final BlockManagerMaster blockManagerMaster;
  private final MetricMessageHandler metricMessageHandler;
  private final MessageEnvironment masterMessageEnvironment;

  // For converting json data. This is a thread safe.
  private final ObjectMapper objectMapper;

  private final String dagDirectory;
  private final Set<IRVertex> irVertices;

  private final AtomicInteger resourceRequestCount;


  @Inject
  public RuntimeMaster(final Scheduler scheduler,
                       final SchedulerRunner schedulerRunner,
                       final PendingTaskGroupQueue pendingTaskGroupQueue,
                       final ContainerManager containerManager,
                       final BlockManagerMaster blockManagerMaster,
                       final MetricMessageHandler metricMessageHandler,
                       final MessageEnvironment masterMessageEnvironment,
                       @Parameter(JobConf.DAGDirectory.class) final String dagDirectory) {
    // We would like to keep the master event thread pool single threaded
    // since the processing logic in master takes a very short amount of time
    // compared to the job completion times of executed jobs
    // and keeping it single threaded removes the complexity of multi-thread synchronization.
    this.masterControlEventExecutor = Executors.newSingleThreadExecutor();
    this.scheduler = scheduler;
    this.schedulerRunner = schedulerRunner;
    this.pendingTaskGroupQueue = pendingTaskGroupQueue;
    this.containerManager = containerManager;
    this.blockManagerMaster = blockManagerMaster;
    this.metricMessageHandler = metricMessageHandler;
    this.masterMessageEnvironment = masterMessageEnvironment;
    this.masterMessageEnvironment
        .setupListener(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID, new MasterControlMessageReceiver());
    this.dagDirectory = dagDirectory;
    this.irVertices = new HashSet<>();
    this.resourceRequestCount = new AtomicInteger(0);
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
          new JobStateManager(plan, blockManagerMaster, metricMessageHandler, maxScheduleAttempt);

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
      masterControlEventExecutor.shutdown();

      scheduler.terminate();
      schedulerRunner.terminate();
      pendingTaskGroupQueue.close();
      masterMessageEnvironment.close();
      metricMessageHandler.terminate();

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
    final Future<?> containerRequestEventResult = masterControlEventExecutor.submit(() -> {
      try {
        final TreeNode jsonRootNode = objectMapper.readTree(resourceSpecificationString);

        for (int i = 0; i < jsonRootNode.size(); i++) {
          final TreeNode resourceNode = jsonRootNode.get(i);
          final ResourceSpecification.Builder builder = ResourceSpecification.newBuilder();
          builder.setContainerType(resourceNode.get("type").traverse().nextTextValue());
          builder.setMemory(resourceNode.get("memory_mb").traverse().getIntValue());
          builder.setCapacity(resourceNode.get("capacity").traverse().getIntValue());
          final int executorNum = resourceNode.path("num").traverse().nextIntValue(1);
          resourceRequestCount.getAndAdd(executorNum);
          containerManager.requestContainer(executorNum, builder.build());
        }
      } catch (final Exception e) {
        throw new ContainerException(e);
      }
    });
    try {
      containerRequestEventResult.get();
    } catch (final Exception e) {
      LOG.error("Exception while requesting for a container: ", e);
      throw new ContainerException(e);
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
    masterControlEventExecutor.execute(() ->
      containerManager.onContainerAllocated(executorId, allocatedEvaluator, executorConfiguration));
  }

  /**
   * Called when an executor is launched on a container for this runtime.
   * @param activeContext of the launched executor.
   * @return true if all requested executors have been launched, false otherwise.
   */
  public boolean onExecutorLaunched(final ActiveContext activeContext) {
    final Callable<Boolean> processExecutorLaunchedEvent = () -> {
      containerManager.onExecutorLaunched(activeContext);
      scheduler.onExecutorAdded(activeContext.getId());
      return (resourceRequestCount.decrementAndGet() == 0);
    };

    final boolean eventResult;
    try {
      eventResult = masterControlEventExecutor.submit(processExecutorLaunchedEvent).get();
    } catch (final Exception e) {
      throw new ContainerException(e);
    }
    return eventResult;
  }

  /**
   * Called when an executor fails due to container failure on this runtime.
   * @param failedExecutorId of the failed executor.
   */
  public void onExecutorFailed(final String failedExecutorId) {
    masterControlEventExecutor.execute(() -> {
      containerManager.onExecutorRemoved(failedExecutorId);
      scheduler.onExecutorRemoved(failedExecutorId);
    });
  }

  /**
   * Handler for control messages received by Master.
   */
  public final class MasterControlMessageReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      masterControlEventExecutor.execute(() -> handleControlMessage(message));
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
      default:
        throw new IllegalMessageException(
            new Exception("This message should not be requested to Master :" + message.getType()));
      }
    }
  }

  private void handleControlMessage(final ControlMessage.Message message) {
    switch (message.getType()) {
    case TaskGroupStateChanged:
      final ControlMessage.TaskGroupStateChangedMsg taskGroupStateChangedMsg
          = message.getTaskGroupStateChangedMsg();

      scheduler.onTaskGroupStateChanged(taskGroupStateChangedMsg.getExecutorId(),
          taskGroupStateChangedMsg.getTaskGroupId(),
          convertTaskGroupState(taskGroupStateChangedMsg.getState()),
          taskGroupStateChangedMsg.getAttemptIdx(),
          taskGroupStateChangedMsg.getTaskPutOnHoldId(),
          convertFailureCause(taskGroupStateChangedMsg.getFailureCause()));
      break;
    case ExecutorFailed:
      final ControlMessage.ExecutorFailedMsg executorFailedMsg = message.getExecutorFailedMsg();
      final String failedExecutorId = executorFailedMsg.getExecutorId();
      final Exception exception = SerializationUtils.deserialize(executorFailedMsg.getException().toByteArray());
      LOG.error(failedExecutorId + " failed, Stack Trace: ", exception);
      containerManager.onExecutorRemoved(failedExecutorId);
      throw new RuntimeException(exception);
    case ContainerFailed:
      final ControlMessage.ContainerFailedMsg containerFailedMsg = message.getContainerFailedMsg();
      LOG.error(containerFailedMsg.getExecutorId() + " failed");
      break;
    case DataSizeMetric:
      final ControlMessage.DataSizeMetricMsg dataSizeMetricMsg = message.getDataSizeMetricMsg();
      // TODO #511: Refactor metric aggregation for (general) run-rime optimization.
      accumulateBarrierMetric(dataSizeMetricMsg.getPartitionSizeInfoList(),
          dataSizeMetricMsg.getSrcIRVertexId(), dataSizeMetricMsg.getBlockId());
      break;
    case MetricMessageReceived:
      final List<ControlMessage.Metric> metricList = message.getMetricMsg().getMetricList();
      metricList.forEach(metric ->
          metricMessageHandler.onMetricMessageReceived(metric.getMetricKey(), metric.getMetricValue()));
      break;
    default:
      throw new IllegalMessageException(
          new Exception("This message should not be received by Master :" + message.getType()));
    }
  }


  /**
   * Accumulates the metric data for a barrier vertex.
   * TODO #511: Refactor metric aggregation for (general) run-rime optimization.
   * TODO #513: Replace MetricCollectionBarrierVertex with a Customizable IRVertex.
   *
   * @param blockSizeInfo the block size info to accumulate.
   * @param srcVertexId   the ID of the source vertex.
   * @param blockId       the ID of the block.
   */
  public void accumulateBarrierMetric(final List<Long> blockSizeInfo,
                                      final String srcVertexId,
                                      final String blockId) {
    final IRVertex vertexToSendMetricDataTo = irVertices.stream()
        .filter(irVertex -> irVertex.getId().equals(srcVertexId)).findFirst()
        .orElseThrow(() -> new RuntimeException(srcVertexId + " doesn't exist in the submitted Physical Plan"));

    if (vertexToSendMetricDataTo instanceof MetricCollectionBarrierVertex) {
      final MetricCollectionBarrierVertex<Long> metricCollectionBarrierVertex =
          (MetricCollectionBarrierVertex) vertexToSendMetricDataTo;
      metricCollectionBarrierVertex.accumulateMetric(blockId, blockSizeInfo);
    } else {
      throw new RuntimeException("Something wrong happened at DataSkewCompositePass.");
    }
  }

  // TODO #164: Cleanup Protobuf Usage
  private static TaskGroupState.State convertTaskGroupState(final ControlMessage.TaskGroupStateFromExecutor state) {
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

  public static BlockState.State convertBlockState(final ControlMessage.BlockStateFromExecutor state) {
    switch (state) {
    case BLOCK_READY:
      return BlockState.State.READY;
    case SCHEDULED:
      return BlockState.State.SCHEDULED;
    case COMMITTED:
      return BlockState.State.COMMITTED;
    case LOST_BEFORE_COMMIT:
      return BlockState.State.LOST_BEFORE_COMMIT;
    case LOST:
      return BlockState.State.LOST;
    case REMOVED:
      return BlockState.State.REMOVED;
    default:
      throw new UnknownExecutionStateException(new Exception("This BlockState is unknown: " + state));
    }
  }

  public static ControlMessage.BlockStateFromExecutor convertBlockState(final BlockState.State state) {
    switch (state) {
      case READY:
        return ControlMessage.BlockStateFromExecutor.BLOCK_READY;
      case SCHEDULED:
        return ControlMessage.BlockStateFromExecutor.SCHEDULED;
      case COMMITTED:
        return ControlMessage.BlockStateFromExecutor.COMMITTED;
      case LOST_BEFORE_COMMIT:
        return ControlMessage.BlockStateFromExecutor.LOST_BEFORE_COMMIT;
      case LOST:
        return ControlMessage.BlockStateFromExecutor.LOST;
      case REMOVED:
        return ControlMessage.BlockStateFromExecutor.REMOVED;
      default:
        throw new UnknownExecutionStateException(new Exception("This BlockState is unknown: " + state));
    }
  }

  private TaskGroupState.RecoverableFailureCause convertFailureCause(
      final ControlMessage.RecoverableFailureCause cause) {
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
