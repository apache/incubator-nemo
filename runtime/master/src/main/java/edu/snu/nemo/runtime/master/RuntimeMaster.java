/*
 * Copyright (C) 2018 Seoul National University
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

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.common.exception.*;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.MetricCollectionBarrierVertex;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageContext;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageListener;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.master.resource.ContainerManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import edu.snu.nemo.runtime.master.resource.ResourceSpecification;
import edu.snu.nemo.runtime.master.scheduler.Scheduler;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static edu.snu.nemo.runtime.common.state.TaskState.State.COMPLETE;
import static edu.snu.nemo.runtime.common.state.TaskState.State.ON_HOLD;

/**
 * (WARNING) Use runtimeMasterThread for all public methods to avoid race conditions.
 * See comments in the {@link Scheduler} for avoiding race conditions.
 *
 * Runtime Master is the central controller of Runtime.
 * Compiler submits an {@link PhysicalPlan} to Runtime Master to execute a job.
 * Runtime Master handles:
 *    a) Scheduling the job with {@link Scheduler}.
 *    b) Managing resources with {@link ContainerManager}.
 *    c) Managing blocks with {@link BlockManagerMaster}.
 *    d) Receiving and sending control messages with {@link MessageEnvironment}.
 *    e) Metric using {@link MetricMessageHandler}.
 */
@DriverSide
public final class RuntimeMaster {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMaster.class.getName());
  private static final int DAG_LOGGING_PERIOD = 3000;

  private final ExecutorService runtimeMasterThread;

  private final Scheduler scheduler;
  private final ContainerManager containerManager;
  private final BlockManagerMaster blockManagerMaster;
  private final MetricMessageHandler metricMessageHandler;
  private final MessageEnvironment masterMessageEnvironment;
  private final Map<Integer, Long> aggregatedMetricData;

  // For converting json data. This is a thread safe.
  private final ObjectMapper objectMapper;

  private final String dagDirectory;
  private final Set<IRVertex> irVertices;

  private final AtomicInteger resourceRequestCount;


  @Inject
  public RuntimeMaster(final Scheduler scheduler,
                       final ContainerManager containerManager,
                       final BlockManagerMaster blockManagerMaster,
                       final MetricMessageHandler metricMessageHandler,
                       final MessageEnvironment masterMessageEnvironment,
                       @Parameter(JobConf.DAGDirectory.class) final String dagDirectory) {
    // We would like to use a single thread for runtime master operations
    // since the processing logic in master takes a very short amount of time
    // compared to the job completion times of executed jobs
    // and keeping it single threaded removes the complexity of multi-thread synchronization.
    this.runtimeMasterThread = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "RuntimeMaster"));
    this.scheduler = scheduler;
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
    this.aggregatedMetricData = new HashMap<>();
  }

  /**
   * Submits the {@link PhysicalPlan} to Runtime.
   * @param plan to execute.
   * @param maxScheduleAttempt the max number of times this plan/sub-part of the plan should be attempted.
   */
  public Pair<JobStateManager, ScheduledExecutorService> execute(final PhysicalPlan plan,
                                                                 final int maxScheduleAttempt) {
    final Callable<Pair<JobStateManager, ScheduledExecutorService>> jobExecutionCallable = () -> {
      this.irVertices.addAll(plan.getIdToIRVertex().values());
      try {
        final JobStateManager jobStateManager =
            new JobStateManager(plan, blockManagerMaster, metricMessageHandler, maxScheduleAttempt);
        scheduler.scheduleJob(plan, jobStateManager);
        final ScheduledExecutorService dagLoggingExecutor = scheduleDagLogging(jobStateManager);
        return Pair.of(jobStateManager, dagLoggingExecutor);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    try {
      return runtimeMasterThread.submit(jobExecutionCallable).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void terminate() {
    runtimeMasterThread.execute(() -> {

      scheduler.terminate();
      try {
        masterMessageEnvironment.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      metricMessageHandler.terminate();
      containerManager.terminate();

    });

    // Do not shutdown runtimeMasterThread. We need it to clean things up.
  }

  public void requestContainer(final String resourceSpecificationString) {
    final Future<?> containerRequestEventResult = runtimeMasterThread.submit(() -> {
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
    runtimeMasterThread.execute(() -> {

      containerManager.onContainerAllocated(executorId, allocatedEvaluator, executorConfiguration);

    });
  }

  /**
   * Called when an executor is launched on a container for this runtime.
   * @param activeContext of the launched executor.
   * @return true if all requested executors have been launched, false otherwise.
   */
  public boolean onExecutorLaunched(final ActiveContext activeContext) {
    final Callable<Boolean> processExecutorLaunchedEvent = () -> {
      final Optional<ExecutorRepresenter> executor = containerManager.onContainerLaunched(activeContext);
      if (executor.isPresent()) {
        scheduler.onExecutorAdded(executor.get());
        return (resourceRequestCount.decrementAndGet() == 0);
      } else {
        return false;
      }
    };

    final boolean eventResult;
    try {
      eventResult = runtimeMasterThread.submit(processExecutorLaunchedEvent).get();
    } catch (final Exception e) {
      throw new ContainerException(e);
    }
    return eventResult;
  }

  /**
   * Called when an executor fails due to container failure on this runtime.
   * @param failedEvaluator that failed.
   */
  public void onExecutorFailed(final FailedEvaluator failedEvaluator) {
    runtimeMasterThread.execute(() -> {
      LOG.info("onExecutorFailed: {}", failedEvaluator.getId());

      // Note that getFailedContextList() can be empty if the failure occurred
      // prior to launching an Executor on the Evaluator.
      failedEvaluator.getFailedContextList().forEach(failedContext -> {
        final String failedExecutorId = failedContext.getId();
        scheduler.onExecutorRemoved(failedExecutorId);
      });

      containerManager.onContainerFailed(failedEvaluator.getId());
    });
  }

  /**
   * Handler for control messages received by Master.
   */
  public final class MasterControlMessageReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      runtimeMasterThread.execute(() -> {

        handleControlMessage(message);

      });
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
      case TaskStateChanged:
        final ControlMessage.TaskStateChangedMsg taskStateChangedMsg
            = message.getTaskStateChangedMsg();

        scheduler.onTaskStateReportFromExecutor(taskStateChangedMsg.getExecutorId(),
            taskStateChangedMsg.getTaskId(),
            taskStateChangedMsg.getAttemptIdx(),
            convertTaskState(taskStateChangedMsg.getState()),
            taskStateChangedMsg.getVertexPutOnHoldId(),
            convertFailureCause(taskStateChangedMsg.getFailureCause()));
        break;
      case ExecutorFailed:
        // Executor failed due to user code.
        final ControlMessage.ExecutorFailedMsg executorFailedMsg = message.getExecutorFailedMsg();
        final String failedExecutorId = executorFailedMsg.getExecutorId();
        final Exception exception = SerializationUtils.deserialize(executorFailedMsg.getException().toByteArray());
        LOG.error(failedExecutorId + " failed, Stack Trace: ", exception);
        throw new RuntimeException(exception);
      case DataSizeMetric:
        final ControlMessage.DataSizeMetricMsg dataSizeMetricMsg = message.getDataSizeMetricMsg();
        // TODO #96: Modularize DataSkewPolicy to use MetricVertex and BarrierVertex.
        accumulateBarrierMetric(dataSizeMetricMsg.getPartitionSizeList(),
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
   * TODO #96: Modularize DataSkewPolicy to use MetricVertex and BarrierVertex.
   * TODO #98: Implement MetricVertex that collect metric used for dynamic optimization.
   *
   * @param partitionSizeInfo the size of partitions in a block to accumulate.
   * @param srcVertexId       the ID of the source vertex.
   * @param blockId           the ID of the block.
   */
  private void accumulateBarrierMetric(final List<ControlMessage.PartitionSizeEntry> partitionSizeInfo,
                                       final String srcVertexId,
                                       final String blockId) {
    final IRVertex vertexToSendMetricDataTo = irVertices.stream()
        .filter(irVertex -> irVertex.getId().equals(srcVertexId)).findFirst()
        .orElseThrow(() -> new RuntimeException(srcVertexId + " doesn't exist in the submitted Physical Plan"));

    // For each hash range index, aggregate the metric data as they arrive.
    partitionSizeInfo.forEach(partitionSizeEntry -> {
      final int key = partitionSizeEntry.getKey();
      final long size = partitionSizeEntry.getSize();
      if (aggregatedMetricData.containsKey(key)) {
        aggregatedMetricData.compute(key, (existKey, existValue) -> existValue + size);
      } else {
        aggregatedMetricData.put(key, size);
      }
    });

    if (vertexToSendMetricDataTo instanceof MetricCollectionBarrierVertex) {
      final MetricCollectionBarrierVertex<Integer, Long> metricCollectionBarrierVertex =
          (MetricCollectionBarrierVertex) vertexToSendMetricDataTo;
      metricCollectionBarrierVertex.addBlockId(blockId);
      metricCollectionBarrierVertex.setMetricData(aggregatedMetricData);
    } else {
      throw new RuntimeException("Something wrong happened at DataSkewCompositePass.");
    }
  }

  private static TaskState.State convertTaskState(final ControlMessage.TaskStateFromExecutor state) {
    switch (state) {
      case READY:
        return TaskState.State.READY;
      case EXECUTING:
        return TaskState.State.EXECUTING;
      case COMPLETE:
        return COMPLETE;
      case FAILED_RECOVERABLE:
        return TaskState.State.FAILED_RECOVERABLE;
      case FAILED_UNRECOVERABLE:
        return TaskState.State.FAILED_UNRECOVERABLE;
      case ON_HOLD:
        return ON_HOLD;
      default:
        throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + state));
    }
  }

  private TaskState.RecoverableFailureCause convertFailureCause(
      final ControlMessage.RecoverableFailureCause cause) {
    switch (cause) {
      case InputReadFailure:
        return TaskState.RecoverableFailureCause.INPUT_READ_FAILURE;
      case OutputWriteFailure:
        return TaskState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE;
      default:
        throw new UnknownFailureCauseException(
            new Throwable("The failure cause for the recoverable failure is unknown"));
    }
  }

  /**
   * Schedules a periodic DAG logging thread.
   * @param jobStateManager for the job the DAG should be logged.
   * TODO #20: RESTful APIs to Access Job State and Metric.
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
}
