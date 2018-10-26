package org.apache.nemo.runtime.master.scheduler;

import com.google.common.collect.Lists;
import org.apache.nemo.common.exception.UnknownExecutionStateException;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.*;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A simple scheduler for streaming workloads.
 * - Keeps track of new executors
 * - Schedules all tasks in a reverse topological order.
 * - Crashes the system upon any other events (should be fixed in the future)
 * - Never stops running.
 */
@DriverSide
@NotThreadSafe
public final class StreamingScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingScheduler.class.getName());

  private final TaskDispatcher taskDispatcher;
  private final PendingTaskCollectionPointer pendingTaskCollectionPointer;
  private final ExecutorRegistry executorRegistry;
  private final PlanStateManager planStateManager;

  StreamingScheduler(final TaskDispatcher taskDispatcher,
                     final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                     final ExecutorRegistry executorRegistry,
                     final PlanStateManager planStateManager) {
    this.taskDispatcher = taskDispatcher;
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.executorRegistry = executorRegistry;
    this.planStateManager = planStateManager;
  }

  @Override
  public void schedulePlan(final PhysicalPlan submittedPhysicalPlan,
                           final int maxScheduleAttempt) {
    // Housekeeping stuff
    taskDispatcher.run();
    planStateManager.updatePlan(submittedPhysicalPlan, maxScheduleAttempt);
    planStateManager.storeJSON("submitted");

    // Prepare tasks
    final List<Stage> reverseTopoStages = Lists.reverse(submittedPhysicalPlan.getStageDAG().getTopologicalSort());
    final List<Task> reverseTopoTasks = reverseTopoStages.stream().flatMap(stageToSchedule -> {
      // Helper variables for this stage
      final List<StageEdge> stageIncomingEdges =
        submittedPhysicalPlan.getStageDAG().getIncomingEdgesOf(stageToSchedule.getId());
      final List<StageEdge> stageOutgoingEdges =
        submittedPhysicalPlan.getStageDAG().getOutgoingEdgesOf(stageToSchedule.getId());
      final List<Map<String, Readable>> vertexIdToReadables = stageToSchedule.getVertexIdToReadables();
      final List<String> taskIdsToSchedule = planStateManager.getTaskAttemptsToSchedule(stageToSchedule.getId());

      // Create tasks of this stage
      return taskIdsToSchedule.stream().map(taskId -> new Task(
        submittedPhysicalPlan.getPlanId(),
        taskId,
        stageToSchedule.getExecutionProperties(),
        stageToSchedule.getSerializedIRDAG(),
        stageIncomingEdges,
        stageOutgoingEdges,
        vertexIdToReadables.get(RuntimeIdManager.getIndexFromTaskId(taskId))));
    }).collect(Collectors.toList());

    // Schedule everything at once
    pendingTaskCollectionPointer.setToOverwrite(reverseTopoTasks);
  }

  @Override
  public void updatePlan(final PhysicalPlan newPhysicalPlan) {
    // TODO #227: StreamingScheduler Dynamic Optimization
    throw new UnsupportedOperationException();
  }

  @Override
  public void onTaskStateReportFromExecutor(final String executorId,
                                            final String taskId,
                                            final int taskAttemptIndex,
                                            final TaskState.State newState,
                                            @Nullable final String vertexPutOnHold,
                                            final TaskState.RecoverableTaskFailureCause failureCause) {
    switch (newState) {
      case COMPLETE:
      case SHOULD_RETRY:
      case ON_HOLD:
      case FAILED:
        // TODO #226: StreamingScheduler Fault Tolerance
        throw new UnsupportedOperationException();
      case READY:
      case EXECUTING:
        throw new RuntimeException("The states READY/EXECUTING cannot occur at this point");
      default:
        throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + newState));
    }
  }

  @Override
  public void onSpeculativeExecutionCheck() {
    // TODO #228: StreamingScheduler Speculative Execution
    throw new UnsupportedOperationException();
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executorRepresenter) {
    LOG.info("{} added (node: {})", executorRepresenter.getExecutorId(), executorRepresenter.getNodeName());
    executorRegistry.registerExecutor(executorRepresenter);
  }

  @Override
  public void onExecutorRemoved(final String executorId) {
    // TODO #226: StreamingScheduler Fault Tolerance
    throw new UnsupportedOperationException();
  }

  @Override
  public void terminate() {
    this.taskDispatcher.terminate();
    this.executorRegistry.terminate();
  }
}
