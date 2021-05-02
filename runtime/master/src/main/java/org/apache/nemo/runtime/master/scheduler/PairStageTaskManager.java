package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.Stage;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.utility.ConditionalRouterVertex;
import org.apache.nemo.common.ir.vertex.utility.StateMergerVertex;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.nemo.common.Task.TaskType.TransientTask;
import static org.apache.nemo.common.Task.TaskType.VMTask;

public final class PairStageTaskManager {

  private static final Logger LOG = LoggerFactory.getLogger(PairStageTaskManager.class.getName());

  // pair<taskId,edgeId>
  private final Map<String, Pair<String, String>> pairTaskEdgeMap = new ConcurrentHashMap<>();
  private final Map<String, String> taskCrTaskMap = new ConcurrentHashMap<>();
  private DAG<Stage, StageEdge> stageDag;

  @Inject
  private PairStageTaskManager() {

  }

  public void registerStageDag(final DAG<Stage, StageEdge> stageDag) {
    this.stageDag = stageDag;
  }

  public Pair<String, String> getPairTaskEdgeId(final String taskId) {
    return pairTaskEdgeMap.get(taskId);
  }

  public String getPairStageId(final String stageId) {
    final String stageTaskId = pairTaskEdgeMap.keySet()
      .stream()
      .filter(taskId -> RuntimeIdManager.getStageIdFromTaskId(taskId).equals(stageId))
      .findFirst()
      .get();

    return RuntimeIdManager.getStageIdFromTaskId(pairTaskEdgeMap.get(stageTaskId).left());
  }

  public Task.TaskType registerPairTask(final List<StageEdge> taskIncomingEdges,
                                        final List<StageEdge> taskOutgoingEdges,
                                        final String taskId,
                                        final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag) {

    if (stageDag == null) {
      throw new RuntimeException("Stage DAG is null...");
    }

    final boolean crTask = isCrTask(irDag);
    final boolean smTask = isStateMergerTask(irDag);
    final boolean transientTask = isTransientTask(taskIncomingEdges, irDag);
    final int index = RuntimeIdManager.getIndexFromTaskId(taskId);

    if (isStreamTask(irDag)) {
      return Task.TaskType.StreamTask;
    }
    if (smTask) {
      return Task.TaskType.MergerTask;
    }if (crTask) {
      return Task.TaskType.CRTask;
      // find output edge with PartialRR tag
      /*
      final Optional<String> pairTask = taskOutgoingEdges.stream().filter(edge ->
        edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent()
          && edge.getPropertyValue(AdditionalOutputTagProperty.class).get().equals(Util.TRANSIENT_PATH))
        .map(edge ->
          RuntimeIdManager.generateTaskId(edge.getDst().getId(), index, 0))
        .findFirst();

      pairTask.ifPresent(pairTaskId -> {
        LOG.info("Registering pair task 111 {} <-> {}", taskId, pairTaskId);
        pairTaskEdgeMap.put(taskId, pairTaskId);
        pairTaskEdgeMap.put(pairTaskId, taskId);
      });
      */
    } else if (transientTask) {
      final Pair<String, String> pairTaskEdgeId =
        getPairTaskEdge(taskIncomingEdges, taskId, Task.TaskType.TransientTask);
      final String crTaskId = getCrTaskId(taskIncomingEdges, taskId, Task.TaskType.TransientTask);
      LOG.info("Registering pair task 222 {} <-> {}", taskId, pairTaskEdgeId);
      pairTaskEdgeMap.put(taskId, pairTaskEdgeId);
      taskCrTaskMap.put(taskId, crTaskId);
      return Task.TaskType.TransientTask;
    } else {
      final Pair<String, String> pairTaskEdgeId = getPairTaskEdge(taskIncomingEdges, taskId, VMTask);

      if (pairTaskEdgeId == null) {
        return Task.TaskType.DefaultTask;
      } else {
        LOG.info("Registering pair task 444 {} <-> {}", taskId, pairTaskEdgeId);
        pairTaskEdgeMap.put(taskId, pairTaskEdgeId);
        final String crTaskId = getCrTaskId(taskIncomingEdges, taskId, Task.TaskType.VMTask);
        taskCrTaskMap.put(taskId, crTaskId);
        return VMTask;
      }
    }
  }

  private String getCrTaskId(final List<StageEdge> taskIncomingEdges,
                            final String taskId,
                            final Task.TaskType currTaskType) {
    final int index = RuntimeIdManager.getIndexFromTaskId(taskId);

    if (taskIncomingEdges.isEmpty()) {
      return null;
    }


    final Stage srcStage = taskIncomingEdges.stream().findFirst().get().getSrc();

    if (currTaskType.equals(TransientTask) || currTaskType.equals(VMTask)) {
      return RuntimeIdManager.generateTaskId(srcStage.getId(), index, 0);
    } else {
      return null;
    }
  }

  private Pair<String, String> getPairTaskEdge(final List<StageEdge> taskIncomingEdges,
                                               final String taskId,
                                               final Task.TaskType currTaskType) {
    final int index = RuntimeIdManager.getIndexFromTaskId(taskId);

    if (taskIncomingEdges.isEmpty()) {
      return null;
    }

    final Stage srcStage = taskIncomingEdges.stream().findFirst().get().getSrc();

    if (currTaskType.equals(Task.TaskType.TransientTask)) {
      // find vm task
      return stageDag.getOutgoingEdgesOf(srcStage)
        .stream().filter(stageEdge -> !stageEdge.isTransientPath())
        .map(stageEdge ->
          Pair.of(RuntimeIdManager.generateTaskId(stageEdge.getDst().getId(), index, 0),
            stageEdge.getId()))
        .findFirst().get();
    } else if (currTaskType.equals(VMTask)) {
      // find transient task
      return stageDag.getOutgoingEdgesOf(srcStage)
        .stream().filter(stageEdge -> stageEdge.isTransientPath())
        .map(stageEdge ->
          Pair.of(RuntimeIdManager.generateTaskId(stageEdge.getDst().getId(), index, 0),
            stageEdge.getId()))
        .findFirst().orElse(null);
    } else {
      return null;
    }
  }

  public static boolean isStreamTask(final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag) {
    return
      irDag.getRootVertices().stream().anyMatch(vertex -> vertex instanceof StreamVertex);
  }

  public static boolean isCrTask(final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag) {
    return
      irDag.getRootVertices().stream().anyMatch(vertex -> vertex instanceof ConditionalRouterVertex);
  }

  public static boolean isStateMergerTask(final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag) {
    return
      irDag.getRootVertices().stream().anyMatch(vertex -> vertex instanceof StateMergerVertex);
  }

  public static boolean isTransientTask(final List<StageEdge> taskIncomingEdges,
                                        final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag) {
    final boolean hasTransientIncomingEdge = taskIncomingEdges.stream().anyMatch(edge ->
      edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.TransientOneToOne) ||
        edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.TransientRR) ||
        edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.TransientShuffle));
    return hasTransientIncomingEdge;
  }
}
