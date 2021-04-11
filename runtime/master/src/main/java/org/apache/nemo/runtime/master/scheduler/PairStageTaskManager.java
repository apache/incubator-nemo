package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.Stage;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.utility.ConditionalRouterVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class PairStageTaskManager {

  private static final Logger LOG = LoggerFactory.getLogger(PairStageTaskManager.class.getName());

  private final Map<String, String> pairTaskMap = new ConcurrentHashMap<>();
  private DAG<Stage, StageEdge> stageDag;

  @Inject
  private PairStageTaskManager() {

  }

  public void registerStageDag(final DAG<Stage, StageEdge> stageDag) {
    this.stageDag = stageDag;
  }

  public String getPairTaskId(final String taskId) {
    return pairTaskMap.get(taskId);
  }

  public void registerPairTask(final List<StageEdge> taskIncomingEdges,
                               final List<StageEdge> taskOutgoingEdges,
                               final String taskId,
                               final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag) {

    if (stageDag == null) {
      throw new RuntimeException("Stage DAG is null...");
    }

    final boolean crTask = isCrTask(taskIncomingEdges, irDag);
    final boolean lambdaAffinity = isLambdaAffinity(taskIncomingEdges, irDag);
    final int index = RuntimeIdManager.getIndexFromTaskId(taskId);

    if (crTask) {
      // find output edge with PartialRR tag
      final Optional<String> pairTask = taskOutgoingEdges.stream().filter(edge ->
        edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent()
          && edge.getPropertyValue(AdditionalOutputTagProperty.class).get().equals(Util.TRANSIENT_PATH))
        .map(edge ->
          RuntimeIdManager.generateTaskId(edge.getDst().getId(), index, 0))
        .findFirst();

      pairTask.ifPresent(pairTaskId -> {
        LOG.info("Registering pair task 111 {} <-> {}", taskId, pairTaskId);
        pairTaskMap.put(taskId, pairTaskId);
        pairTaskMap.put(pairTaskId, taskId);
      });
    } else if (lambdaAffinity) {
      // find incoming edge
      // we should find outgoing edge of source stage
      final Stage srcStage = taskIncomingEdges.stream().findFirst().get().getSrc();
      if (srcStage.getId().equals("Stage0")) {
        // source stage
        stageDag.getOutgoingEdgesOf(srcStage)
          .stream().filter(stageEdge -> !stageEdge
          .getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
          .map(stageEdge ->
            RuntimeIdManager.generateTaskId(stageEdge.getDst().getId(), index, 0))
          .findFirst()
          .ifPresent(pairTaskId -> {
            LOG.info("Registering pair task 222 {} <-> {}", taskId, pairTaskId);
            pairTaskMap.put(taskId, pairTaskId);
            pairTaskMap.put(pairTaskId, taskId);
          });
      } else {
        // this is a transient path connected with CR vertex
        taskIncomingEdges.stream().findFirst().map(stageEdge ->
          RuntimeIdManager.generateTaskId(stageEdge.getSrc().getId(), index, 0))
          .ifPresent(pairTaskId -> {
            LOG.info("Registering pair task 333 {} <-> {}", taskId, pairTaskId);
            pairTaskMap.put(taskId, pairTaskId);
            pairTaskMap.put(pairTaskId, taskId);
          });
      }
    } else {
      // this is a normal path task connected with source
      // we should find outgoing edge of source stage
      // source stage
      final Stage srcStage = taskIncomingEdges.stream().findFirst().get().getSrc();
      stageDag.getOutgoingEdgesOf(srcStage)
        .stream().filter(stageEdge -> stageEdge
        .getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
        .map(stageEdge ->
          RuntimeIdManager.generateTaskId(stageEdge.getDst().getId(), index, 0))
        .findFirst()
        .ifPresent(pairTaskId -> {
          LOG.info("Registering pair task 444 {} <-> {}", taskId, pairTaskId);
          pairTaskMap.put(taskId, pairTaskId);
          pairTaskMap.put(pairTaskId, taskId);
        });
    }
  }


  public static boolean isCrTask(final List<StageEdge> taskIncomingEdges,
                          final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag) {
    final boolean hasTransientIncomingEdge = taskIncomingEdges.stream().anyMatch(edge ->
      edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.TransientOneToOne) ||
        edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.TransientRR) ||
        edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.TransientShuffle));

    return hasTransientIncomingEdge &&
      irDag.getRootVertices().stream().anyMatch(vertex -> vertex instanceof ConditionalRouterVertex);
  }

  public static boolean isLambdaAffinity(final List<StageEdge> taskIncomingEdges,
                                  final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag) {

    final boolean hasTransientIncomingEdge = taskIncomingEdges.stream().anyMatch(edge ->
      edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.TransientOneToOne) ||
        edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.TransientRR) ||
        edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.TransientShuffle));

    return hasTransientIncomingEdge &&
      irDag.getRootVertices().stream().noneMatch(vertex -> vertex instanceof ConditionalRouterVertex);
  }
}
