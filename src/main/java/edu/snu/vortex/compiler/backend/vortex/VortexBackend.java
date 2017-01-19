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
package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.ir.Attributes;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.runtime.common.*;
import edu.snu.vortex.runtime.exception.NoSuchRtStageException;

import java.util.*;

public final class VortexBackend implements Backend {
  public ExecutionPlan compile(final DAG dag) {
    final ExecutionPlan execPlan = new ExecutionPlan();
    final OperatorConverter converter = new OperatorConverter();

    final List<RtStage> rtStageList = new ArrayList<>();
    final Map<String, RtOperator> rtOperatorMap = new HashMap<>();

    final List<Operator> topoSorted = new LinkedList<>();
    dag.doDFS((operator -> topoSorted.add(0, operator)), DAG.VisitOrder.PostOrder);

    RtStage rtStage = null;

    for (int idx = 0; idx < topoSorted.size(); idx++) {
      final Operator operator = topoSorted.get(idx);
      final RtOperator rtOperator = converter.convert(operator);
      rtOperatorMap.put(rtOperator.getId(), rtOperator);

      final Optional<List<Edge>> inEdges = dag.getInEdgesOf(operator);
      if (isSource(inEdges)) { // in case of a source operator
        final Object parallelism = operator.getAttrByKey(Attributes.Key.Parallelism);
        Map<RtAttributes.RtStageAttribute, Object> rStageAttr = new HashMap<>();
        rStageAttr.put(RtAttributes.RtStageAttribute.PARALLELISM, parallelism);

        rtStage = new RtStage(rStageAttr);
        rtStage.addRtOp(rtOperator);
        execPlan.addRtStage(rtStage);

        rtStageList.add(rtStage);

      } else if (hasM2M(inEdges.get())) {
        final Object parallelism = operator.getAttrByKey(Attributes.Key.Parallelism);
        Map<RtAttributes.RtStageAttribute, Object> rStageAttr = new HashMap<>();
        rStageAttr.put(RtAttributes.RtStageAttribute.PARALLELISM, parallelism);

        rtStage = new RtStage(rStageAttr);
        rtStage.addRtOp(rtOperator);
        execPlan.addRtStage(rtStage);

        rtStageList.add(rtStage);

        Iterator<Edge> edges = inEdges.get().iterator();
        try {
          while(edges.hasNext()) {
            final Edge edge = edges.next();

            String srcROperId = converter.convertId(edge.getSrc().getId());
            RtStage srcRtStage = findRtStageOf(rtStageList, srcROperId);
            RtOperator srcROper = srcRtStage.getRtOpById(srcROperId);

            String dstROperId = converter.convertId(edge.getDst().getId());
            RtStage dstRtStage = findRtStageOf(rtStageList, dstROperId);
            RtOperator dstROper = dstRtStage.getRtOpById(dstROperId);

            Map<RtAttributes.RtOpLinkAttribute, Object> rOpLinkAttr = new HashMap<>();
            rOpLinkAttr.put(RtAttributes.RtOpLinkAttribute.COMM_PATTERN, convertEdgeTypeToROpLinkAttr(edge.getType()));
            rOpLinkAttr.put(RtAttributes.RtOpLinkAttribute.CHANNEL, convert((Attributes.EdgeChannel)edge.getAttr(Attributes.Key.EdgeChannel)));

            RtOpLink rtOpLink = new RtOpLink(srcROper, dstROper, rOpLinkAttr);
            execPlan.connectRtStages(srcRtStage, dstRtStage, rtOpLink);
          }
        } catch (NoSuchRtStageException e) {
          throw new RuntimeException(e.getMessage());
        }
      }
      else {
        rtStage.addRtOp(rtOperator);

        Iterator<Edge> edges = inEdges.get().iterator();
        while(edges.hasNext()) {
          Edge edge = edges.next();
          Map<RtAttributes.RtOpLinkAttribute, Object> rOpLinkAttr = new HashMap<>();
          rOpLinkAttr.put(RtAttributes.RtOpLinkAttribute.COMM_PATTERN, convertEdgeTypeToROpLinkAttr(edge.getType()));
          rOpLinkAttr.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.LOCAL_MEM);

          String srcId = converter.convertId(edge.getSrc().getId());
          RtOpLink rtOpLink = new RtOpLink(rtOperatorMap.get(srcId), rtOperator, rOpLinkAttr);
          rtStage.connectRtOps(srcId, rtOperator.getId(), rtOpLink);
        }
      }
    }

    return execPlan;
  }

  /*
  private void printStages(final DAG dag) {
    final Set<Operator> operators = new HashSet<>();
    dag.doDFS(operators::add, DAG.VisitOrder.PostOrder);

    final Set<Operator> printed = new HashSet<>();
    operators.stream()
        .filter(operator -> !printed.contains(operator))
        .forEach(operator -> {
          final Set<Operator> stage = new HashSet<>();
          getFifoQueueNeighbors(dag, operator, stage);
          System.out.println(stage);
          printed.addAll(stage);
        });
  }

  private void getFifoQueueNeighbors(final DAG dag, final Operator operator, final Set<Operator> stage) {
    stage.add(operator);
    final Optional<List<Edge>> inEdges = dag.getInEdgesOf(operator);
    if (inEdges.isPresent()) {
      inEdges.get().stream()
          .filter(edge -> edge.getAttr(Attributes.Key.EdgeChannel) == Attributes.EdgeChannel.Memory)
          .map(Edge::getSrc)
          .filter(src -> !stage.contains(src))
          .forEach(src -> getFifoQueueNeighbors(dag, src, stage));
    }
    final Optional<List<Edge>> outEdges = dag.getOutEdgesOf(operator);
    if (outEdges.isPresent()) {
      outEdges.get().stream()
          .filter(edge -> edge.getAttr(Attributes.Key.EdgeChannel) == Attributes.EdgeChannel.Memory)
          .map(Edge::getDst)
          .filter(dst -> !stage.contains(dst))
          .forEach(dst -> getFifoQueueNeighbors(dag, dst, stage));
    }
  }
  */


  private static RtAttributes.CommPattern convertEdgeTypeToROpLinkAttr(Edge.Type edgeType) {
    switch (edgeType) {
      case O2O:
        return RtAttributes.CommPattern.ONE_TO_ONE;
      case O2M:
        return RtAttributes.CommPattern.BROADCAST;
      case M2M:
        return RtAttributes.CommPattern.SCATTER_GATHER;
      default:
        throw new RuntimeException("no such edge type");
    }
  }

  private static RtAttributes.Channel convert(Attributes.EdgeChannel channel) {
    switch (channel) {
      case File:
        return RtAttributes.Channel.FILE;
      case DistributedStorage:
        return RtAttributes.Channel.DISTR_STORAGE;
      case TCPPipe:
        return RtAttributes.Channel.TCP;
      case Memory:
        return RtAttributes.Channel.LOCAL_MEM;
      default:
        throw new RuntimeException("no such edge type");
    }
  }

  private static RtStage findRtStageOf(List<RtStage> rtStages, String operatorId) {
    Iterator<RtStage> iterator = rtStages.iterator();

    while (iterator.hasNext()) {
      RtStage rtStage = iterator.next();
      if (rtStage.contains(operatorId))
        return rtStage;
    }

    return null;
  }

  private static boolean isSource(final Optional<List<Edge>> edges) {
    return (!edges.isPresent());
  }
  private static boolean hasM2M(final List<Edge> edges) {
    return edges.stream().filter(edge -> edge.getType() == Edge.Type.M2M).count() > 0;
  }

}
