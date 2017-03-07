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
package edu.snu.vortex.runtime.common.example;

import edu.snu.vortex.compiler.backend.vortex.OperatorConverter;
import edu.snu.vortex.compiler.ir.Attributes;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.DAGBuilder;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.operator.Do;
import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.compiler.ir.operator.Source;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.runtime.common.*;
import edu.snu.vortex.runtime.exception.NoSuchRtStageException;

import java.util.*;

/**
 * Execution Plan generation.
 */
public final class ExecutionPlanGeneration {
  private ExecutionPlanGeneration() {
  }

  public static void main(final String[] args) throws Exception {

    final DAG dag = buildMapReduceIRDAG();

    final Optimizer dagOptimizer = new Optimizer();
    dagOptimizer.optimize(dag, Optimizer.PolicyType.Pado);
    System.out.println("=== Optimized IR DAG ===");
    System.out.println(dag);

    ExecutionPlan execPlan = transformToExecDAG(dag);
    System.out.println("=== Execution Plan ===");
    System.out.println(execPlan);
  }

  private static ExecutionPlan transformToExecDAG(final DAG dag) {
    final ExecutionPlan execPlan = new ExecutionPlan();
    final OperatorConverter compiler = new OperatorConverter();

    final List<RtStage> rtStageList = new ArrayList<>();
    final Map<String, RtOperator> rtOperatorMap = new HashMap<>();

    final List<Operator> topoSorted = new LinkedList<>();
    dag.doTopological(operator -> topoSorted.add(operator));

    RtStage rtStage = null;
    for (int idx = 0; idx < topoSorted.size(); idx++) {
      final Operator operator = topoSorted.get(idx);
      final RtOperator rtOperator = compiler.convert(operator);
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
          while (edges.hasNext()) {
            final Edge edge = edges.next();

            String srcROperId = compiler.convertId(edge.getSrc().getId());
            RtStage srcRtStage = findRtStageOf(rtStageList, srcROperId);
            RtOperator srcROper = srcRtStage.getRtOpById(srcROperId);

            String dstROperId = compiler.convertId(edge.getDst().getId());
            RtStage dstRtStage = findRtStageOf(rtStageList, dstROperId);
            RtOperator dstROper = dstRtStage.getRtOpById(dstROperId);

            Map<RtAttributes.RtOpLinkAttribute, Object> rOpLinkAttr = new HashMap<>();
            rOpLinkAttr.put(RtAttributes.RtOpLinkAttribute.COMM_PATTERN, convertEdgeTypeToROpLinkAttr(edge.getType()));
            rOpLinkAttr.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.FILE);

            RtOpLink rtOpLink = new RtOpLink(srcROper, dstROper, rOpLinkAttr);
            execPlan.connectRtStages(srcRtStage, dstRtStage, rtOpLink);
          }
        } catch (NoSuchRtStageException e) {
          throw new RuntimeException(e.getMessage());
        }
      } else {
        rtStage.addRtOp(rtOperator);

        Iterator<Edge> edges = inEdges.get().iterator();
        while (edges.hasNext()) {
          Edge edge = edges.next();
          Map<RtAttributes.RtOpLinkAttribute, Object> rOpLinkAttr = new HashMap<>();
          rOpLinkAttr.put(RtAttributes.RtOpLinkAttribute.COMM_PATTERN, convertEdgeTypeToROpLinkAttr(edge.getType()));
          rOpLinkAttr.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.LOCAL_MEM);

          String srcId = compiler.convertId(edge.getSrc().getId());
          RtOpLink rtOpLink = new RtOpLink(rtOperatorMap.get(srcId), rtOperator, rOpLinkAttr);
          rtStage.connectRtOps(srcId, rtOperator.getId(), rtOpLink);
        }
      }
    }

    return execPlan;
  }

  private static RtAttributes.CommPattern convertEdgeTypeToROpLinkAttr(final Edge.Type edgeType) {
    switch (edgeType) {
      case OneToOne:
        return RtAttributes.CommPattern.ONE_TO_ONE;
      case Broadcast:
        return RtAttributes.CommPattern.BROADCAST;
      case ScatterGather:
        return RtAttributes.CommPattern.SCATTER_GATHER;
      default:
        throw new RuntimeException("no such edge type");
    }
  }

  private static RtStage findRtStageOf(final List<RtStage> rtStages, final String operatorId) {
    final Iterator<RtStage> iterator = rtStages.iterator();

    while (iterator.hasNext()) {
      RtStage rtStage = iterator.next();
      if (rtStage.contains(operatorId)) {
        return rtStage;
      }
    }

    return null;
  }

  private static boolean isSource(final Optional<List<Edge>> edges) {
    return (!edges.isPresent());
  }
  private static boolean hasM2M(final List<Edge> edges) {
    return edges.stream().filter(edge -> edge.getType() == Edge.Type.ScatterGather).count() > 0;
  }

  private static DAG buildMapReduceIRDAG() {
    final EmptySource source = new EmptySource();
    final EmptyDo<String, Pair<String, Integer>, Void> map = new EmptyDo<>("MapOperator");
    final EmptyDo<Pair<String, Iterable<Integer>>, String, Void> reduce = new EmptyDo<>("ReduceOperator");

    // Before
    final DAGBuilder builder = new DAGBuilder();
    builder.addOperator(source);
    builder.addOperator(map);
    builder.addOperator(reduce);
    builder.connectOperators(source, map, Edge.Type.OneToOne);
    builder.connectOperators(map, reduce, Edge.Type.ScatterGather);
    return builder.build();
  }

  /**
   * Pair.
   * @param <K> .
   * @param <V> .
   */
  private static class Pair<K, V> {
    private K key;
    private V val;

    Pair(final K key, final V val) {
      this.key = key;
      this.val = val;
    }
  }

  /**
   * Empty Source.
   */
  private static class EmptySource extends Source {
    @Override
    public List<Reader> getReaders(final long desiredBundleSizeBytes) throws Exception {
      return null;
    }
  }

  /**
   * Empty Do.
   * @param <I> .
   * @param <O> .
   * @param <T> .
   */
  private static class EmptyDo<I, O, T> extends Do<I, O, T> {
    private final String name;

    EmptyDo(final String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(super.toString());
      sb.append(", name: ");
      sb.append(name);
      return sb.toString();
    }

    @Override
    public Iterable<O> transform(final Iterable<I> input, final Map<T, Object> broadcasted) {
      return null;
    }
  }

}
