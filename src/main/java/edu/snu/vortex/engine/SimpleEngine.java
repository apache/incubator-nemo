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
package edu.snu.vortex.engine;

import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.operator.*;
import org.apache.beam.sdk.values.KV;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Simply prints out intermediate results
 */
public final class SimpleEngine {

  public void executeDAG(final DAG dag) throws Exception {
    final List<Operator> topoSorted = new LinkedList<>();
    dag.doDFS(node -> topoSorted.add(node));

    final Map<String, List<Iterable>> edgeIdToData = new HashMap<>();
    final Map<String, Object> edgeIdToBroadcast = new HashMap<>();

    for (final Operator node : topoSorted) {
      if (node instanceof Source) {
        final List<Source.Reader> readers = ((Source)node).getReaders(10); // 10 Bytes per Reader
        final List<Iterable> data = new ArrayList<>(readers.size());
        for (final Source.Reader reader : readers) {
          data.add(reader.read());
        }
        dag.getOutEdgesOf(node).get().stream()
            .map(outEdge -> outEdge.getId())
            .forEach(id -> edgeIdToData.put(id, data));
      } else if (node instanceof Do) {
        final Do op = (Do)node;

        // Get Broadcasted SideInputs
        final Map broadcastInput = new HashMap<>();
        dag.getInEdgesOf(node).get().stream()
            .filter(inEdge -> inEdge.getSrc() instanceof Broadcast)
            .forEach(inEdge -> broadcastInput.put(((Broadcast)inEdge.getSrc()).getTag(), edgeIdToBroadcast.get(inEdge.getId())));

        // Get MainInputs
        final List<Iterable> mainInput = dag.getInEdgesOf(node).get().stream()
            .filter(inEdge -> !(inEdge.getSrc() instanceof Broadcast))
            .map(inEdge -> edgeIdToData.get(inEdge.getId()))
            .findFirst()
            .get();

        // Get Output
        final List<Iterable> output = mainInput.stream()
            .map(iterable -> op.transform(iterable, broadcastInput))
            .collect(Collectors.toList());

        // TODO #12: Implement Sink Operator
        if (dag.getOutEdgesOf(node).isPresent()) {
          // TODO #14: Implement Multi-Output Do Operators
          edgeIdToData.put(getSingleEdgeId(dag, node, EdgeDirection.Out), output);
        } else {
          edgeIdToData.put("NOEDGE", output);

        }
      } else if (node instanceof GroupByKey) {
        final List<Iterable> data = shuffle(edgeIdToData.get(getSingleEdgeId(dag, node, EdgeDirection.In)));
        edgeIdToData.put(getSingleEdgeId(dag, node, EdgeDirection.Out), data);
      } else if (node instanceof Broadcast) {
        final Broadcast broadcastOperator = (Broadcast)node;
        final List<Iterable> beforeBroadcasted = edgeIdToData.get(getSingleEdgeId(dag, node, EdgeDirection.In));
        final Iterable afterBroadcasted = broadcast(beforeBroadcasted);
        edgeIdToBroadcast.put(getSingleEdgeId(dag, node, EdgeDirection.Out), broadcastOperator.transform(afterBroadcasted));
      } else if (node instanceof Sink) {
        throw new UnsupportedOperationException();
      } else {
        throw new UnsupportedOperationException();
      }

      System.out.println("All non-broadcast data after " + node.getId() + ": " + edgeIdToData);
      System.out.println("Also, All broadcast data: " + edgeIdToBroadcast);
    }
  }

  private enum EdgeDirection {
    In,
    Out
  }

  private String getSingleEdgeId(final DAG dag, final Operator node, final EdgeDirection ed) {
    final Optional<List<Edge>> optional = (ed == EdgeDirection.In) ? dag.getInEdgesOf(node) : dag.getOutEdgesOf(node);
    if (optional.isPresent()) {
      final List<Edge> edges = optional.get();
      if (edges.size() != 1) {
        throw new IllegalArgumentException();
      } else {
        return edges.get(0).getId();
      }
    } else {
      throw new IllegalArgumentException();
    }
  }


  private Iterable broadcast(final List<Iterable> iterables) {
    final List result = new ArrayList();
    iterables.stream().forEach(iterable -> iterable.forEach(x -> result.add(x)));
    return result;
  }


  private List<Iterable> shuffle(final List<Iterable> data) {
    final HashMap<Integer, HashMap<Object, KV<Object, List>>> dstIdToCombined = new HashMap<>();
    final int numDest = 3;

    data.forEach(iterable -> iterable.forEach(element -> {
      final KV kv = (KV) element;
      final int dstId = kv.getKey().hashCode() % numDest;
      dstIdToCombined.putIfAbsent(dstId, new HashMap<>());
      final HashMap<Object, KV<Object, List>> combined = dstIdToCombined.get(dstId);
      combined.putIfAbsent(kv.getKey(), KV.of(kv.getKey(), new ArrayList()));
      combined.get(kv.getKey()).getValue().add(kv.getValue());
    }));

    return dstIdToCombined.values().stream()
        .map(map -> map.values().stream().collect(Collectors.toList()))
        .collect(Collectors.toList());
  }
}
