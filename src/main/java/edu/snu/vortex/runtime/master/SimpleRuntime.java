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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.Reader;
import edu.snu.vortex.compiler.ir.Transform;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeOperatorVertex;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.executor.channel.LocalChannel;
import edu.snu.vortex.utils.DAG;
import org.apache.commons.lang3.SerializationUtils;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * Simple Runtime that logs intermediate results.
 */
public final class SimpleRuntime {
  private static final Logger LOG = Logger.getLogger(SimpleRuntime.class.getName());

  // TODO #91: Implement Channels
  private static final String HACK_DUMMY_CHAND_ID = "HACK";

  /**
   * Physical DAG and Logical DAG have incomplete data structures.
   * As a result the dependency information between tasks within a stage in Physical DAG is lost in the translation.
   * So for now we just assume that a stage is a sequence of tasks that only have 0 or 1 child/parent.
   * This hack will be fixed by the following to do.
   * TODO #132: Refactor DAG
   *
   * @param physicalPlan Physical Plan.
   * @throws Exception during execution.
   */
  public void executePhysicalPlan(final PhysicalPlan physicalPlan) throws Exception {
    final Map<String, List<LocalChannel>> edgeIdToChannels = new HashMap<>();

    // TODO #93: Implement Batch Scheduler
    physicalPlan.getTaskGroupsByStage().forEach(stage -> {
      stage.forEach(taskGroup -> {

        // compute tasks in a taskgroup, supposedly 'rootVertices' at a time
        // (another shortcoming of the current physical DAG)
        final DAG<Task> taskDAG = taskGroup.getTaskDAG();
        Iterable<Element> data = null; // hack (TODO #132: Refactor DAG)
        Set<Task> currentTaskSet = new HashSet<>();
        currentTaskSet.addAll(taskDAG.getRootVertices());
        while (!currentTaskSet.isEmpty()) {
          for (final Task task : currentTaskSet) {
            final String vertexId = task.getRuntimeVertexId();

            // TODO #141: Remove instanceof
            if (task instanceof BoundedSourceTask) {
              try {
                final BoundedSourceTask boundedSourceTask = (BoundedSourceTask) task;
                final Reader reader = boundedSourceTask.getReader();
                data = reader.read();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            } else if (task instanceof OperatorTask) {
              // It the current task has any incoming edges, it reads data from the channels associated to the edges.
              // After that, it applies its transform function to the data read.
              final Set<StageBoundaryEdgeInfo> inEdges = taskGroup.getIncomingEdges().get(vertexId);
              final Map<Transform, Object> sideInputs = getSideInputs(inEdges, task, edgeIdToChannels);
              final Set<StageBoundaryEdgeInfo> nonSideInputEdges = getNonSideInputEdges(inEdges);
              if (nonSideInputEdges.size() > 1) {
                // TODO #13: Implement Join Node
                throw new UnsupportedOperationException("Multi inedge not yet supported");
              } else if (nonSideInputEdges.size() == 1) { // We fetch 'data' from the incoming stage
                final StageBoundaryEdgeInfo inEdge = nonSideInputEdges.iterator().next();
                data = edgeIdToChannels.get(inEdge.getStageBoundaryEdgeInfoId()).get(task.getIndex()).read();
              }

              final OperatorTask operatorTask = (OperatorTask) task;

              // TODO #18: Support code/data serialization
              final Transform transform = SerializationUtils.clone(operatorTask.getTransform());

              final Transform.Context transformContext = new ContextImpl(sideInputs);
              final OutputCollectorImpl outputCollector = new OutputCollectorImpl();
              transform.prepare(transformContext, outputCollector);
              transform.onData(data, null); // hack (TODO #132: Refactor DAG)
              transform.close();
              data = outputCollector.getOutputList();

            } else {
              throw new UnsupportedOperationException(task.toString());
            }

            LOG.log(Level.INFO, " Output of {" + task.getTaskId() + "}: " +
                (data.toString().length() > 5000 ?
                    data.toString().substring(0, 5000) + "..." : data.toString()));

            // If the current task has any outgoing edges, it writes data to channels associated to the edges.
            final Set<StageBoundaryEdgeInfo> outEdges = taskGroup.getOutgoingEdges().get(vertexId);
            if (outEdges != null) {
              final Iterable<Element> finalData = data;
              outEdges.forEach(outEdge -> {
                writeToChannels(task.getIndex(), edgeIdToChannels, outEdge, finalData);
              });
            }
          }

          // this is the only way to 'traverse' the DAG<Task>..... (TODO #132: Refactor DAG)
          currentTaskSet.forEach(task -> taskDAG.removeVertex(task));

          // get the next 'rootVertices'
          currentTaskSet.clear();
          currentTaskSet.addAll(taskDAG.getRootVertices());
        }
      });
    });
  }

  private Set<StageBoundaryEdgeInfo> getNonSideInputEdges(final Set<StageBoundaryEdgeInfo> inEdges) {
    if (inEdges != null) {
      return inEdges.stream()
          .filter(inEdge ->
              inEdge.getEdgeAttributes().get(RuntimeAttribute.Key.SideInput) != RuntimeAttribute.SideInput)
          .collect(Collectors.toSet());
    } else {
      return new HashSet<>(0);
    }
  }

  private Map<Transform, Object> getSideInputs(final Set<StageBoundaryEdgeInfo> inEdges,
                                               final Task task,
                                               final Map<String, List<LocalChannel>> edgeIdToChannels) {
    if (inEdges != null) {
      // We assume that all sideinputs are fetched from outside of the task's stage TODO #132: Refactor DAG
      final Map<Transform, Object> sideInputs = new HashMap<>();
      inEdges.stream()
          .filter(inEdge ->
              inEdge.getEdgeAttributes().get(RuntimeAttribute.Key.SideInput) == RuntimeAttribute.SideInput)
          .forEach(inEdge -> {
            final Iterable<Element> elementSideInput =
                edgeIdToChannels.get(inEdge.getStageBoundaryEdgeInfoId()).get(task.getIndex()).read();
            final List<Object> objectSideInput = StreamSupport
                .stream(elementSideInput.spliterator(), false)
                .map(element -> element.getData())
                .collect(Collectors.toList());
            if (objectSideInput.size() != 1) {
              throw new RuntimeException("Size of out data partitions of a broadcast operator must match 1");
            }
            sideInputs.put(
                ((RuntimeOperatorVertex) inEdge.getExternalVertex()).getOperatorVertex().getTransform(),
                objectSideInput.get(0));
          });
      return sideInputs;
    } else {
      return new HashMap<>(0);
    }
  }

  private void writeToChannels(final int srcTaskIndex,
                               final Map<String, List<LocalChannel>> edgeIdToChannels,
                               final StageBoundaryEdgeInfo edge,
                               final Iterable<Element> data) {
    final int dstParallelism = edge.getExternalVertexAttr().get(RuntimeAttribute.IntegerKey.Parallelism);
    final List<LocalChannel> dstChannels = edgeIdToChannels.computeIfAbsent(edge.getStageBoundaryEdgeInfoId(), s -> {
      final List<LocalChannel> newChannels = new ArrayList<>(dstParallelism);
      IntStream.range(0, dstParallelism).forEach(x -> {
        // This is a hack to make the runtime work for now
        // In the future, channels should be passed to tasks via their methods (e.g., Task#compute)
        // TODO #91: Implement Channels
        final LocalChannel newChannel = new LocalChannel(HACK_DUMMY_CHAND_ID);
        newChannel.initialize(null);
        newChannels.add(newChannel);
      });
      return newChannels;
    });

    final RuntimeAttribute attribute = edge.getEdgeAttributes().get(RuntimeAttribute.Key.CommPattern);
    switch (attribute) {
      case OneToOne:
        dstChannels.get(srcTaskIndex).write(data);
        break;
      case Broadcast:
        dstChannels.forEach(chan -> chan.write(data));
        break;
      case ScatterGather:
        final RuntimeAttribute partitioningAttribute = edge.getEdgeAttributes().get(RuntimeAttribute.Key.Partition);
        switch (partitioningAttribute) {
          case Hash:
            final List<List<Element>> routedPartitions = new ArrayList<>(dstParallelism);
            IntStream.range(0, dstParallelism).forEach(x -> routedPartitions.add(new ArrayList<>()));
            data.forEach(element -> {
              final int dstIndex = Math.abs(element.getKey().hashCode() % dstParallelism);
              routedPartitions.get(dstIndex).add(element);
            });
            IntStream.range(0, dstParallelism).forEach(x -> dstChannels.get(x).write(routedPartitions.get(x)));
            break;
          case Range:
            throw new UnsupportedOperationException("Range partitioning not yet supported");
          default:
            throw new RuntimeException("Unknown attribute: " + partitioningAttribute);
        }
        break;
      default:
        throw new UnsupportedOperationException(edge.toString());
    }
  }
}

