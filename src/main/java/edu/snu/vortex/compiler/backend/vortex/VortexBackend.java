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
import edu.snu.vortex.compiler.ir.operator.Do;
import edu.snu.vortex.compiler.ir.operator.GroupByKey;
import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.compiler.ir.operator.Source;
import edu.snu.vortex.runtime.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class VortexBackend implements Backend {

  public TaskDAG compile(final DAG dag) {
    final List<List<Operator>> stages = toStages(dag);
    final Map<String, List<Task>> operatorIdToTasks = new HashMap<>();
    final TaskDAG taskDAG = new TaskDAG();
    stages.forEach(stage -> taskDAG.addStage(toTaskStage(dag, stage, operatorIdToTasks)));
    return taskDAG;
  }

  private List<TaskGroup> toTaskStage(final DAG dag,
                                      final List<Operator> stage,
                                      final Map<String, List<Task>> operatorIdToTasks) {
    final int reduceParallelism = 2; // hack
    int desiredByte = 20; // HACK

    final List<List<Task>> result = new ArrayList<>();

    for (final Operator operator : stage) {
      final List<Task> tasksForMap = new ArrayList<>();

      if (operator instanceof Do) {
        // simply transform
        final Do doOperator = (Do) operator;
        result.forEach(list -> {
          final Channel lastTaskOutChan = list.get(list.size()-1).getOutChans().get(0);
          final Task newTask = new DoTask(Arrays.asList(lastTaskOutChan), doOperator, Arrays.asList(new MemoryChannel()));
          tasksForMap.add(newTask);
          list.add(newTask);
        });

      } else if (operator instanceof GroupByKey) {
        final List<Task> prevTasks = operatorIdToTasks.get(dag.getInEdgesOf(operator).get().get(0).getSrc().getId());
        final List<List<Channel>> prevOutChans = prevTasks.stream()
            .map(Task::getOutChans)
            .collect(Collectors.toList());
        final int numOfReducers = prevOutChans.get(0).size();

        /*
        result.addAll(IntStream.range(0, numOfReducers).mapToObj(index -> index)
            .map(index -> {
              final List<Channel> inChans = prevOutChans.stream()
                  .map(chanList -> chanList.get(index))
                  .collect(Collectors.toList());
              return new MergeTask(inChans, new MemoryChannel());})
            .map(task -> Arrays.asList(task))
            .collect(Collectors.toList()));
            */


      } else if (operator instanceof Source) {
        try {
          // simply read
          final Source sourceOperator = (Source) operator;
          final List<Source.Reader> readers = sourceOperator.getReaders(desiredByte);
          result.addAll(readers.stream()
              .map(reader -> new SourceTask(reader, Arrays.asList(new MemoryChannel())))
              .map(task -> {
                final List<Task> newList = new ArrayList<>();
                newList.add(task);
                tasksForMap.add(task);
                return newList;
              })
              .collect(Collectors.toList()));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        System.out.println("Source TaskList " + result);
      } else {
        throw new RuntimeException("Unknown operator");
      }

      operatorIdToTasks.put(operator.getId(), tasksForMap);
    }


    final Optional<List<Edge>> finalEdges = dag.getOutEdgesOf(stage.get(stage.size()-1));
    if (finalEdges.isPresent() && finalEdges.get().stream().anyMatch(edge -> edge.getType() == Edge.Type.M2M)) {
      result.forEach(list -> {
        final Channel lastTaskOutChan = list.get(list.size()-1).getOutChans().get(0);
        final List<Channel> newTaskOutChans = IntStream.range(0, reduceParallelism)
            .mapToObj(x -> x)
            .map(x -> new TCPChannel())
            .collect(Collectors.toList());
        final Task newTask = new PartitionTask(lastTaskOutChan, newTaskOutChans);
        list.add(newTask);
      });
    }

    return result.stream().map(taskList -> new TaskGroup(taskList)).collect(Collectors.toList());
  }

  private List<List<Operator>> toStages(final DAG dag) {
    final List<List<Operator>> stages = new ArrayList<>();
    final List<Operator> topoSorted = new ArrayList<>();
    dag.doDFS((op -> topoSorted.add(0, op)), DAG.VisitOrder.PostOrder);

    final Set<Operator> printed = new HashSet<>();
    topoSorted.stream()
        .filter(operator -> !printed.contains(operator))
        .forEach(operator -> {
          final List<Operator> stage = new ArrayList<>();
          getFifoQueueNeighbors(dag, operator, stage);
          stages.add(stage);
          printed.addAll(stage);
        });

    return stages;
  }

  private void getFifoQueueNeighbors(final DAG dag, final Operator operator, final List<Operator> stage) {
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
}
