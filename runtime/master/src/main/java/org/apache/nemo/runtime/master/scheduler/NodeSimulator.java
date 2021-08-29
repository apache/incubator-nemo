/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nemo.runtime.master.scheduler;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/**
 * Class for simulating node.
 * This class manages resource distribution for each tasks.
 */
public final class NodeSimulator {
  private static final Logger LOG = LoggerFactory.getLogger(NodeSimulator.class.getName());
  private static final int DEFAULT_BYTES_PER_LINE = 5000;
  // TODO XXX: Strict definition for CPU Resource required.
  private static final int DEFAULT_CPU_RESOURCE = 100;
  private static final double DEFAULT_TRANSFORM_CPU_UTILIZATION = 1.3; // tuples / ms

  private final NetworkSimulator networkSimulator;

  // whether it allocate executor of not.
  private boolean allocated = false;
  private final String nodeName;
  private String executorId = "";
  private int capacity;
  private final Map<String, TaskHarness> taskMap;

  private int cpuResource;

  NodeSimulator(final String nodeName, final NetworkSimulator networkSimulator) {
    this.nodeName = nodeName;
    this.networkSimulator = networkSimulator;
    this.cpuResource = DEFAULT_CPU_RESOURCE;
    this.taskMap = new HashMap<>();
  }

  /**
   * add the number of tuples to process.
   *
   * @param taskId Target task id to add the number of tuples to process
   * @param dstVertexId Vertex id that consume tuples.
   * @param srcVertexId Vertex id that process tuples.
   * @param srcNodeName the name of node that process tuples.
   * @param numOfTuples the number of tuples to process that processed from upstream task.
   */
  public void addNumOfTuplesToProcess(final String taskId,
                                      final String dstVertexId,
                                      final String srcVertexId,
                                      final String srcNodeName,
                                      final long numOfTuples) {
    taskMap.get(taskId).addNumOfTuplesToProcess(dstVertexId, srcVertexId, srcNodeName, numOfTuples);
  }

  /**
   * Distribute CPU resource to tasks.
   * All tasks get resource equally.
   */
  // TODO XXX: It is required to distribute resource according to the number of tuples that can be processed.
  public void distributeResource() {
    long distributedCPU = this.cpuResource / (taskMap.size() == 0 ? 1 : taskMap.size());
    this.taskMap.values().forEach(taskHarness -> {
      taskHarness.setAllocatedCPU(distributedCPU);
    });
  }

  public String getNodeName() {
    return nodeName;
  }

  /**
   * allocated executor.
   *
   * @param targetExecutorId Executor Id to allocate.
   * @param executorCapacity   The number of tasks that can be processed at the same time in the executor.
   */
  public void allocateExecutor(final String targetExecutorId, final int executorCapacity) {
    LOG.info(String.format("put executor %s to %s", executorId, nodeName));
    this.executorId = executorId;
    this.capacity = executorCapacity;
    this.allocated = true;
  }

  /**
   * simulate task for duration.
   *
   * @param task     task to simulate.
   * @param duration The duration of the simulation.
   */
  public Map<String, Map<Pair<String, String>, Long>> simulate(final Task task, final long duration) {
    return taskMap.get(task.getTaskId()).simulate(duration);
  }

  /**
   * assign task to executor.
   *
   * @param task the task to execute.
   */
  public void onTaskReceived(final Task task) {
    LOG.info(String.format("task receive %s %s", executorId, task.getTaskId()));
    taskMap.put(task.getTaskId(), new TaskHarness(task));
  }

  public void prepare() {
    taskMap.values().forEach(TaskHarness::prepare);
  }


  boolean isAllocated() {
    return allocated;
  }

  /**
   * Class for simulating node.
   * This class manages resource distribution for each tasks.
   */
  private class TaskHarness {
    private final Task task;

    // the time it takes until the first tuples is processed, including latency.
    private long waitUntilProduceFirstTuple;

    // allocated CPU resource from node.
    private long allocatedCPU;

    // CPU utilization of task.
    private double cpuUtilization;

    // dstVertexId, srcVertexId pair of stage edge to dataFetcher
    private final Map<Pair<String, String>, DataFetchSimulator> dataFetcherMap;

    // srcVertexId of stage edge to writer
    private final Map<String, WriteSimulator> writerMap;

    TaskHarness(final Task task) {
      this.task = task;
      this.allocatedCPU = 0;
      this.waitUntilProduceFirstTuple = Long.MAX_VALUE;
      this.cpuUtilization = 0;
      this.dataFetcherMap = new HashMap<>();
      this.writerMap = new HashMap<>();
    }

    /**
     * Decompose task into a structure that is easy to simulate.
     */
    public void prepare() {
      final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag =
        SerializationUtils.deserialize(task.getSerializedIRDag());

      for (IRVertex irVertex : irDag.getTopologicalSort()) {
        if (irVertex instanceof SourceVertex) {
          // Source vertex DataFetcher
          String vertexId = irVertex.getId();
          dataFetcherMap.put(Pair.of(vertexId, vertexId),
            new DataFetchSimulator(Collections.singletonList(nodeName), true));
        }

        // Set Data Fetcher of incoming edge
        task.getTaskIncomingEdges()
          .stream()
          .filter(stageEdge -> stageEdge.getDstIRVertex().getId().equals(irVertex.getId())) // edge to this vertex
          .forEach(stageEdge -> {
            if (irVertex instanceof OperatorVertex) {

              List<String> nodeNames = getDataFetcherNodeNames(stageEdge);
              String srcVertexId = stageEdge.getSrcIRVertex().getId();
              String dstVertexId = irVertex.getId();

              dataFetcherMap.put(Pair.of(dstVertexId, srcVertexId), new DataFetchSimulator(nodeNames, false));
            }
          });

        // Set Writer
        task.getTaskOutgoingEdges()
          .stream()
          .filter(stageEdge -> stageEdge.getSrcIRVertex().getId().equals(irVertex.getId())) // edge to this vertex
          .forEach(stageEdge -> {
            if (irVertex instanceof OperatorVertex) {
              WriteSimulator writeSimulator = getWriter(stageEdge);
              writerMap.put(irVertex.getId(), writeSimulator);
            }
          });
      }

      // Set CPU utilization of task.
      this.cpuUtilization = DEFAULT_TRANSFORM_CPU_UTILIZATION;
    }

    /**
     * add the number of tuples to process.
     *
     * @param dstVertexId Vertex id that consume tuples.
     * @param srcVertexId Vertex id that process tuples.
     * @param srcNodeName the name of node that process tuples.
     * @param numOfTuples the number of tuples to process that processed from upstream task.
     */
    public void addNumOfTuplesToProcess(final String dstVertexId,
                                        final String srcVertexId,
                                        final String srcNodeName,
                                        final Long numOfTuples) {
      dataFetcherMap.get(Pair.of(dstVertexId, srcVertexId)).addNumOfTuplesToProcess(srcNodeName, numOfTuples);
    }

    /**
     * set allocate CPU resource.
     */
    public void setAllocatedCPU(final long cpu) {
      this.allocatedCPU = cpu;
    }

    /**
     * calculate latency.
     *
     * @return latency of task.
     */
    public long calculateExpectedTaskLatency() {
      long networkLatency = dataFetcherMap.values().stream()
        .map(DataFetchSimulator::getMinimumLatency)
        .reduce(Long.MAX_VALUE, Math::min);

      long transformLatency = (long) (1L / (cpuUtilization * allocatedCPU)); // i have to sophisticated here
      return networkLatency + transformLatency; // ms
    }

    /**
     * calculate maximum throughput when there are infinite tuples to process.
     *
     * @return maximum bandwidth of task.
     */
    public double calculateExpectedMaximumTaskThroughput() {
      double networkBandwidth = dataFetcherMap.values().stream()
        .map(DataFetchSimulator::getMaximumBandwidth)
        .flatMapToDouble(DoubleStream::of)
        .average()
        .orElse(0) / DEFAULT_BYTES_PER_LINE;

      double transformThroughput = cpuUtilization * allocatedCPU;
      return Math.min(networkBandwidth, transformThroughput);
    }

    /**
     * get total number of tuples to process from all data fetchers.
     *
     * @return the number of tuples to process
     */
    public long getTotalNumOfTuplesToProcess() {
      return this.dataFetcherMap.values().stream()
        .map(DataFetchSimulator::getTotalNumOfTuplesToProcess)
        .flatMapToLong(LongStream::of)
        .sum();
    }

    /**
     * calculates the number of tuples that are processed for duration.
     * Consumes the number of processed tuples and returns the number of tuples to transfer to downstream task.
     *
     * @param duration The duration of the simulation.
     * @return the number of tuples per downstream tasks.
     */
    public Map<String, Map<Pair<String, String>, Long>> simulate(final long duration) {
      long totalNumOfTupelsToProcess = getTotalNumOfTuplesToProcess();
      long executionDuration;

      // It means there are no tuples to process.
      if (totalNumOfTupelsToProcess == 0) {
        return new HashMap<>();
      }

      // It means that it starts to process tuples.
      if (waitUntilProduceFirstTuple == Long.MAX_VALUE) {
        waitUntilProduceFirstTuple = calculateExpectedTaskLatency();
      }

      if (waitUntilProduceFirstTuple < 0) {
        executionDuration = duration;
      } else {
        executionDuration = duration - waitUntilProduceFirstTuple;
      }
      waitUntilProduceFirstTuple -= duration;

      // It means that it until process first tuples.
      if (executionDuration <= 0) {
        return new HashMap<>();
      }

      // calculate the number of processed tuples.

      long maximumNumOfProcessedTuple = (long) calculateExpectedMaximumTaskThroughput() * executionDuration;
      long numOfprocessedTuple = Math.min(maximumNumOfProcessedTuple, totalNumOfTupelsToProcess);

      // calculate rate to total number of tuples to process.
      double consumeRate = (double) numOfprocessedTuple / totalNumOfTupelsToProcess;

      // consume tuples from data fetcher.
      long finalNumOfprocessedTuple = dataFetcherMap.values().stream()
        .map(dataFetcher -> dataFetcher.consume(consumeRate))
        .flatMapToLong(LongStream::of)
        .sum();

      // return the number of tuples to transfer to downstream tasks.
      return writerMap.values().stream()
        .map(writer -> writer.write(finalNumOfprocessedTuple))
        .collect(Collectors.toMap(Pair::left, Pair::right));
    }

    /**
     * Create writer according to the runtimeEdge.
     *
     * @param runtimeEdge runtimeEdge.
     * @return WriterSimulator corresponds to edge.
     */
    public WriteSimulator getWriter(final StageEdge runtimeEdge) {
      final Optional<CommunicationPatternProperty.Value> comValueOptional =
        runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);
      final CommunicationPatternProperty.Value comm = comValueOptional.orElseThrow(IllegalStateException::new);

      if (comm == CommunicationPatternProperty.Value.ONE_TO_ONE) {
        String taskId = networkSimulator.getDownstreamTaskId(runtimeEdge.getId(),
          RuntimeIdManager.getIndexFromTaskId(task.getTaskId()));
        return new WriteSimulator(Collections.singletonList(taskId), runtimeEdge.getDstIRVertex().getId(),
          runtimeEdge.getSrcIRVertex().getId(), 1);
      }
      String taskId;
      final List<String> taskIds = new LinkedList<>();
      final int numDstTasks = runtimeEdge.getDstIRVertex().getPropertyValue(ParallelismProperty.class).get();
      for (int dstTaskIdx = 0; dstTaskIdx < numDstTasks; dstTaskIdx++) {
        taskId = networkSimulator.getDownstreamTaskId(runtimeEdge.getId(),
          RuntimeIdManager.getIndexFromTaskId(task.getTaskId()));
        taskIds.add(taskId);
      }
      int partition;
      if (comm == CommunicationPatternProperty.Value.BROADCAST) {
        partition = 1;
      } else {
        partition = numDstTasks;
      }
      return new WriteSimulator(taskIds, runtimeEdge.getDstIRVertex().getId(),
        runtimeEdge.getSrcIRVertex().getId(), partition);
    }

    /**
     * get the list of node name of upstream tasks.
     *
     * @param runtimeEdge runtimeEdge.
     * @return list of node name of upstream task.
     */
    public List<String> getDataFetcherNodeNames(final StageEdge runtimeEdge) {
      final Optional<CommunicationPatternProperty.Value> comValueOptional =
        runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);
      final CommunicationPatternProperty.Value comm = comValueOptional.orElseThrow(IllegalStateException::new);

      switch (comm) {
        case ONE_TO_ONE:
          return Collections.singletonList(networkSimulator.getUpstreamNode(runtimeEdge.getId(),
            RuntimeIdManager.getIndexFromTaskId(task.getTaskId())));
        case BROADCAST:
        case SHUFFLE:
          final List<String> nodeNames = new LinkedList<>();
          final int numDstTasks = runtimeEdge.getDstIRVertex().getPropertyValue(ParallelismProperty.class).get();
          for (int dstTaskIdx = 0; dstTaskIdx < numDstTasks; dstTaskIdx++) {
            nodeNames.add(networkSimulator.getUpstreamNode(runtimeEdge.getId(),
              RuntimeIdManager.getIndexFromTaskId(task.getTaskId())));
          }
          return nodeNames;
        default:
          throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
      }
    }
  }

  /**
   * This class manages downstream tasks and edge type.
   */
  private class WriteSimulator {
    private final List<String> taskIds;
    private final String srcVertexId;
    private final String dstVertexId;
    private final int partition;

    WriteSimulator(final List<String> taskIds,
                   final String dstVertexId,
                   final String srcVertexId,
                   final int partition) {
      this.taskIds = taskIds;
      this.dstVertexId = dstVertexId;
      this.srcVertexId = srcVertexId;
      this.partition = partition;
    }

    /**
     * calculate the number of tuples to transfer to downstream tasks.
     *
     * @param numOfProcessedTuples the number of processed tuple.
     * @return the number of tuples to transfer downstream tasks.
     */
    public Pair<String, Map<Pair<String, String>, Long>> write(final long numOfProcessedTuples) {
      Map<Pair<String, String>, Long> numOfProcessedTUples = new HashMap<>();
      long numOfTuplesToTransfer = numOfProcessedTuples / partition;
      for (String taskId : taskIds) {
        numOfProcessedTUples.put(Pair.of(taskId, dstVertexId), numOfTuplesToTransfer);
      }
      return Pair.of(srcVertexId, numOfProcessedTUples);
    }
  }

  /**
   * This class manages upstream tasks and node name where it is.
   */
  private class DataFetchSimulator {
    private final boolean isSource;
    private final Map<String, Long> numOfTuplesToProcess;

    DataFetchSimulator(final List<String> nodeNames, final boolean isSource) {
      this.numOfTuplesToProcess = new HashMap<>();
      this.isSource = isSource;
      for (String srcNodeName : nodeNames) {
        if (isSource) {
          this.numOfTuplesToProcess.put(srcNodeName, Long.MAX_VALUE);
        } else {
          this.numOfTuplesToProcess.put(srcNodeName, 0L);
        }
      }
    }

    /**
     * add the number of tuples to process.
     *
     * @param srcNodeName node name of upstream task.
     * @param numOfTuplesToAdd the number of tuples to add.
     */
    public void addNumOfTuplesToProcess(final String srcNodeName, final long numOfTuplesToAdd) {
      long beforeNumOfTuplesToProcess = this.numOfTuplesToProcess.get(srcNodeName);
      this.numOfTuplesToProcess.put(srcNodeName, beforeNumOfTuplesToProcess + numOfTuplesToAdd);
    }

    /**
     * get total number of tuples to process.
     */
    public long getTotalNumOfTuplesToProcess() {
      if (isSource) {
        return Long.MAX_VALUE;
      } else {
        return this.numOfTuplesToProcess.values().stream().flatMapToLong(LongStream::of).sum();
      }
    }

    /**
     * calculate current latency considering whether there is tuples to process for each upstream task.
     */
    public long getCurrLatency() {
      return this.numOfTuplesToProcess.entrySet().stream()
        .filter(entry -> entry.getValue() > 0)
        .map(Map.Entry::getKey)
        .map(srcNodeName -> networkSimulator.getLatency(nodeName, srcNodeName))
        .reduce(Long.MAX_VALUE, Math::min);
    }

    /**
     * calculate the minimum latency assuming there is tuples to process for every upstream task.
     */
    public long getMinimumLatency() {
      return this.numOfTuplesToProcess.keySet().stream()
        .map(srcNodeName -> networkSimulator.getLatency(nodeName, srcNodeName))
        .reduce(Long.MAX_VALUE, Math::min);
    }

    /**
     * calculate current latency considering the ratio of tuples to be processed for each upstream task.
     */
    public long getCurrBandwidth() {
      long numWeighted = 0;
      long sum = 0;

      for (Map.Entry<String, Long> entry : numOfTuplesToProcess.entrySet()) {
        if (entry.getValue() <= 0) {
          continue;
        }

        numWeighted += entry.getValue();
        sum += entry.getValue() * networkSimulator.getBandwidth(nodeName, entry.getKey());
      }

      if (numWeighted == 0) {
        return 0;
      }
      return sum / numWeighted;
    }

    /**
     * calculate Maximum latency assuming that every upstream task has the same number of the tuples to process.
     */
    public double getMaximumBandwidth() {
      return this.numOfTuplesToProcess.keySet().stream()
        .flatMapToLong(srcNodeName -> LongStream.of(networkSimulator.getBandwidth(nodeName, srcNodeName)))
        .average()
        .orElse(0);
    }

    /**
     * Consume tuples.
     *
     * @param consumeRate consume rate to consume tuples.
     * @return the number of the tuples that are processed.
     */
    public long consume(final double consumeRate) {
      long numOfConsumedTuples = 0;
      for (String srcNodeName : numOfTuplesToProcess.keySet()) {
        long beforeNumOfTuples = numOfTuplesToProcess.get(srcNodeName);
        long numOfProcessedTuples = (long) Math.ceil(numOfTuplesToProcess.get(srcNodeName) * consumeRate);
        numOfTuplesToProcess.put(srcNodeName, beforeNumOfTuples - numOfProcessedTuples);
        numOfConsumedTuples += numOfProcessedTuples;
      }
      return numOfConsumedTuples;
    }
  }
}



