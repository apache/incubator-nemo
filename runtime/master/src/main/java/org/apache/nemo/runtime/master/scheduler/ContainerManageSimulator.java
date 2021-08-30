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

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.exception.JsonParseException;
import org.apache.nemo.runtime.common.plan.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This class manages virtual nodes and data transfer size between tasks.
 */
final class ContainerManageSimulator {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerManageSimulator.class.getName());
  private final Map<String, NodeSimulator> nodeSimulatorMap;
  private final Map<String, String> executorIdToNodeId;
  private final Map<String, String> taskIdToNodeId;
  private final Map<String, Task> taskMap;
  private final NetworkSimulator networkSimulator;

  ContainerManageSimulator() {
    nodeSimulatorMap = new HashMap<>();
    executorIdToNodeId = new HashMap<>();
    taskIdToNodeId = new HashMap<>();
    taskMap = new HashMap<>();
    networkSimulator = new NetworkSimulator();
  }

  /**
   * Utility method for parsing the node specification string.
   */
  public void parseNodeSpecificationString(final String nodeSpecficationString) {
    final Map<Pair<String, String>, Long> latencyMap = new HashMap<>();
    final Map<Pair<String, String>, Long> bandwidthMap = new HashMap<>();

    try {
      final TreeNode jsonRootNode = new ObjectMapper().readTree(nodeSpecficationString);

      // parse network specification
      final TreeNode networkNode = jsonRootNode.get("network");
      for (Iterator<String> it = networkNode.fieldNames(); it.hasNext();) {
        String pairs = it.next();

        final TreeNode pairNode = networkNode.get(pairs);
        List<String> hostNames = Arrays.asList(pairs.split("/"));
        if (hostNames.size() != 2) {
          continue;
        }

        Pair<String, String> nodePair = Pair.of(hostNames.get(0), hostNames.get(1));

        final long bandwidth = Long.parseLong(pairNode.get("bw").traverse().nextTextValue().split(" ")[0]);
        final long latency = Long.parseLong(pairNode.get("latency").traverse().nextTextValue().split(" ")[0]);
        latencyMap.put(nodePair, latency / 1000);
        bandwidthMap.put(nodePair, bandwidth / 1000);
      }

      // update bandwidth and latency to networkSimulator
      this.networkSimulator.updateBandwidth(bandwidthMap);
      this.networkSimulator.updateLatency(latencyMap);

      // parse node names
      final TreeNode nodeNode = jsonRootNode.get("nodes");
      for (int i = 0; i < nodeNode.size(); i++) {
        final String nodeName = nodeNode.get(i).traverse().nextTextValue();
        this.nodeSimulatorMap.put(nodeName, new NodeSimulator(nodeName, networkSimulator));
      }

    } catch (final Exception e) {
      throw new JsonParseException(e);
    }
  }

  /**
   * allocated executor to virtual node.
   * Assign in order in nodeSimulatorMap.
   *
   * @param executorId Executor Id to allocate.
   * @param capacity   The number of tasks that can be processed at the same time in the executor.
   */
  // TODO XXX: We need to check how hadoop assigns executors.
  public void allocateExecutor(final String executorId, final int capacity) throws Exception {
    for (NodeSimulator simulator : this.nodeSimulatorMap.values()) {
      if (!simulator.isAllocated()) {
        simulator.allocateExecutor(executorId, capacity);
        executorIdToNodeId.put(executorId, simulator.getNodeName());
        return;
      }
    }
    throw new Exception();
  }

  /**
   * assign task to executor.
   *
   * @param executorId Executor Id to allocate.
   * @param task       task to allocate.
   */
  public void onTaskReceived(final String executorId, final Task task) {
    nodeSimulatorMap.get(executorIdToNodeId.get(executorId)).onTaskReceived(task);
    taskIdToNodeId.put(task.getTaskId(), executorIdToNodeId.get(executorId));
    taskMap.put(task.getTaskId(), task);

    // get upstream task
    networkSimulator.updateRuntimeEdgeSrcIndexToNode(task.getTaskOutgoingEdges(),
      task.getTaskId(), executorIdToNodeId.get(executorId));
    networkSimulator.updateRuntimeEdgeDstIndestToTaskId(task.getTaskIncomingEdges(), task.getTaskId());
  }

  /**
   * trigger prepare to every executors.
   */
  public void prepare() {
    nodeSimulatorMap.values().forEach(NodeSimulator::prepare);
  }

  public void reset() {
    nodeSimulatorMap.clear();
    executorIdToNodeId.clear();
    taskIdToNodeId.clear();
    taskMap.clear();
    networkSimulator.reset();
  }
  /**
   * get every tasks of job.
   *
   * @return list of all tasks.
   */
  public List<Task> getTasks() {
    return new ArrayList<>(taskMap.values());
  }

  /**
   * get every executors of job.
   *
   * @return list of all nodeSimulators
   */
  public List<NodeSimulator> getNodeSimulators() {
    return new ArrayList<>(this.nodeSimulatorMap.values());
  }

  /**
   * simulate task for duration.
   * When task and durations are entered, the number of tuples that can be processed in a given period is calculated
   * and the number of tuples that the downstream task needs to process is updated.
   *
   * @param task     task to simulate.
   * @param duration The duration of the simulation.
   */
  public void simulate(final Task task, final long duration) throws Exception {
    String srcNodeName = this.taskIdToNodeId.get(task.getTaskId());
    // simulate task
    Map<String, Map<Pair<String, String>, Long>> numOfProcessedTuples =
      nodeSimulatorMap.get(srcNodeName).simulate(task, duration);

    // add the number of tuples to process of downstream tasks.
    numOfProcessedTuples.forEach((srcVertexId, numberOfProcessedTupleMap) -> {
      numberOfProcessedTupleMap.forEach((pair, numberOfProcessedTuples) -> {
        addNumOfTuplesToProcess(pair.left(), pair.right(), srcVertexId, srcNodeName, numberOfProcessedTuples);
      });
    });
  }

  /**
   * transfer the number of tuples to process to nodeSimulator.
   */
  public void addNumOfTuplesToProcess(final String dstTaskId,
                                      final String dstVertexId,
                                      final String srcVertexId,
                                      final String srcNodeName,
                                      final long numOfTuples) {
    String nodeName = this.taskIdToNodeId.get(dstTaskId);
    nodeSimulatorMap.get(nodeName).addNumOfTuplesToProcess(dstTaskId, dstVertexId, srcVertexId,
      srcNodeName, numOfTuples);
  }
}
