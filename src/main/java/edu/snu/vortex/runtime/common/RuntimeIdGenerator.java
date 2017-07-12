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
package edu.snu.vortex.runtime.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ID Generator.
 */
public final class RuntimeIdGenerator {
  private static AtomicInteger executionPlanIdGenerator = new AtomicInteger(1);
  private static AtomicInteger stageIdGenerator = new AtomicInteger(1);
  private static AtomicInteger taskIdGenerator = new AtomicInteger(1);
  private static AtomicInteger taskGroupIdGenerator = new AtomicInteger(1);
  private static AtomicInteger executorIdGenerator = new AtomicInteger(1);
  private static AtomicLong messageIdGenerator = new AtomicLong(1L);
  private static String partitionPrefix = "Partition-";
  private static String partitionIdSplitter = "_";

  private RuntimeIdGenerator() {
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan}.
   *
   * @return the generated ID
   */
  public static String generateExecutionPlanId() {
    return "Plan-" + executionPlanIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex}.
   *
   * @param irVertexId .
   * @return the generated ID
   */
  public static String generateRuntimeVertexId(final String irVertexId) {
    return "RVertex-" + irVertexId;
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.logical.StageEdge}.
   *
   * @param irEdgeId .
   * @return the generated ID
   */
  public static String generateStageEdgeId(final String irEdgeId) {
    return "SEdge-" + irEdgeId;
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.RuntimeEdge}.
   *
   * @param irEdgeId .
   * @return the generated ID
   */
  public static String generateRuntimeEdgeId(final String irEdgeId) {
    return "REdge-" + irEdgeId;
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.logical.Stage}.
   *
   * @return the generated ID
   */
  public static String generateStageId() {
    return "Stage-" + stageIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.physical.Task}.
   *
   * @return the generated ID
   */
  public static String generateTaskId() {
    return "Task-" + taskIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.plan.physical.TaskGroup}.
   *
   * @return the generated ID
   */
  public static String generateTaskGroupId() {
    return "TaskGroup-" + taskGroupIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for executor.
   *
   * @return the generated ID
   */
  public static String generateExecutorId() {
    return "Executor-" + executorIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for a partition, whose data is the output of a task.
   *
   * @param runtimeEdgeId of the partition
   * @param taskIndex     of the partition
   * @return the generated ID
   */
  public static String generatePartitionId(final String runtimeEdgeId,
                                           final int taskIndex) {
    return partitionPrefix + runtimeEdgeId + partitionIdSplitter + taskIndex;
  }

  /**
   * Generates the ID for a partition, whose data is a part of a task's output.
   *
   * @param runtimeEdgeId    of the partition
   * @param taskIndex        of the partition
   * @param destinationIndex of the partition
   * @return the generated ID
   */
  public static String generatePartitionId(final String runtimeEdgeId,
                                           final int taskIndex,
                                           final int destinationIndex) {
    return generatePartitionId(runtimeEdgeId, taskIndex) + partitionIdSplitter + destinationIndex;
  }

  /**
   * Generates the ID for a control message.
   *
   * @return the generated ID
   */
  public static long generateMessageId() {
    return messageIdGenerator.getAndIncrement();
  }

  /**
   * Parses a partition id.
   * If the id represents a partition in scatter gather edge,
   * the result array will contain runtime edge id, task index, and destination index in order.
   * In other cases, the result array will contain runtime edge id and task index only.
   *
   * @param partitionId to parse.
   * @return the array of parsed information.
   */
  public static String[] parsePartitionId(final String partitionId) {
    final String woPrefix = partitionId.split(partitionPrefix)[1];
    return woPrefix.split(partitionIdSplitter);
  }

  /**
   * Checks whether a partition id represents a partition in scatter gather edge or not.
   *
   * @param partitionId to check.
   * @return {@code true} if it represents a scatter gather partition, {@code false} if doesn't.
   */
  public static boolean isScatterGatherEdge(final String partitionId) {
    return partitionId.split(partitionIdSplitter).length == 3;
  }
}
