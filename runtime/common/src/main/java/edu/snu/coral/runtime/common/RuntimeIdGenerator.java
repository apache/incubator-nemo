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
package edu.snu.coral.runtime.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ID Generator.
 */
public final class RuntimeIdGenerator {
  private static AtomicInteger physicalPlanIdGenerator = new AtomicInteger(0);
  private static AtomicInteger executorIdGenerator = new AtomicInteger(0);
  private static AtomicLong messageIdGenerator = new AtomicLong(1L);
  private static AtomicLong resourceSpecIdGenerator = new AtomicLong(0);
  private static final String BLOCK_PREFIX = "Block-";
  private static final String BLOCK_ID_SPLITTER = "_";
  private static final String TASK_GROUP_INFIX = "-TaskGroup-";
  private static final String PHYSICAL_TASK_ID_SPLITTER = "_";

  /**
   * Private constructor which will not be used.
   */
  private RuntimeIdGenerator() {
  }

  /**
   * Generates the ID for {@link edu.snu.coral.runtime.common.plan.physical.PhysicalPlan}.
   *
   * @return the generated ID
   */
  public static String generatePhysicalPlanId() {
    return "Plan-" + physicalPlanIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link edu.snu.coral.runtime.common.plan.stage.StageEdge}.
   *
   * @param irEdgeId .
   * @return the generated ID
   */
  public static String generateStageEdgeId(final String irEdgeId) {
    return "SEdge-" + irEdgeId;
  }

  /**
   * Generates the ID for {@link edu.snu.coral.runtime.common.plan.RuntimeEdge}.
   *
   * @param irEdgeId .
   * @return the generated ID
   */
  public static String generateRuntimeEdgeId(final String irEdgeId) {
    return "REdge-" + irEdgeId;
  }

  /**
   * Generates the ID for {@link edu.snu.coral.runtime.common.plan.stage.Stage}.
   * @param stageId stage ID in numeric form.
   * @return the generated ID
   */
  public static String generateStageId(final Integer stageId) {
    return "Stage-" + stageId;
  }

  /**
   * Generates the ID for {@link edu.snu.coral.runtime.common.plan.physical.Task}.
   *
   * @param irVertexId the ID of the IR vertex.
   * @return the generated ID
   */
  public static String generateLogicalTaskId(final String irVertexId) {
    return "Task-" + irVertexId;
  }

  /**
   * Generates the ID for {@link edu.snu.coral.runtime.common.plan.physical.Task}.
   *
   * @param index         the index of the physical task.
   * @param logicalTaskId the logical ID of the task.
   * @return the generated ID
   */
  public static String generatePhysicalTaskId(final int index,
                                              final String logicalTaskId) {
    return logicalTaskId + PHYSICAL_TASK_ID_SPLITTER + index;
  }

  /**
   * Generates the ID for {@link edu.snu.coral.runtime.common.plan.physical.ScheduledTaskGroup}.
   *
   * @param index   the index of this task group.
   * @param stageId the ID of the stage.
   * @return the generated ID
   */
  public static String generateTaskGroupId(final int index,
                                           final String stageId) {
    return stageId + TASK_GROUP_INFIX + index;
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
   * Generates the ID for a block, whose data is the output of a task.
   *
   * @param runtimeEdgeId of the block
   * @param taskIndex     of the block
   * @return the generated ID
   */
  public static String generateBlockId(final String runtimeEdgeId,
                                       final int taskIndex) {
    return BLOCK_PREFIX + runtimeEdgeId + BLOCK_ID_SPLITTER + taskIndex;
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
   * Generates the ID for a resource specification.
   *
   * @return the generated ID
   */
  public static String generateResourceSpecId() {
    return "ResourceSpec-" + resourceSpecIdGenerator.getAndIncrement();
  }

  /**
   * Extracts runtime edge ID from a block ID.
   *
   * @param blockId the block ID to extract.
   * @return the runtime edge ID.
   */
  public static String getRuntimeEdgeIdFromBlockId(final String blockId) {
    return parseBlockId(blockId)[0];
  }

  /**
   * Extracts task index from a block ID.
   *
   * @param blockId the block ID to extract.
   * @return the task index.
   */
  public static String getTaskIndexFromBlockId(final String blockId) {
    return parseBlockId(blockId)[1];
  }

  /**
   * Parses a block id.
   * The result array will contain runtime edge id and task index in order.
   *
   * @param blockId to parse.
   * @return the array of parsed information.
   */
  private static String[] parseBlockId(final String blockId) {
    final String woPrefix = blockId.split(BLOCK_PREFIX)[1];
    return woPrefix.split(BLOCK_ID_SPLITTER);
  }

  /**
   * Extracts stage ID from a task group ID.
   *
   * @param taskGroupId the task group ID to extract.
   * @return the stage ID.
   */
  public static String getStageIdFromTaskGroupId(final String taskGroupId) {
    return parseTaskGroupId(taskGroupId)[0];
  }

  /**
   * Extracts task group index from a task group ID.
   *
   * @param taskGroupId the task group ID to extract.
   * @return the index.
   */
  public static int getIndexFromTaskGroupId(final String taskGroupId) {
    return Integer.valueOf(parseTaskGroupId(taskGroupId)[1]);
  }

  /**
   * Parses a task group id.
   * The result array will contain the stage id and the index of the task group in order.
   *
   * @param taskGroupId to parse.
   * @return the array of parsed information.
   */
  private static String[] parseTaskGroupId(final String taskGroupId) {
    return taskGroupId.split(TASK_GROUP_INFIX);
  }
  
  /**
   * Extracts logical task ID from a physical task ID.
   *
   * @param physicalTaskId the physical task ID to extract.
   * @return the logical task ID.
   */
  public static String getLogicalTaskIdIdFromPhysicalTaskId(final String physicalTaskId) {
    return physicalTaskId.split(PHYSICAL_TASK_ID_SPLITTER)[0];
  }
}
