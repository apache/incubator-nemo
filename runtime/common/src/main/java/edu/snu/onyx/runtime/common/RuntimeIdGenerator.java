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
package edu.snu.onyx.runtime.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ID Generator.
 */
public final class RuntimeIdGenerator {
  private static AtomicInteger physicalPlanIdGenerator = new AtomicInteger(0);
  private static AtomicInteger taskIdGenerator = new AtomicInteger(0);
  private static AtomicInteger taskGroupIdGenerator = new AtomicInteger(0);
  private static AtomicInteger executorIdGenerator = new AtomicInteger(0);
  private static AtomicLong messageIdGenerator = new AtomicLong(1L);
  private static AtomicLong resourceSpecIdGenerator = new AtomicLong(0);
  private static String blockPrefix = "Block-";
  private static String blockIdSplitter = "_";

  private RuntimeIdGenerator() {
  }

  /**
   * Generates the ID for {@link edu.snu.onyx.runtime.common.plan.physical.PhysicalPlan}.
   *
   * @return the generated ID
   */
  public static String generatePhysicalPlanId() {
    return "Plan-" + physicalPlanIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link edu.snu.onyx.runtime.common.plan.stage.StageEdge}.
   *
   * @param irEdgeId .
   * @return the generated ID
   */
  public static String generateStageEdgeId(final String irEdgeId) {
    return "SEdge-" + irEdgeId;
  }

  /**
   * Generates the ID for {@link edu.snu.onyx.runtime.common.plan.RuntimeEdge}.
   *
   * @param irEdgeId .
   * @return the generated ID
   */
  public static String generateRuntimeEdgeId(final String irEdgeId) {
    return "REdge-" + irEdgeId;
  }

  /**
   * Generates the ID for {@link edu.snu.onyx.runtime.common.plan.stage.Stage}.
   * @param stageId stage ID in numeric form.
   * @return the generated ID
   */
  public static String generateStageId(final Integer stageId) {
    return "Stage-" + stageId;
  }

  /**
   * Generates the ID for {@link edu.snu.onyx.runtime.common.plan.physical.Task}.
   *
   * @return the generated ID
   */
  public static String generateTaskId() {
    return "Task-" + taskIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link edu.snu.onyx.runtime.common.plan.physical.TaskGroup}.
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
   * Generates the ID for a block, whose data is the output of a task.
   *
   * @param runtimeEdgeId of the block
   * @param taskIndex     of the block
   * @return the generated ID
   */
  public static String generateBlockId(final String runtimeEdgeId,
                                       final int taskIndex) {
    return blockPrefix + runtimeEdgeId + blockIdSplitter + taskIndex;
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
    final String woPrefix = blockId.split(blockPrefix)[1];
    return woPrefix.split(blockIdSplitter);
  }
}
