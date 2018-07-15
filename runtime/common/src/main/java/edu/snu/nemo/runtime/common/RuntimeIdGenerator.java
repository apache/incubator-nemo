/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.runtime.common;

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
  private static final String TASK_INFIX = "-Task-";

  /**
   * Private constructor which will not be used.
   */
  private RuntimeIdGenerator() {
  }


  //////////////////////////////////////////////////////////////// Generate IDs

  /**
   * Generates the ID for physical plan.
   *
   * @return the generated ID
   */
  public static String generatePhysicalPlanId() {
    return "Plan-" + physicalPlanIdGenerator.get();
  }

  /**
   * Generates the ID for {@link edu.snu.nemo.runtime.common.plan.StageEdge}.
   *
   * @param irEdgeId .
   * @return the generated ID
   */
  public static String generateStageEdgeId(final String irEdgeId) {
    return "SEdge-" + irEdgeId;
  }

  /**
   * Generates the ID for {@link edu.snu.nemo.runtime.common.plan.Stage}.
   * @param stageId stage ID in numeric form.
   * @return the generated ID
   */
  public static String generateStageId(final Integer stageId) {
    return "Stage-" + stageId;
  }

  /**
   * Generates the ID for a task.
   *
   * @param index   the index of this task.
   * @param stageId the ID of the stage.
   * @return the generated ID
   */
  public static String generateTaskId(final int index, final String stageId) {
    return stageId + TASK_INFIX + index;
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
   * @param producerTaskIndex of the block
   * @return the generated ID
   */
  public static String generateBlockId(final String runtimeEdgeId,
                                       final int producerTaskIndex) {
    return BLOCK_PREFIX + runtimeEdgeId + BLOCK_ID_SPLITTER + producerTaskIndex;
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

  //////////////////////////////////////////////////////////////// Parse IDs

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
   * Extracts stage ID from a task ID.
   *
   * @param taskId the task ID to extract.
   * @return the stage ID.
   */
  public static String getStageIdFromTaskId(final String taskId) {
    return parseTaskId(taskId)[0];
  }

  /**
   * Extracts task index from a task ID.
   *
   * @param taskId the task ID to extract.
   * @return the index.
   */
  public static int getIndexFromTaskId(final String taskId) {
    return Integer.valueOf(parseTaskId(taskId)[1]);
  }

  /**
   * Parses a task id.
   * The result array will contain the stage id and the index of the task in order.
   *
   * @param taskId to parse.
   * @return the array of parsed information.
   */
  private static String[] parseTaskId(final String taskId) {
    return taskId.split(TASK_INFIX);
  }
}
