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
public final class RuntimeIdManager {
  private static AtomicInteger physicalPlanIdGenerator = new AtomicInteger(0);
  private static AtomicInteger executorIdGenerator = new AtomicInteger(0);
  private static AtomicLong messageIdGenerator = new AtomicLong(1L);
  private static AtomicLong resourceSpecIdGenerator = new AtomicLong(0);
  private static final String SPLITTER = "-";

  /**
   * Private constructor which will not be used.
   */
  private RuntimeIdManager() {
  }


  //////////////////////////////////////////////////////////////// Generate IDs

  /**
   * Generates the ID for physical plan.
   *
   * @return the generated ID
   */
  public static String generatePhysicalPlanId() {
    return "Plan" + physicalPlanIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link edu.snu.nemo.runtime.common.plan.StageEdge}.
   *
   * @param irEdgeId .
   * @return the generated ID
   */
  public static String generateStageEdgeId(final String irEdgeId) {
    return "SEdge" + irEdgeId;
  }

  /**
   * Generates the ID for {@link edu.snu.nemo.runtime.common.plan.Stage}.
   * @param stageId stage ID in numeric form.
   * @return the generated ID
   */
  public static String generateStageId(final Integer stageId) {
    return "Stage" + stageId;
  }

  /**
   * Generates the ID for a task.
   *
   * @param stageId the ID of the stage.
   * @param index   the index of this task.
   * @param attempt the attempt of this task.
   * @return the generated ID
   */
  public static String generateTaskId(final String stageId, final int index, final int attempt) {
    if (index < 0 || attempt < 0) {
      throw new IllegalStateException(index + ", " + attempt);
    }
    return stageId + SPLITTER + index + SPLITTER + attempt;
  }

  /**
   * Generates the ID for executor.
   *
   * @return the generated ID
   */
  public static String generateExecutorId() {
    return "Executor" + executorIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for a block, whose data is the output of a task.
   *
   * @param runtimeEdgeId of the block
   * @param producerTaskId of the block
   * @return the generated ID
   */
  public static String generateBlockId(final String runtimeEdgeId,
                                       final String producerTaskId) {
    return runtimeEdgeId + SPLITTER + getIndexFromTaskId(producerTaskId)
        + SPLITTER + getAttemptFromTaskId(producerTaskId);
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
    return "ResourceSpec" + resourceSpecIdGenerator.getAndIncrement();
  }

  //////////////////////////////////////////////////////////////// Parse IDs

  /**
   * Extracts runtime edge ID from a block ID.
   *
   * @param blockId the block ID to extract.
   * @return the runtime edge ID.
   */
  public static String getRuntimeEdgeIdFromBlockId(final String blockId) {
    return split(blockId)[0];
  }

  /**
   * Extracts task index from a block ID.
   *
   * @param blockId the block ID to extract.
   * @return the task index.
   */
  public static String getTaskIndexFromBlockId(final String blockId) {
    return split(blockId)[1];
  }

  /**
   * Extracts stage ID from a task ID.
   *
   * @param taskId the task ID to extract.
   * @return the stage ID.
   */
  public static String getStageIdFromTaskId(final String taskId) {
    return split(taskId)[0];
  }

  /**
   * Extracts task index from a task ID.
   *
   * @param taskId the task ID to extract.
   * @return the index.
   */
  public static int getIndexFromTaskId(final String taskId) {
    return Integer.valueOf(split(taskId)[1]);
  }

  /**
   * Extracts the attempt from a task ID.
   *
   * @param taskId the task ID to extract.
   * @return the attempt.
   */
  public static int getAttemptFromTaskId(final String taskId) {
    return Integer.valueOf(split(taskId)[2]);
  }

  private static String[] split(final String id) {
    return id.split(SPLITTER);
  }
}