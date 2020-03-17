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
package org.apache.nemo.runtime.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ID Generator.
 */
public final class RuntimeIdManager {
  private static AtomicInteger physicalPlanIdGenerator = new AtomicInteger(0);
  private static AtomicInteger executorIdGenerator = new AtomicInteger(0);
  private static AtomicLong messageIdGenerator = new AtomicLong(1L);
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
   * TODO #100: Refactor string-based RuntimeIdGenerator for IR-based DynOpt
   */
  public static String generatePhysicalPlanId() {
    return "Plan" + physicalPlanIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link org.apache.nemo.runtime.common.plan.Stage}.
   *
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
   * Generates the ID for a block, whose data is the output of a task attempt.
   *
   * @param runtimeEdgeId  of the block
   * @param producerTaskId of the block
   * @return the generated ID
   */
  public static String generateBlockId(final String runtimeEdgeId,
                                       final String producerTaskId) {
    return runtimeEdgeId + SPLITTER + getIndexFromTaskId(producerTaskId)
      + SPLITTER + getAttemptFromTaskId(producerTaskId);
  }

  /**
   * The block ID wildcard indicates to use 'ANY' of the available blocks produced by different task attempts.
   * (Notice that a task clone or a task retry leads to a new task attempt)
   * <p>
   * Wildcard block id looks like SEdge4-1-* (task index = 1), where the '*' matches with any task attempts.
   * For this example, the ids of the producer task attempts will look like [Stage1-1-0, Stage1-1-1, Stage1-1-2, ...],
   * with the (1) task stage id corresponding to the outgoing edge, (2) task index = 1, and (3) all task attempts.
   *
   * @param runtimeEdgeId     of the block
   * @param producerTaskIndex of the block
   * @return the generated WILDCARD ID
   */
  public static String generateBlockIdWildcard(final String runtimeEdgeId,
                                               final int producerTaskIndex) {
    return runtimeEdgeId + SPLITTER + producerTaskIndex + SPLITTER + "*";
  }

  /**
   * Generates the ID for a control message.
   *
   * @return the generated ID
   */
  public static long generateMessageId() {
    return messageIdGenerator.getAndIncrement();
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
  public static int getTaskIndexFromBlockId(final String blockId) {
    return Integer.valueOf(split(blockId)[1]);
  }

  /**
   * Extracts wild card from a block ID.
   *
   * @param blockId the block ID to extract.
   * @return the wild card.
   */
  public static String getWildCardFromBlockId(final String blockId) {
    return generateBlockIdWildcard(getRuntimeEdgeIdFromBlockId(blockId), getTaskIndexFromBlockId(blockId));
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
