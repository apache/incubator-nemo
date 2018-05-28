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
package edu.snu.nemo.runtime.executor.datatransfer;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-Task data handler.
 * This is a wrapper class that handles data transfer of a Task.
 * As Task input is processed element-wise, Task output element percolates down
 * through the DAG of children TaskDataHandlers.
 */
public final class TaskDataHandler {
  private final Task task;
  private List<TaskDataHandler> children;
  private final List<OutputCollectorImpl> inputFromThisStage;
  private final List<InputReader> sideInputFromOtherStages;
  private final List<OutputCollectorImpl> sideInputFromThisStage;
  private OutputCollectorImpl outputCollector;
  private final List<OutputWriter> outputWriters;

  /**
   * TaskDataHandler Constructor.
   *
   * @param task Task of this TaskDataHandler.
   */
  public TaskDataHandler(final Task task) {
    this.task = task;
    this.children = new ArrayList<>();
    this.inputFromThisStage = new ArrayList<>();
    this.sideInputFromOtherStages = new ArrayList<>();
    this.sideInputFromThisStage = new ArrayList<>();
    this.outputCollector = null;
    this.outputWriters = new ArrayList<>();
  }

  /**
   * Get the task that owns this TaskDataHandler.
   *
   * @return task of this TaskDataHandler.
   */
  public Task getTask() {
    return task;
  }

  /**
   * Get a DAG of children tasks' TaskDataHandlers.
   *
   * @return DAG of children tasks' TaskDataHandlers.
   */
  public List<TaskDataHandler> getChildren() {
    return children;
  }

  /**
   * Get side input from other Task.
   *
   * @return InputReader that has side input.
   */
  public List<InputReader> getSideInputFromOtherStages() {
    return sideInputFromOtherStages;
  }

  /**
   * Get intra-Task side input from parent tasks.
   * Just like normal intra-Task inputs, intra-Task side inputs are
   * collected in parent tasks' OutputCollectors.
   *
   * @return OutputCollectors of all parent tasks which are marked as having side input.
   */
  public List<OutputCollectorImpl> getSideInputFromThisStage() {
    return sideInputFromThisStage;
  }

  /**
   * Get OutputCollector of this task.
   *
   * @return OutputCollector of this task.
   */
  public OutputCollectorImpl getOutputCollector() {
    return outputCollector;
  }

  /**
   * Get OutputWriters of this task.
   *
   * @return OutputWriters of this task.
   */
  public List<OutputWriter> getOutputWriters() {
    return outputWriters;
  }

  /**
   * Set a DAG of children tasks' DataHandlers.
   *
   * @param childrenDataHandler list of children TaskDataHandlers.
   */
  public void setChildrenDataHandler(final List<TaskDataHandler> childrenDataHandler) {
    children = childrenDataHandler;
  }

  /**
   * Add OutputCollector of a parent task that will provide intra-stage input.
   *
   * @param input OutputCollector of a parent task.
   */
  public void addInputFromThisStages(final OutputCollectorImpl input) {
    inputFromThisStage.add(input);
  }

  /**
   * Add InputReader that will provide inter-stage side input.
   *
   * @param sideInputReader InputReader that will provide inter-stage side input.
   */
  public void addSideInputFromOtherStages(final InputReader sideInputReader) {
    sideInputFromOtherStages.add(sideInputReader);
  }

  /**
   * Add OutputCollector of a parent task that will provide intra-stage side input.
   *
   * @param ocAsSideInput OutputCollector of a parent task with side input.
   */
  public void addSideInputFromThisStage(final OutputCollectorImpl ocAsSideInput) {
    sideInputFromThisStage.add(ocAsSideInput);
  }

  /**
   * Set OutputCollector of this task.
   *
   * @param oc OutputCollector of this task.
   */
  public void setOutputCollector(final OutputCollectorImpl oc) {
    outputCollector = oc;
  }

  /**
   * Add OutputWriter of this task.
   *
   * @param outputWriter OutputWriter of this task.
   */
  public void addOutputWriter(final OutputWriter outputWriter) {
    outputWriters.add(outputWriter);
  }
}
