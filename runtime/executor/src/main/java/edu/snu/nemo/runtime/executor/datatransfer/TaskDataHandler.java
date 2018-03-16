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

import edu.snu.nemo.runtime.common.plan.physical.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-Task data handler.
 * This is a wrapper class that handles data transfer of a Task.
 * As TaskGroup input is processed element-wise, Task output element percolates down
 * through the DAG of children TaskDataHandlers.
 */
public final class TaskDataHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TaskDataHandler.class.getName());

  public TaskDataHandler(final Task task) {
    this.task = task;
    this.children = new ArrayList<>();
    this.inputFromThisStage = new ArrayList<>();
    this.sideInputFromOtherStages = new ArrayList<>();
    this.sideInputFromThisStage = new ArrayList<>();
    this.outputCollector = null;
    this.outputWriters = new ArrayList<>();
  }

  private final Task task;
  private List<TaskDataHandler> children;
  private final List<OutputCollectorImpl> inputFromThisStage;
  private final List<InputReader> sideInputFromOtherStages;
  private final List<OutputCollectorImpl> sideInputFromThisStage;
  private OutputCollectorImpl outputCollector;
  private final List<OutputWriter> outputWriters;

  public Task getTask() {
    return task;
  }

  public List<TaskDataHandler> getChildren() {
    return children;
  }

  /**
   * Get intra-TaskGroup input from parent tasks.
   * We keep parent tasks' OutputCollectors, as they're the place where parent task output
   * becomes available element-wise.
   * @return OutputCollectors of all parent tasks.
   */
  public List<OutputCollectorImpl> getInputFromThisStage() {
    return inputFromThisStage;
  }

  /**
   * Get side input from other TaskGroup.
   * @return InputReader that has side input.
   */
  public List<InputReader> getSideInputFromOtherStages() {
    return sideInputFromOtherStages;
  }

  /**
   * Get intra-TaskGroup side input from parent tasks.
   * Just like normal intra-TaskGroup inputs, intra-TaskGroup side inputs are
   * collected in parent tasks' OutputCollectors.
   * @return OutputCollectors of all parent tasks which are marked as having side input.
   */
  public List<OutputCollectorImpl> getSideInputFromThisStage() {
    return sideInputFromThisStage;
  }

  /**
   * Get OutputCollector of this task.
   * @return OutputCollector of this task.
   */
  public OutputCollectorImpl getOutputCollector() {
    return outputCollector;
  }

  /**
   * Get OutputWriters of this task.
   * @return OutputWriters of this task.
   */
  public List<OutputWriter> getOutputWriters() {
    return outputWriters;
  }

  /**
   * Set a DAG of children tasks' DataHandlers.
   */
  public void setChildrenDataHandler(final List<TaskDataHandler> childrenDataHandler) {
    children = childrenDataHandler;
  }

  /**
   * Add OutputCollector of a parent task.
   */
  public void addInputFromThisStages(final OutputCollectorImpl input) {
    inputFromThisStage.add(input);
  }

  /**
   * Add InputReader that contains side input from other TaskGroup.
   */
  public void addSideInputFromOtherStages(final InputReader sideInputReader) {
    sideInputFromOtherStages.add(sideInputReader);
  }

  /**
   * Add parent OutputCollector that contains side input from the parent task.
   */
  public void addSideInputFromThisStage(final OutputCollectorImpl ocAsSideInput) {
    sideInputFromThisStage.add(ocAsSideInput);
  }

  /**
   * Set OutputCollector of this task.
   */
  public void setOutputCollector(final OutputCollectorImpl oc) {
    outputCollector = oc;
  }

  /**
   * Add OutputWriter of this task.
   */
  public void addOutputWriters(final OutputWriter outputWriter) {
    outputWriters.add(outputWriter);
  }
}
