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

  public List<OutputCollectorImpl> getInputFromThisStage() {
    return inputFromThisStage;
  }

  public List<InputReader> getSideInputFromOtherStages() {
    return sideInputFromOtherStages;
  }

  public List<OutputCollectorImpl> getSideInputFromThisStage() {
    return sideInputFromThisStage;
  }

  public OutputCollectorImpl getOutputCollector() {
    return outputCollector;
  }

  public List<OutputWriter> getOutputWriters() {
    return outputWriters;
  }

  public void setChildrenDataHandler(final List<TaskDataHandler> childrenDataHandler) {
    children = childrenDataHandler;
  }

  // Add OutputCollectors of parent tasks.
  public void addInputFromThisStages(final OutputCollectorImpl input) {
    inputFromThisStage.add(input);
  }

  public void addSideInputFromOtherStages(final InputReader sideInputReader) {
    sideInputFromOtherStages.add(sideInputReader);
  }

  public void addSideInputFromThisStage(final OutputCollectorImpl ocAsSideInput) {
    sideInputFromThisStage.add(ocAsSideInput);
  }

  // Set OutputCollector of this task.
  // Mark if the data from this OutputCollector is used as side input.
  public void setOutputCollector(final OutputCollectorImpl oc) {
    outputCollector = oc;
  }

  public void addOutputWriters(final OutputWriter outputWriter) {
    outputWriters.add(outputWriter);
  }
}
