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

import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-Task data handler.
 * This is a wrapper class that handles data transfer of a Task.
 * As Task input is processed element-wise, Task output element percolates down
 * through the DAG of children TaskDataHandlers.
 */
public final class IRVertexDataHandler {
  private final IRVertex irVertex;
  private List<IRVertexDataHandler> children;
  private final List<OutputCollectorImpl> inputFromThisStage;
  private final List<InputReader> sideInputFromOtherStages;
  private final List<OutputCollectorImpl> sideInputFromThisStage;
  private OutputCollectorImpl outputCollector;
  private final List<OutputWriter> outputWriters;

  /**
   * IRVertexDataHandler Constructor.
   *
   * @param irVertex Task of this IRVertexDataHandler.
   */
  public IRVertexDataHandler(final IRVertex irVertex) {
    this.irVertex = irVertex;
    this.children = new ArrayList<>();
    this.inputFromThisStage = new ArrayList<>();
    this.sideInputFromOtherStages = new ArrayList<>();
    this.sideInputFromThisStage = new ArrayList<>();
    this.outputCollector = null;
    this.outputWriters = new ArrayList<>();
  }

  /**
   * Get the irVertex that owns this IRVertexDataHandler.
   *
   * @return irVertex of this IRVertexDataHandler.
   */
  public IRVertex getIRVertex() {
    return irVertex;
  }

  /**
   * Get a DAG of children tasks' TaskDataHandlers.
   *
   * @return DAG of children tasks' TaskDataHandlers.
   */
  public List<IRVertexDataHandler> getChildren() {
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
   * Get OutputCollector of this irVertex.
   *
   * @return OutputCollector of this irVertex.
   */
  public OutputCollectorImpl getOutputCollector() {
    return outputCollector;
  }

  /**
   * Get OutputWriters of this irVertex.
   *
   * @return OutputWriters of this irVertex.
   */
  public List<OutputWriter> getOutputWriters() {
    return outputWriters;
  }

  /**
   * Set a DAG of children tasks' DataHandlers.
   *
   * @param childrenDataHandler list of children TaskDataHandlers.
   */
  public void setChildrenDataHandler(final List<IRVertexDataHandler> childrenDataHandler) {
    children = childrenDataHandler;
  }

  /**
   * Add OutputCollector of a parent irVertex that will provide intra-stage input.
   *
   * @param input OutputCollector of a parent irVertex.
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
   * Add OutputCollector of a parent irVertex that will provide intra-stage side input.
   *
   * @param ocAsSideInput OutputCollector of a parent irVertex with side input.
   */
  public void addSideInputFromThisStage(final OutputCollectorImpl ocAsSideInput) {
    sideInputFromThisStage.add(ocAsSideInput);
  }

  /**
   * Set OutputCollector of this irVertex.
   *
   * @param oc OutputCollector of this irVertex.
   */
  public void setOutputCollector(final OutputCollectorImpl oc) {
    outputCollector = oc;
  }

  /**
   * Add OutputWriter of this irVertex.
   *
   * @param outputWriter OutputWriter of this irVertex.
   */
  public void addOutputWriter(final OutputWriter outputWriter) {
    outputWriters.add(outputWriter);
  }
}
