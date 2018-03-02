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
package edu.snu.nemo.common.ir.edge.executionproperty;

import java.io.Serializable;

/**
 * Value of DuplicateDataProperty.
 * If decided is false, its physical edge id(edgeId) is not yet discovered.
 * edgeId will be set in PhysicalPlanGenerator.
 * dataId is just used for marking edges that holds same data.
 */
public final class DuplicateDataPropertyValue implements Serializable {
  private boolean decided = false;
  private String dataId;
  private String edgeId;
  private int duplicateCount = -1;

  /**
   * Constructor.
   */
  public DuplicateDataPropertyValue(final String dataId) {
    this.dataId = dataId;
  }

  /**
   * Set physical edge id.
   * @param edgeId physical edge id.
   */
  public void setEdgeId(final String edgeId) {
    this.decided = true;
    this.edgeId = edgeId;
  }

  /**
   * Set the number of duplicate data.
   * @param duplicateCount number of duplicate data.
   */
  public void setDuplicateCount(final int duplicateCount) {
    if (duplicateCount < 0) {
      throw new RuntimeException("negative value of duplicateCount is not allowed");
    }
    this.duplicateCount = duplicateCount;
  }

  /**
   * Get the physical edge id.
   * @return physical edge id.
   */
  public String getEdgeId() {
    if (!decided) {
      throw new RuntimeException("edgeId is not decided yet");
    }
    return edgeId;
  }

  /**
   * Get the data id.
   * @return data id.
   */
  public String getDataId() {
    return dataId;
  }

  /**
   * Get the number of duplicate data.
   * @return number of duplicate data.
   */
  public int getDuplicateCount() {
    if (duplicateCount == -1) {
      throw new RuntimeException("duplicateCount is not decided yet");
    }
    return duplicateCount;
  }

  @Override
  public String toString() {
    return String.format("DuplicateData(%s)", edgeId != null ? edgeId : "");
  }
}
