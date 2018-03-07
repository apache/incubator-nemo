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
 * Value of DuplicateEdgeGroupProperty.
 * If isRepresentativeEdgeDecided is false, its physical edge id(representativeEdgeId) is not yet discovered.
 * representativeEdgeId is the id of an edge that represents the group with an id of groupId.
 * groupId uniquely defines a group of edges that handle the same data.
 */
public final class DuplicateEdgeGroupPropertyValue implements Serializable {
  private static final int GROUP_SIZE_UNDECIDED = -1;
  private boolean isRepresentativeEdgeDecided;
  private String groupId;
  private String representativeEdgeId;
  private int groupSize;

  /**
   * Constructor.
   * @param groupId Group ID.
   */
  public DuplicateEdgeGroupPropertyValue(final String groupId) {
    this.isRepresentativeEdgeDecided = false;
    this.groupId = groupId;
    this.groupSize = GROUP_SIZE_UNDECIDED;
  }

  /**
   * Set physical edge id.
   * @param representativeEdgeId physical edge id of representative edge.
   */
  public void setRepresentativeEdgeId(final String representativeEdgeId) {
    if (isRepresentativeEdgeDecided && !this.representativeEdgeId.equals(representativeEdgeId)) {
      throw new RuntimeException("edge id is already decided");
    }
    this.isRepresentativeEdgeDecided = true;
    this.representativeEdgeId = representativeEdgeId;
  }

  /**
   * Set the group size.
   * @param groupSize the group size.
   */
  public void setGroupSize(final int groupSize) {
    if (groupSize <= 0) {
      throw new RuntimeException("non-positive value of groupSize is not allowed");
    }
    this.groupSize = groupSize;
  }

  /**
   * Get the physical edge id of the representative edge.
   * @return physical edge id of the representative edge.
   */
  public String getRepresentativeEdgeId() {
    if (!isRepresentativeEdgeDecided) {
      throw new RuntimeException("representativeEdgeId is not decided yet");
    }
    return representativeEdgeId;
  }

  /**
   * Get the data id.
   * @return data id.
   */
  public String getGroupId() {
    return groupId;
  }

  /**
   * Get the group size.
   * @return the group size.
   */
  public int getGroupSize() {
    if (groupSize == -1) {
      throw new RuntimeException("groupSize is not decided yet");
    }
    return groupSize;
  }

  @Override
  public String toString() {
    return String.format("DuplicateEdgeGroup(%s)", representativeEdgeId != null ? representativeEdgeId : "");
  }
}
