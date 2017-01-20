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
package edu.snu.vortex.runtime.common;

import java.util.HashSet;
import java.util.Set;

public final class RtStageLink {
  private final String rtStageLinkId;
  private final RtStage srcStage;
  private final RtStage dstStage;

  /**
   * Set of {@link RtOpLink} between pairs of {@link RtOperator},
   * each part of the pair in {@link RtStageLink#srcStage} and {@link RtStageLink#dstStage}
   */
  private final Set<RtOpLink> rtOpLinkSet;

  /**
   * Represents the connection/edge between two stages to be executed in Vortex runtime.
   * A unique instance of {@link RtStageLink} must exist between {@param srcStage} and {@param dstStage}.
   * @param rtStageLinkId ID given to this {@link RtStageLink} between
   * @param srcStage and
   * @param dstStage
   */
  public RtStageLink(final String rtStageLinkId, RtStage srcStage, RtStage dstStage) {
    this.rtStageLinkId = rtStageLinkId;
    this.srcStage = srcStage;
    this.dstStage = dstStage;
    this.rtOpLinkSet = new HashSet<>();
  }

  /**
   * Connects two operators, each in {@link RtStageLink#srcStage} and {@link RtStageLink#dstStage}.
   * @param rtOpLink The {@link RtOpLink} between the two operators
   */
  public void addRtOpLink(final RtOpLink rtOpLink) {
    rtOpLinkSet.add(rtOpLink);
  }

  public Set<RtOpLink> getRtOpLinks() {
    return rtOpLinkSet;
  }

  public String getId() {
    return rtStageLinkId;
  }

  public RtStage getSrcStage() {
    return srcStage;
  }

  public RtStage getDstStage() {
    return dstStage;
  }

  @Override
  public String toString() {
    return "RtStageLink{" +
        "rtStageLinkId='" + rtStageLinkId + '\'' +
        ", srcStage=" + srcStage.getId() +
        ", dstStage=" + dstStage.getId() +
        ", rtOpLinkSet=" + rtOpLinkSet +
        '}';
  }
}
