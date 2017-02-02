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

import edu.snu.vortex.runtime.exception.NoSuchRtStageException;
import edu.snu.vortex.utils.DAG;
import edu.snu.vortex.utils.DAGImpl;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Execution Plan.
 */
public final class ExecutionPlan {
  private static final Logger LOG = Logger.getLogger(ExecutionPlan.class.getName());

  /**
   * A list of {@link RtStage} to be executed in this plan, sorted in topological order.
   */
  private final DAG<RtStage> rtStages;

  /**
   * Map of <ID, {@link RtStageLink}> connecting the {@link ExecutionPlan#rtStages} contained in this plan.
   */
  private final Map<String, RtStageLink> rtStageLinks;

  /**
   * An execution plan for Vortex runtime.
   */
  public ExecutionPlan() {
    this.rtStages = new DAGImpl<>();
    this.rtStageLinks = new HashMap<>();
  }

  /**
   * Adds a {@link RtStage} to this plan.
   * @param rtStage to be added
   */
  public void addRtStage(final RtStage rtStage) {
    if (!rtStages.addVertex(rtStage)) {
      LOG.log(Level.FINE, "RtStage {0} already exists", rtStage.getId());
    }
  }

  /**
   * Connects two {@link RtStage} in the plan.
   * There can be multiple {@link RtOpLink} in a unique {@link RtStageLink} connecting the two stages.
   * @param srcRtStage .
   * @param dstRtStage .
   * @param rtOpLink that connects two {@link RtOperator} each in {@param srcRtStage} and {@param dstRtStage}.
   */
  public void connectRtStages(final RtStage srcRtStage,
                              final RtStage dstRtStage,
                              final RtOpLink rtOpLink) {
    try {
      rtStages.addEdge(srcRtStage, dstRtStage);
    } catch (final NoSuchElementException e) {
      throw new NoSuchRtStageException("The requested RtStage does not exist in this ExecutionPlan");
    }

    final String rtStageLinkId = IdGenerator.generateRtStageLinkId(srcRtStage.getId(), dstRtStage.getId());
    RtStageLink rtStageLink = rtStageLinks.get(rtStageLinkId);

    if (rtStageLink == null) {
      rtStageLink = new RtStageLink(rtStageLinkId, srcRtStage, dstRtStage);
      rtStageLinks.put(rtStageLinkId, rtStageLink);
    }
    rtStageLink.addRtOpLink(rtOpLink);

    srcRtStage.addOutputRtStageLink(rtStageLink);
    dstRtStage.addInputRtStageLink(rtStageLink);
  }

  public Set<RtStage> getNextRtStagesToExecute() {
    return rtStages.getRootVertices();
  }

  public boolean removeCompleteStage(final RtStage rtStageToRemove) {
    return rtStages.removeVertex(rtStageToRemove);
  }

  public Map<String, RtStageLink> getRtStageLinks() {
    return rtStageLinks;
  }

  @Override
  public String toString() {
    return "RtStages: " + rtStages + " / RtStageLinks: " + rtStageLinks;
  }
}
