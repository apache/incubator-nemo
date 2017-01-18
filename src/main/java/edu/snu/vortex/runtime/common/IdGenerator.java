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


import java.util.concurrent.atomic.AtomicInteger;

public final class IdGenerator {
  private static AtomicInteger RStageIdGenerator = new AtomicInteger(1);
  private static AtomicInteger ROpLinkIdGenerator = new AtomicInteger(1);

  /**
   * Generates the ID for {@link RtOperator},
   * given the ID of the corresponding {@link edu.snu.vortex.compiler.ir.operator.Operator}
   * @param irOpId
   * @return the generated ID
   */
  public static String generateRtOpId(final String irOpId) {
    return "ROp-" + irOpId;
  }

  /**
   * Generates the ID for {@link RtOpLink}.
   * @return the generated ID
   */
  public static String generateRtOpLinkId() {
    return "RtOpLink-" + ROpLinkIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link RtStage}.
   * @return the generated ID
   */
  public static String generateRtStageId() {
    return "RtStage-" + RStageIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link RtStageLink}, generating a unique ID for a pair of {@link RtStage}.
   * @return the generated ID
   */
  public static String generateRtStageLinkId(final String srcRStageId, final String dstRStageId) {
    return "RtStageLink-" + srcRStageId + '_' + dstRStageId;
  }
}
