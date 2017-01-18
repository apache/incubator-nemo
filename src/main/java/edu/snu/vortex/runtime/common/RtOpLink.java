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

import java.util.Map;

public final class RtOpLink {
  private final String rtOpLinkId;
  private final Map<RtAttributes.RtOpLinkAttribute, Object> rtOpLinkAttr;

  private final RtOperator srcRtOp;
  private final RtOperator dstRtOp;

  /**
   * Represents a connection/edge between two {@link RtOperator} to be executed in Vortex runtime.
   * @param srcRtOp
   * @param dstRtOp
   * @param rtOpLinkAttr attributes that can be given to this {@link RtOpLink}
   */
  public RtOpLink(final RtOperator srcRtOp,
                  final RtOperator dstRtOp,
                  final Map<RtAttributes.RtOpLinkAttribute, Object> rtOpLinkAttr) {
    this.rtOpLinkId = IdGenerator.generateRtOpLinkId();
    this.srcRtOp = srcRtOp;
    this.dstRtOp = dstRtOp;
    this.rtOpLinkAttr = rtOpLinkAttr;
  }

  public String getRtOpLinkId() {
    return rtOpLinkId;
  }

  public RtOperator getSrcRtOp() {
    return srcRtOp;
  }

  public RtOperator getDstRtOp() {
    return dstRtOp;
  }

  public Map<RtAttributes.RtOpLinkAttribute, Object> getRtOpLinkAttr() {
    return rtOpLinkAttr;
  }

  @Override
  public String toString() {
    return "RtOpLink{" +
        "rtOpLinkId='" + rtOpLinkId + '\'' +
        ", rtOpLinkAttr=" + rtOpLinkAttr +
        ", srcRtOp=" + srcRtOp.getId() +
        ", dstRtOp=" + dstRtOp.getId() +
        '}';
  }
}
