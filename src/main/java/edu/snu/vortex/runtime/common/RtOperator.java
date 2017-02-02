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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Runtime Operator.
 * @param <I>
 * @param <O>
 */
public final class RtOperator<I, O> implements Serializable {
  private final String rtOpId;
  private final Map<RtAttributes.RtOpAttribute, Object> rtOpAttr;

  /**
   * Map of <ID, {@link RtOpLink}> connecting previous {@link RtOperator} to this {@link RtOperator}.
   */
  private Map<String, RtOpLink> inputLinks;

  /**
   * Map of <ID, {@link RtOpLink}> connecting this {@link RtOperator} to the next {@link RtOperator}.
   */
  private Map<String, RtOpLink> outputLinks;

  public RtOperator(final String irOpId, final Map<RtAttributes.RtOpAttribute, Object> rtOpAttr) {
    this.rtOpId = IdGenerator.generateRtOpId(irOpId);
    this.rtOpAttr = rtOpAttr;
    this.inputLinks = new HashMap<>();
    this.outputLinks = new HashMap<>();
  }

  public String getId() {
    return rtOpId;
  }

  public void addAttrbute(final RtAttributes.RtOpAttribute key, final Object value) {
    rtOpAttr.put(key, value);
  }

  public void removeAttrbute(final RtAttributes.RtOpAttribute key) {
    rtOpAttr.remove(key);
  }

  public Map<RtAttributes.RtOpAttribute, Object> getRtOpAttr() {
    return rtOpAttr;
  }

  public void addOutputLink(final RtOpLink rtOpLink) {
    if (outputLinks.containsKey(rtOpLink.getRtOpLinkId())) {
      throw new RuntimeException("the given link is already in the output link list");
    }
    outputLinks.put(rtOpLink.getRtOpLinkId(), rtOpLink);
  }

  public void addInputLink(final RtOpLink rtOpLink) {
    if (inputLinks.containsKey(rtOpLink.getRtOpLinkId())) {
      throw new RuntimeException("the given link is already in the input link list");
    }
    inputLinks.put(rtOpLink.getRtOpLinkId(), rtOpLink);
  }

  @Override
  public String toString() {
    return "RtOperator{" +
        "rOpId='" + rtOpId + '\'' +
        ", rtOpAttr=" + rtOpAttr +
        ", inputLinks=" + inputLinks +
        ", outputLinks=" + outputLinks +
        '}';
  }
}
