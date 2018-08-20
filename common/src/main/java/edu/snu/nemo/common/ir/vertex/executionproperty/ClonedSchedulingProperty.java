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
package edu.snu.nemo.common.ir.vertex.executionproperty;

import edu.snu.nemo.common.ir.executionproperty.VertexExecutionProperty;

import java.io.Serializable;

/**
 * Specifies cloned execution of a vertex.
 *
 * A major limitations of the current implementation:
 * *ALL* of the clones are always scheduled immediately
 */
public final class ClonedSchedulingProperty extends VertexExecutionProperty<ClonedSchedulingProperty.CloneConf> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private ClonedSchedulingProperty(final CloneConf value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   * @param conf value of the new execution property.
   * @return the newly created execution property.
   */
  public static ClonedSchedulingProperty of(final CloneConf conf) {
    if (conf.getFractionToWaitFor() >= 1.0 || conf.getFractionToWaitFor() < 0) {
      throw new IllegalArgumentException(String.valueOf(conf.getFractionToWaitFor()));
    }
    if (conf.getMedianTimeMultiplier() >= 1.0 || conf.getMedianTimeMultiplier() < 0) {
      throw new IllegalArgumentException(String.valueOf(conf.getMedianTimeMultiplier()));
    }
    return new ClonedSchedulingProperty(conf);
  }

  public class CloneConf implements Serializable {
    // Fraction of tasks to wait for completion, before trying to clone.
    private final float fractionToWaitFor;

    // How many times slower is a task than the median, in order to be cloned.
    private final float medianTimeMultiplier;

    public CloneConf(final float fractionToWaitFor, final float medianTimeMultiplier) {
      this.fractionToWaitFor = fractionToWaitFor;
      this.medianTimeMultiplier = medianTimeMultiplier;
    }

    public float getFractionToWaitFor() {
      return fractionToWaitFor;
    }

    public float getMedianTimeMultiplier(){
      return medianTimeMultiplier;
    }
  }
}
