/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

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
    return new ClonedSchedulingProperty(conf);
  }

  /**
   * Configurations for cloning.
   * TODO #199: Slot-aware cloning
   */
  public static final class CloneConf implements Serializable {
    // Always clone, upfront.
    private final boolean upFrontCloning;

    // Fraction of tasks to wait for completion, before trying to clone.
    // If this value is 0, then we always clone.
    private final double fractionToWaitFor;

    // How many times slower is a task than the median, in order to be cloned.
    private final double medianTimeMultiplier;

    /**
     * Always clone, upfront.
     */
    public CloneConf() {
      this.upFrontCloning = true;
      this.fractionToWaitFor = 0.0;
      this.medianTimeMultiplier = 0.0;
    }

    /**
     * Clone stragglers judiciously.
     * @param fractionToWaitFor before trying to clone.
     * @param medianTimeMultiplier to identify stragglers.
     */
    public CloneConf(final double fractionToWaitFor, final double medianTimeMultiplier) {
      if (fractionToWaitFor >= 1.0 || fractionToWaitFor <= 0) {
        throw new IllegalArgumentException(String.valueOf(fractionToWaitFor));
      }
      if (medianTimeMultiplier < 1.0) {
        throw new IllegalArgumentException(String.valueOf(medianTimeMultiplier));
      }
      this.upFrontCloning = false;
      this.fractionToWaitFor = fractionToWaitFor;
      this.medianTimeMultiplier = medianTimeMultiplier;
    }

    /**
     * @return fractionToWaitFor.
     */
    public double getFractionToWaitFor() {
      return fractionToWaitFor;
    }

    /**
     * @return medianTimeMultiplier.
     */
    public double getMedianTimeMultiplier() {
      return medianTimeMultiplier;
    }

    /**
     * @return true if it is upfront cloning.
     */
    public boolean isUpFrontCloning() {
      return upFrontCloning;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("upfront: ");
      sb.append(upFrontCloning);
      sb.append(" / fraction: ");
      sb.append(fractionToWaitFor);
      sb.append(" / multiplier: ");
      sb.append(medianTimeMultiplier);
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CloneConf cloneConf = (CloneConf) o;

      return new EqualsBuilder()
        .append(isUpFrontCloning(), cloneConf.isUpFrontCloning())
        .append(getFractionToWaitFor(), cloneConf.getFractionToWaitFor())
        .append(getMedianTimeMultiplier(), cloneConf.getMedianTimeMultiplier())
        .isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder(17, 37)
        .append(isUpFrontCloning())
        .append(getFractionToWaitFor())
        .append(getMedianTimeMultiplier())
        .toHashCode();
    }
  }
}
