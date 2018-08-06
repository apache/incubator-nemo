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
package edu.snu.nemo.runtime.master.scheduler;

import edu.snu.nemo.common.ir.executionproperty.AssociatedProperty;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for {@link SchedulingConstraint}.
 */
@DriverSide
@ThreadSafe
public final class SchedulingConstraintRegistry {
  private final Map<Type, SchedulingConstraint> typeToSchedulingConstraintMap = new ConcurrentHashMap<>();

  @Inject
  private SchedulingConstraintRegistry(
      final ContainerTypeAwareSchedulingConstraint containerTypeAwareSchedulingConstraint,
      final FreeSlotSchedulingConstraint freeSlotSchedulingConstraint,
      final SourceLocationAwareSchedulingConstraint sourceLocationAwareSchedulingConstraint,
      final SkewnessAwareSchedulingConstraint skewnessAwareSchedulingConstraint,
      final NodeShareSchedulingConstraint nodeShareSchedulingConstraint,
      final IntermediateDataLocationAwareSchedulingConstraint intermediateDataLocationAwareSchedulingConstraint) {
    registerSchedulingConstraint(containerTypeAwareSchedulingConstraint);
    registerSchedulingConstraint(freeSlotSchedulingConstraint);
    registerSchedulingConstraint(sourceLocationAwareSchedulingConstraint);
    registerSchedulingConstraint(nodeShareSchedulingConstraint);
    registerSchedulingConstraint(skewnessAwareSchedulingConstraint);
    registerSchedulingConstraint(intermediateDataLocationAwareSchedulingConstraint);
  }

  /**
   * Registers a {@link SchedulingConstraint}.
   * @param policy the policy to register
   */
  public void registerSchedulingConstraint(final SchedulingConstraint policy) {
    final AssociatedProperty associatedProperty = policy.getClass().getAnnotation(AssociatedProperty.class);
    if (associatedProperty == null || associatedProperty.value() == null) {
      throw new RuntimeException(String.format("SchedulingConstraint %s has no associated VertexExecutionProperty",
          policy.getClass()));
    }
    final Class<? extends ExecutionProperty> property = associatedProperty.value();
    if (typeToSchedulingConstraintMap.putIfAbsent(property, policy) != null) {
      throw new RuntimeException(String.format("Multiple SchedulingConstraint for VertexExecutionProperty %s:"
          + "%s, %s", property, typeToSchedulingConstraintMap.get(property), policy));
    }
  }

  /**
   * Returns {@link SchedulingConstraint} for the given {@link VertexExecutionProperty}.
   * @param propertyClass {@link VertexExecutionProperty} class
   * @return the corresponding {@link SchedulingConstraint} object,
   *         or {@link Optional#EMPTY} if no such policy was found
   */
  public Optional<SchedulingConstraint> get(final Class<? extends VertexExecutionProperty> propertyClass) {
    return Optional.ofNullable(typeToSchedulingConstraintMap.get(propertyClass));
  }
}
