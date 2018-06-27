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

import edu.snu.nemo.common.ir.executionproperty.VertexExecutionProperty;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * (WARNING) Implementations of this interface must be thread-safe.
 *
 * @param <T> {@link VertexExecutionProperty} associated with the policy
 */
@DriverSide
@ThreadSafe
@FunctionalInterface
@DefaultImplementation(CompositeSchedulingConstraint.class)
public interface SchedulingConstraint<T extends VertexExecutionProperty> {
  boolean testSchedulability(final ExecutorRepresenter executor, final Task task);

  /**
   * Registry for {@link SchedulingConstraint}.
   */
  @DriverSide
  @ThreadSafe
  final class Registry {
    private final Map<Type, SchedulingConstraint> typeToSchedulingConstraintMap = new ConcurrentHashMap<>();

    @Inject
    private Registry(final ContainerTypeAwareSchedulingConstraint containerTypeAwareSchedulingConstraint,
                     final FreeSlotSchedulingConstraint freeSlotSchedulingConstraint,
                     final SourceLocationAwareSchedulingConstraint sourceLocationAwareSchedulingConstraint) {
      registerSchedulingConstraint(containerTypeAwareSchedulingConstraint);
      registerSchedulingConstraint(freeSlotSchedulingConstraint);
      registerSchedulingConstraint(sourceLocationAwareSchedulingConstraint);
    }

    /**
     * Registers a {@link SchedulingConstraint}.
     * @param policy the policy to register
     */
    public void registerSchedulingConstraint(final SchedulingConstraint policy) {
      boolean added = false;
      for (final Type interfaceType : policy.getClass().getGenericInterfaces()) {
        if (!(interfaceType instanceof ParameterizedType)) {
          continue;
        }
        final ParameterizedType type = (ParameterizedType) interfaceType;
        if (!type.getRawType().equals(SchedulingConstraint.class)) {
          continue;
        }
        final Type[] typeArguments = type.getActualTypeArguments();
        if (typeArguments.length != 1) {
          throw new RuntimeException(String.format("SchedulingConstraint %s has wrong number of type parameters.",
              policy.getClass()));
        }
        final Type executionPropertyType = typeArguments[0];
        if (typeToSchedulingConstraintMap.putIfAbsent(executionPropertyType, policy) != null) {
          throw new RuntimeException(String.format("Multiple SchedulingConstraint for ExecutionProperty %s: %s, %s",
              executionPropertyType, typeToSchedulingConstraintMap.get(executionPropertyType), policy));
        }
        added = true;
        break;
      }
      if (!added) {
        throw new RuntimeException(String.format("Cannot register SchedulingConstraint %s", policy));
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
}
