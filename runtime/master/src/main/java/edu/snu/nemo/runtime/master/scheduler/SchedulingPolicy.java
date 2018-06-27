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
import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.inject.Inject;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A function to select an executor from collection of available executors.
 *
 * @param <T> {@link VertexExecutionProperty} associated with the policy
 */
@DriverSide
@ThreadSafe
@FunctionalInterface
@DefaultImplementation(MinOccupancyFirstSchedulingPolicy.class)
public interface SchedulingPolicy<T extends VertexExecutionProperty> {
  /**
   * A function to select an executor from the specified collection of available executors.
   *
   * @param executors The collection of available executors.
   *                  Implementations can assume that the collection is not empty.
   * @param task The task to schedule
   * @return The selected executor. It must be a member of {@code executors}.
   */
  ExecutorRepresenter selectExecutor(final Collection<ExecutorRepresenter> executors, final Task task);

  /**
   * Registry for {@link SchedulingPolicy}.
   */
  @DriverSide
  @ThreadSafe
  final class Registry {
    private final Map<Type, SchedulingPolicy> typeToSchedulingPolicyMap = new ConcurrentHashMap<>();

    @Inject
    private Registry(final MinOccupancyFirstSchedulingPolicy minOccupancyFirstSchedulingPolicy) {
      registerSchedulingPolicy(minOccupancyFirstSchedulingPolicy);
    }

    /**
     * Registers a {@link SchedulingPolicy}.
     * @param policy the policy to register
     */
    public void registerSchedulingPolicy(final SchedulingPolicy policy) {
      boolean added = false;
      for (final Type interfaceType : policy.getClass().getGenericInterfaces()) {
        if (!(interfaceType instanceof ParameterizedType)) {
          continue;
        }
        final ParameterizedType type = (ParameterizedType) interfaceType;
        if (!type.getRawType().equals(SchedulingPolicy.class)) {
          continue;
        }
        final Type[] typeArguments = type.getActualTypeArguments();
        if (typeArguments.length != 1) {
          throw new RuntimeException(String.format("SchedulingPolicy %s has wrong number of type parameters.",
              policy.getClass()));
        }
        final Type executionPropertyType = typeArguments[0];
        if (typeToSchedulingPolicyMap.putIfAbsent(executionPropertyType, policy) != null) {
          throw new RuntimeException(String.format("Multiple SchedulingPolicy for ExecutionProperty %s: %s, %s",
              executionPropertyType, typeToSchedulingPolicyMap.get(executionPropertyType), policy));
        }
        added = true;
        break;
      }
      if (!added) {
        throw new RuntimeException(String.format("Cannot register SchedulingPolicy %s", policy));
      }
    }

    /**
     * Returns {@link SchedulingPolicy} for the given {@link VertexExecutionProperty}.
     * @param propertyClass {@link VertexExecutionProperty} class
     * @return the corresponding {@link SchedulingPolicy} object, or {@link Optional#EMPTY} if no such policy was found
     */
    public Optional<SchedulingPolicy> get(final Class<? extends VertexExecutionProperty> propertyClass) {
      return Optional.ofNullable(typeToSchedulingPolicyMap.get(propertyClass));
    }
  }
}
