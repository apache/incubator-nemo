package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.ir.executionproperty.AssociatedProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
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
      final LocalitySchedulingConstraint localitySchedulingConstraint,
      final SkewnessAwareSchedulingConstraint skewnessAwareSchedulingConstraint,
      final NodeShareSchedulingConstraint nodeShareSchedulingConstraint) {
    registerSchedulingConstraint(containerTypeAwareSchedulingConstraint);
    registerSchedulingConstraint(freeSlotSchedulingConstraint);
    registerSchedulingConstraint(localitySchedulingConstraint);
    registerSchedulingConstraint(skewnessAwareSchedulingConstraint);
    registerSchedulingConstraint(nodeShareSchedulingConstraint);
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
