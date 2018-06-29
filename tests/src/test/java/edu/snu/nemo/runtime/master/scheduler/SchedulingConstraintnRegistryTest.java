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
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorSlotComplianceProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.SourceLocationAwareSchedulingProperty;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link SchedulingConstraintRegistry}.
 */
public final class SchedulingConstraintnRegistryTest {
  @Test
  public void testSchedulingConstraintRegistry() throws InjectionException {
    final SchedulingConstraintRegistry registry = Tang.Factory.getTang().newInjector()
        .getInstance(SchedulingConstraintRegistry.class);
    assertEquals(FreeSlotSchedulingConstraint.class, getConstraintOf(ExecutorSlotComplianceProperty.class, registry));
    assertEquals(ContainerTypeAwareSchedulingConstraint.class,
        getConstraintOf(ExecutorPlacementProperty.class, registry));
    assertEquals(SourceLocationAwareSchedulingConstraint.class,
        getConstraintOf(SourceLocationAwareSchedulingProperty.class, registry));
  }

  private static Class<? extends SchedulingConstraint> getConstraintOf(
      final Class<? extends VertexExecutionProperty> property, final SchedulingConstraintRegistry registry) {
    return registry.get(property)
        .orElseThrow(() -> new RuntimeException(String.format(
            "No SchedulingConstraint found for property %s", property)))
        .getClass();
  }
}
