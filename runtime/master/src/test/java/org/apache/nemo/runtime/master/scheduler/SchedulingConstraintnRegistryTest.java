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
package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceLocalityProperty;
import org.apache.nemo.runtime.master.BlockManagerMaster;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link SchedulingConstraintRegistry}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockManagerMaster.class})
public final class SchedulingConstraintnRegistryTest {
  @Test
  public void testSchedulingConstraintRegistry() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(BlockManagerMaster.class, mock(BlockManagerMaster.class));
    final SchedulingConstraintRegistry registry =
        injector.getInstance(SchedulingConstraintRegistry.class);
    assertEquals(FreeSlotSchedulingConstraint.class, getConstraintOf(ResourceSlotProperty.class, registry));
    assertEquals(ContainerTypeAwareSchedulingConstraint.class,
        getConstraintOf(ResourcePriorityProperty.class, registry));
    assertEquals(LocalitySchedulingConstraint.class,
        getConstraintOf(ResourceLocalityProperty.class, registry));
  }

  private static Class<? extends SchedulingConstraint> getConstraintOf(
      final Class<? extends VertexExecutionProperty> property, final SchedulingConstraintRegistry registry) {
    return registry.get(property)
        .orElseThrow(() -> new RuntimeException(String.format(
            "No SchedulingConstraint found for property %s", property)))
        .getClass();
  }
}
