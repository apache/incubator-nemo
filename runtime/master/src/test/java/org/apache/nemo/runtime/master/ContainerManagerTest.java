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
package org.apache.nemo.runtime.master;

import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.master.resource.ContainerManager;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.nemo.common.ir.executionproperty.ResourceSpecification;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link org.apache.nemo.runtime.master.resource.ContainerManager}.
 */
public final class ContainerManagerTest {
  private static final ResourceSpecification RESOURCE_SPEC_A =
    new ResourceSpecification(ResourcePriorityProperty.COMPUTE, 1, 1024);
  private static final ResourceSpecification RESOURCE_SPEC_B =
    new ResourceSpecification(ResourcePriorityProperty.TRANSIENT, 2, 2048);
  private static final ResourceSpecification RESOURCE_SPEC_C =
    new ResourceSpecification(ResourcePriorityProperty.RESERVED, 3, 3072);

  private ContainerManager containerManager;
  private AtomicInteger testIdNumber = new AtomicInteger(0);

  private String getNodeName() {
    return "NODE-" + testIdNumber.incrementAndGet();
  }

  private String getEvaluatorId() {
    return "EVALUATOR-" + testIdNumber.incrementAndGet();
  }

  private String getExecutorId() {
    return "EXECUTOR-" + testIdNumber.incrementAndGet();
  }

  @Before
  public void setUp() throws InjectionException {

    final MessageEnvironment mockMsgEnv = mock(MessageEnvironment.class);
    when(mockMsgEnv.asyncConnect(anyString(), anyString())).thenReturn(mock(Future.class));
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
      .bindNamedParameter(JobConf.ScheduleSerThread.class, "1")
      .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    injector.bindVolatileInstance(EvaluatorRequestor.class, mock(EvaluatorRequestor.class));
    injector.bindVolatileInstance(MessageEnvironment.class, mockMsgEnv);
    containerManager = injector.getInstance(ContainerManager.class);
  }

  @Test
  public void testRequestAllocateLaunch() {
    // Create 2 of A, 2 of B and 1 of C.
    final Map<Integer, ResourceSpecification> numToSpec = new HashMap();
    numToSpec.put(2, RESOURCE_SPEC_A);
    numToSpec.put(2, RESOURCE_SPEC_B);
    numToSpec.put(1, RESOURCE_SPEC_C);

    // Request -> Allocate -> Launch
    for (final Map.Entry<Integer, ResourceSpecification> entry : numToSpec.entrySet()) {
      final int num = entry.getKey();
      final ResourceSpecification spec = entry.getValue();
      containerManager.requestContainer(num, spec);

      for (int i = 0; i < num; i++) {
        final String evaluatorId = getEvaluatorId();
        final String executorId = getExecutorId();
        final EvaluatorDescriptor descriptor = createDescriptor(spec);

        containerManager.onContainerAllocated(
          executorId,
          createMockEvaluator(evaluatorId, descriptor),
          createMockConfiguration());
        final ExecutorRepresenter executorRepresenter =
          containerManager.onContainerLaunched(createMockContext(executorId, descriptor)).get();
        assertEquals(spec.getContainerType(), executorRepresenter.getContainerType());
        assertEquals(spec.getCapacity(), executorRepresenter.getExecutorCapacity());
        assertEquals(descriptor.getNodeDescriptor().getName(), executorRepresenter.getNodeName());
      }
    }
  }

  @Test
  public void testFailureBeforeLaunch() {
    containerManager.requestContainer(1, RESOURCE_SPEC_A);
    final String evaluatorId = getEvaluatorId();

    containerManager.onContainerAllocated(
      getExecutorId(),
      createMockEvaluator(evaluatorId, createDescriptor(RESOURCE_SPEC_A)),
      createMockConfiguration());
    assertEquals(RESOURCE_SPEC_A, containerManager.onContainerFailed(evaluatorId));
  }

  @Test
  public void testFailureAfterLaunch() {
    containerManager.requestContainer(1, RESOURCE_SPEC_A);
    final String evaluatorId = getEvaluatorId();
    final String executorId = getExecutorId();
    final EvaluatorDescriptor descriptor = createDescriptor(RESOURCE_SPEC_A);

    containerManager.onContainerAllocated(
      executorId,
      createMockEvaluator(evaluatorId, descriptor),
      createMockConfiguration());
    containerManager.onContainerLaunched(createMockContext(executorId, descriptor));
    assertEquals(RESOURCE_SPEC_A, containerManager.onContainerFailed(evaluatorId));
  }

  private EvaluatorDescriptor createDescriptor(final ResourceSpecification spec) {
    final EvaluatorDescriptor descriptor = mock(EvaluatorDescriptor.class);
    when(descriptor.getMemory()).thenReturn(spec.getMemory());
    when(descriptor.getNumberOfCores()).thenReturn(spec.getCapacity());

    final NodeDescriptor node = mock(NodeDescriptor.class);
    when(node.getName()).thenReturn(getNodeName());
    when(descriptor.getNodeDescriptor()).thenReturn(node);
    return descriptor;
  }

  private AllocatedEvaluator createMockEvaluator(final String id,
                                                 final EvaluatorDescriptor descriptor) {
    final AllocatedEvaluator evaluator = mock(AllocatedEvaluator.class);
    when(evaluator.getId()).thenReturn(id);
    when(evaluator.getEvaluatorDescriptor()).thenReturn(descriptor);
    return evaluator;
  }

  private ActiveContext createMockContext(final String id,
                                          final EvaluatorDescriptor descriptor) {
    final ActiveContext mockedContext = mock(ActiveContext.class);
    when(mockedContext.getId()).thenReturn(id);
    when(mockedContext.getEvaluatorDescriptor()).thenReturn(descriptor);
    return mockedContext;
  }

  private Configuration createMockConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder().build();
  }
}
