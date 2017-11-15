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
package edu.snu.onyx.runtime.master;

import edu.snu.onyx.compiler.ir.executionproperty.vertex.ExecutorPlacementProperty;
import edu.snu.onyx.runtime.common.grpc.GrpcClient;
import edu.snu.onyx.runtime.master.resource.ContainerManager;
import edu.snu.onyx.runtime.master.resource.ResourceSpecification;
import io.grpc.ManagedChannel;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.*;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link edu.snu.onyx.runtime.master.resource.ContainerManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(GrpcClient.class)
public final class ContainerManagerTest {
  private ContainerManager containerManager;
  private int testIdNumber = 0;

  private final int DEFAULT_CAPACITY = 4;
  private final int DEFAULT_MEMORY = 10240;

  @Before
  public void setUp() {
    final GrpcClient mockedGrpcClient = mock(GrpcClient.class);
    try {
      Mockito.doReturn(mock(ManagedChannel.class)).when(mockedGrpcClient).openChannel(any());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    containerManager = new ContainerManager(mock(EvaluatorRequestor.class), mockedGrpcClient);
  }

  @Test(timeout=5000)
  public void testAllocationAfterJobCompletion() {
    // Create 3 resource specifications, {A, B, C}.
    final ResourceSpecification a =
        new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, DEFAULT_CAPACITY, DEFAULT_MEMORY);
    final ResourceSpecification b =
        new ResourceSpecification(ExecutorPlacementProperty.TRANSIENT, DEFAULT_CAPACITY, DEFAULT_MEMORY);
    final ResourceSpecification c =
        new ResourceSpecification(ExecutorPlacementProperty.RESERVED, DEFAULT_CAPACITY, DEFAULT_MEMORY);

    // Create 2 of A, 2 of B and 1 of C.
    containerManager.requestContainer(2, a);
    containerManager.requestContainer(2, b);
    containerManager.requestContainer(1, c);

    // We allocate 4 containers and start 4 executors.
    allocateResource(createMockContext());
    allocateResource(createMockContext());
    allocateResource(createMockContext());
    allocateResource(createMockContext());

    // Say the job finishes,
    // and we would like to shutdown the running executors and terminate ContainerManager.
    final Future<Boolean> terminationResult = containerManager.terminate();

    // But say, the 5th container and executor was only allocated by this point.
    allocateResource(createMockContext());

    try {
      assertTrue(terminationResult.get());
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  private AllocatedEvaluator createMockEvaluator() {
    return mock(AllocatedEvaluator.class);
  }

  private ActiveContext createMockContext() {
    final ActiveContext mockedContext = mock(ActiveContext.class);
    when(mockedContext.getId()).thenReturn("TestContext" + testIdNumber++);

    return mockedContext;
  }

  private void allocateResource(final ActiveContext mockContext) {
    containerManager.onContainerAllocated(mockContext.getId(), createMockEvaluator(), null);
    containerManager.onExecutorLaunched(mockContext);
  }
}
