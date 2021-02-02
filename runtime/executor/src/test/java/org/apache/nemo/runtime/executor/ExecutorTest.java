/*
  Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.Stage;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.test.EventOrWatermark;
import org.apache.nemo.common.test.TestUnboundedSourceReadable;
import org.apache.nemo.common.test.TestUnboundedSourceVertex;
import org.apache.nemo.runtime.executor.common.TaskOffloadingEvent;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.apache.nemo.runtime.executor.common.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.common.datatransfer.OutputWriter;
import org.apache.nemo.runtime.executor.task.TestDAGBuilder;
import org.apache.nemo.runtime.executor.task.util.*;
import org.apache.nemo.runtime.master.RuntimeMaster;
import org.apache.nemo.runtime.master.resource.ResourceSpecification;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link TaskExecutor}.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "javax.net.ssl.*" })
@PrepareForTest({InputReader.class, OutputWriter.class, IntermediateDataIOFactory.class, BroadcastManagerWorker.class,
  TaskStateManager.class, StageEdge.class, PersistentConnectionToMasterMap.class, Stage.class, IREdge.class})
public final class ExecutorTest {
  private static final AtomicInteger RUNTIME_EDGE_ID = new AtomicInteger(0);
  private static final ExecutionPropertyMap<VertexExecutionProperty> TASK_EXECUTION_PROPERTY_MAP
      = new ExecutionPropertyMap<>("TASK_EXECUTION_PROPERTY_MAP");
  private static final int FIRST_ATTEMPT = 0;

  private Map<String, List> runtimeEdgeToOutputData;
  private AtomicInteger stageId;

  private final Tang TANG = Tang.Factory.getTang();

  private String generateTaskId() {
    return RuntimeIdManager.generateTaskId(
        RuntimeIdManager.generateStageId(stageId.getAndIncrement()), 0, FIRST_ATTEMPT);
  }

  private MasterBuilder masterSetupHelper;
  private StateStore stateStore;
  private RuntimeMaster runtimeMaster;

  @After
  public void tearDown() throws Exception {
    masterSetupHelper.close();
  }

  @Before
  public void setUp() throws Exception {
    stageId = new AtomicInteger(1);
    runtimeEdgeToOutputData = new HashMap<>();

    masterSetupHelper = new MasterBuilder();
    runtimeMaster = masterSetupHelper.runtimeMaster;

    stateStore = new StateStore() {
      final Map<String, byte[]> stateMap = new HashMap<>();

      @Override
      public synchronized  InputStream getStateStream(String taskId) {
        return new ByteArrayInputStream(stateMap.get(taskId));
      }

      @Override
      public byte[] getBytes(String taskId) {
        return stateMap.get(taskId);
      }

      @Override
      public void put(String taskId, byte[] bytes) {
        stateMap.put(taskId, bytes);
      }

      @Override
      public synchronized boolean containsState(String taskId) {
        return stateMap.containsKey(taskId);
      }
    };
  }

  private Pair<IRVertex, Map<String, Readable>> createSource(final List<EventOrWatermark> events) {
    final Readable readable = new TestUnboundedSourceReadable(events);
    final IRVertex sourceIRVertex = new TestUnboundedSourceVertex(Collections.singletonList(readable));
    final Map<String, Readable> vertexIdToReadable = new HashMap<>();
    vertexIdToReadable.put(sourceIRVertex.getId(), readable);
    return Pair.of(sourceIRVertex, vertexIdToReadable);
  }

  @Test
  public void testOffloadingExecution() throws Exception {
    final Pair<Executor, Injector> pair1 = launchExecutor(3);
    final Pair<Executor, Injector> pair2 = launchExecutor(3);

    final int parallelism = 3;
    final TCPSourceGenerator sourceGenerator = new TCPSourceGenerator(parallelism);

    final TestDAGBuilder testDAGBuilder = new TestDAGBuilder(masterSetupHelper.planGenerator, parallelism);
    final PhysicalPlan plan = testDAGBuilder.generatePhysicalPlan(TestDAGBuilder.PlanType.TwoVertices);

    runtimeMaster.execute(plan, 1);

    Thread.sleep(2000);

    // 100
    for (int i = 0; i < 500; i++) {
      sourceGenerator.addEvent(i % parallelism, new EventOrWatermark(Pair.of(i % 5, 1)));

      if ((i) % 50 == 0) {
        for (int j = 0; j < parallelism; j++) {
          sourceGenerator.addEvent(j, new EventOrWatermark((i) + 200, true));
        }
      }
      Thread.sleep(1);
    }

    final OffloadingManager offloadingManager = pair1.right().getInstance(OffloadingManager.class);
    offloadingManager.createWorker(1);

    Thread.sleep(4000);

    final TaskExecutorMapWrapper wrapper = pair1.right().getInstance(TaskExecutorMapWrapper.class);
    final ExecutorThread executorThread = wrapper.getTaskExecutorThread("Stage1-0-0");

    executorThread.addEvent(new TaskOffloadingEvent("Stage1-0-0",
      TaskOffloadingEvent.ControlType.SEND_TO_OFFLOADING_WORKER, null));

    Thread.sleep(3000);

    // 200
    for (int i = 500; i < 1000; i++) {
      sourceGenerator.addEvent(i % parallelism, new EventOrWatermark(Pair.of(i % 5, 1)));

      if ((i) % 50 == 0) {
        for (int j = 0; j < parallelism; j++) {
          sourceGenerator.addEvent(j, new EventOrWatermark((i) + 200, true));
        }
      }
      Thread.sleep(1);
    }
    Thread.sleep(2000);

    /*
    // launch offloading executor
    final String executor1Addr = pair1.right().getInstance(ByteTransport.class).getPublicAddress();
    final int executor1Port = pair1.right().getInstance(ByteTransport.class).getBindingPort();
    final OffloadingExecutor offloadingExecutor =
      new OffloadingExecutor(3, new HashMap<>(), true,
        pair1.left().getExecutorId(), executor1Addr, executor1Port);

    offloadingExecutor.prepare(new OffloadingTransform.OffloadingContext() {
      @Override
      public StateStore getStateStore() {
        return stateStore;
      }
    }, new OffloadingOutputCollector() {
      @Override
      public void emit(Object output) {

      }
    });
    */
  }

  @Test
  public void testThreeStage() throws Exception {
    final Pair<Executor, Injector> pair1 = launchExecutor(5);
    final Pair<Executor, Injector> pair2 = launchExecutor(5);

    final int parallelism = 3;
    final TCPSourceGenerator sourceGenerator = new TCPSourceGenerator(parallelism);

    final TestDAGBuilder testDAGBuilder = new TestDAGBuilder(masterSetupHelper.planGenerator, parallelism);
    final PhysicalPlan plan = testDAGBuilder.generatePhysicalPlan(TestDAGBuilder.PlanType.ThreeVertices);

    runtimeMaster.execute(plan, 1);

    Thread.sleep(2000);

    // 100
    for (int i = 0; i < 500; i++) {
      sourceGenerator.addEvent(i % parallelism, new EventOrWatermark(Pair.of(i % 5, 1)));

      if ((i) % 50 == 0) {
        for (int j = 0; j < parallelism; j++) {
          sourceGenerator.addEvent(j, new EventOrWatermark((i) + 200, true));
        }
        // Thread.sleep(1);
      }
    }

    // move task
    Thread.sleep(3000);
    final Pair<Executor, Injector> pair3 = launchExecutor(parallelism);


    // 400
    for (int i = 500; i < 2000; i++) {
      sourceGenerator.addEvent(i % parallelism, new EventOrWatermark(Pair.of(i % 5, 1)));

      if ((i + 1) % 50 == 0) {
        for (int j = 0; j < parallelism; j++) {
          sourceGenerator.addEvent(j, new EventOrWatermark((i) + 200, true));
        }
      }

      if (i == 1000) {
        masterSetupHelper.taskScheduledMapMaster.stopTask("Stage1-0-0");
        masterSetupHelper.taskScheduledMapMaster.stopTask("Stage0-1-0");
        masterSetupHelper.taskScheduledMapMaster.stopTask("Stage2-1-0");
      }

      Thread.sleep(3);
    }


    Thread.sleep(2000);
  }

  // [stage1]  [stage2]
  // Task1 -> Task2

  // [      stage 1      ]   [ stage 2]
  // (src) -> (op flatten) -> (op noemit)
  @Test
  public void testMultipleTaskExecutors() throws Exception {

    final Pair<Executor, Injector> pair1 = launchExecutor(3);
    final Pair<Executor, Injector> pair2 = launchExecutor(3);

    final int parallelism = 3;
    final TCPSourceGenerator sourceGenerator = new TCPSourceGenerator(parallelism);

    final TestDAGBuilder testDAGBuilder = new TestDAGBuilder(masterSetupHelper.planGenerator, parallelism);
    final PhysicalPlan plan = testDAGBuilder.generatePhysicalPlan(TestDAGBuilder.PlanType.TwoVertices);

    runtimeMaster.execute(plan, 1);

    Thread.sleep(2000);

    // 100
    for (int i = 0; i < 500; i++) {
      sourceGenerator.addEvent(i % parallelism, new EventOrWatermark(Pair.of(i % 5, 1)));

      if ((i) % 50 == 0) {
        for (int j = 0; j < parallelism; j++) {
          sourceGenerator.addEvent(j, new EventOrWatermark((i) + 200, true));
        }
        // Thread.sleep(1);
      }
    }

    // move task
    Thread.sleep(3000);
    final Pair<Executor, Injector> pair3 = launchExecutor(parallelism);
    masterSetupHelper.taskScheduledMapMaster.stopTask("Stage0-0-0");
    Thread.sleep(3000);

    // 200
    for (int i = 500; i < 1000; i++) {
      sourceGenerator.addEvent(i % parallelism, new EventOrWatermark(Pair.of(i % 5, 1)));

      if ((i + 1) % 50 == 0) {
        for (int j = 0; j < parallelism; j++) {
          sourceGenerator.addEvent(j, new EventOrWatermark((i) + 200, true));
        }
        // Thread.sleep(1);
      }
    }

    Thread.sleep(3000);

    // 300: move stateful task
    masterSetupHelper.taskScheduledMapMaster.stopTask("Stage1-0-0");
    Thread.sleep(3000);


    for (int i = 1000; i < 1500; i++) {
      sourceGenerator.addEvent(i % parallelism, new EventOrWatermark(Pair.of(i % 5, 1)));

      if ((i + 1) % 50 == 0) {
        sourceGenerator.addEvent(0, new EventOrWatermark((i+1) + 200, true));
        sourceGenerator.addEvent(1, new EventOrWatermark((i+1) + 250, true));
        sourceGenerator.addEvent(2, new EventOrWatermark((i+1) + 250, true));
        // Thread.sleep(1);
      }
    }

    Thread.sleep(2000);

    // 400: move again
    masterSetupHelper.taskScheduledMapMaster.stopTask("Stage1-0-0");
    masterSetupHelper.taskScheduledMapMaster.stopTask("Stage0-0-0");

    Thread.sleep(3000);

    for (int i = 1500; i < 2000; i++) {
      sourceGenerator.addEvent(i % parallelism, new EventOrWatermark(Pair.of(i % 5, 1)));

      if ((i + 1) % 50 == 0) {
        sourceGenerator.addEvent(0, new EventOrWatermark((i+1) + 200, true));
        sourceGenerator.addEvent(1, new EventOrWatermark((i+1) + 250, true));
        sourceGenerator.addEvent(2, new EventOrWatermark((i+1) + 250, true));
        Thread.sleep(1);
      }
    }

    Thread.sleep(2000);

    // 500: move again
    masterSetupHelper.taskScheduledMapMaster.stopTask("Stage1-0-0");
    masterSetupHelper.taskScheduledMapMaster.stopTask("Stage0-0-0");

    Thread.sleep(3000);

    for (int i = 2000; i < 2500; i++) {
      sourceGenerator.addEvent(i % parallelism, new EventOrWatermark(Pair.of(i % 5, 1)));

      if ((i + 1) % 50 == 0) {
        sourceGenerator.addEvent(0, new EventOrWatermark((i+1) + 200, true));
        sourceGenerator.addEvent(1, new EventOrWatermark((i+1) + 250, true));
        sourceGenerator.addEvent(2, new EventOrWatermark((i+1) + 250, true));
        Thread.sleep(1);
      }
    }

    Thread.sleep(1000);

    // 500~800: move complex
    masterSetupHelper.taskScheduledMapMaster.stopTask("Stage0-1-0");
    masterSetupHelper.taskScheduledMapMaster.stopTask("Stage1-1-0");

    for (int i = 2500; i < 4000; i++) {
      sourceGenerator.addEvent(i % parallelism, new EventOrWatermark(Pair.of(i % 5, 1)));

      if (i == 3200) {
        masterSetupHelper.taskScheduledMapMaster.stopTask("Stage1-2-0");
      }

      if ((i + 1) % 50 == 0) {
        sourceGenerator.addEvent(0, new EventOrWatermark((i+1) + 200, true));
        sourceGenerator.addEvent(1, new EventOrWatermark((i+1) + 250, true));
        sourceGenerator.addEvent(2, new EventOrWatermark((i+1) + 250, true));
        Thread.sleep(10);
      }
    }

    masterSetupHelper.taskScheduledMapMaster.stopTask("Stage1-2-0");

    Thread.sleep(3000);
  }

  private void scheduleTask() {

  }

  private final AtomicInteger nodeNumber = new AtomicInteger(0);
  private Pair<Executor, Injector> launchExecutor(final int capacity) throws InjectionException {
    final Pair<Executor, Injector> pair1 =
      PipeManagerTestHelper.createExecutor("executor" + nodeNumber.incrementAndGet(), masterSetupHelper.nameServer, stateStore);

    final Executor executor = pair1.left();
    final ActiveContext activeContext = mock(ActiveContext.class);
    final EvaluatorDescriptor evaluatorDescriptor = mock(EvaluatorDescriptor.class);
    final NodeDescriptor nodeDescriptor = mock(NodeDescriptor.class);
    when(activeContext.getId()).thenReturn(executor.getExecutorId());
    when(activeContext.getEvaluatorDescriptor()).thenReturn(evaluatorDescriptor);
    when(evaluatorDescriptor.getNodeDescriptor()).thenReturn(nodeDescriptor);
    when(nodeDescriptor.getName()).thenReturn("node " + nodeNumber.get());

    final ResourceSpecification spec1 = new ResourceSpecification("reserved", capacity, 1024);
    masterSetupHelper.pendingContextIdToResourceSpec.put(executor.getExecutorId(), spec1);
    masterSetupHelper.requestLatchByResourceSpecId.put(spec1.getResourceSpecId(), new CountDownLatch(1));
    runtimeMaster.onExecutorLaunched(activeContext);

    return pair1;
  }
}
