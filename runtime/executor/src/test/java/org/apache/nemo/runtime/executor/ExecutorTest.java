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
package org.apache.nemo.runtime.executor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.coder.IntDecoderFactory;
import org.apache.nemo.common.coder.IntEncoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.Stage;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.transform.FlattenTransform;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.InputPipeRegister;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.statestore.StateStore;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.nemo.runtime.executor.task.DefaultTaskExecutorImpl;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link TaskExecutor}.
 */
@RunWith(PowerMockRunner.class)
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

  private List<EventOrWatermark> events1;
  private List<EventOrWatermark> events2;
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
      final Map<String, ByteBuf> stateMap = new HashMap<>();

      @Override
      public synchronized  InputStream getStateStream(String taskId) {
        return new ByteBufInputStream(stateMap.get(taskId).retainedDuplicate());
      }

      @Override
      public synchronized OutputStream getOutputStreamForStoreTaskState(String taskId) {
        stateMap.put(taskId, ByteBufAllocator.DEFAULT.buffer());
        return new ByteBufOutputStream(stateMap.get(taskId));
      }

      @Override
      public synchronized boolean containsState(String taskId) {
        return stateMap.containsKey(taskId);
      }
    };

    events1 = new LinkedList<>();

    events1.add(new EventOrWatermark(10));
    events1.add(new EventOrWatermark(11));
    events1.add(new EventOrWatermark(Util.WATERMARK_PROGRESS,true));
    events1.add(new EventOrWatermark(12));
    events1.add(new EventOrWatermark(13));
    events1.add(new EventOrWatermark(Util.WATERMARK_PROGRESS * 2,true));
    events1.add(new EventOrWatermark(12));
    events1.add(new EventOrWatermark(13));
    events1.add(new EventOrWatermark(Util.WATERMARK_PROGRESS * 3,true));
    events1.add(new EventOrWatermark(14));
    events1.add(new EventOrWatermark(15));

    events2 = new LinkedList<>();

    events1.add(new EventOrWatermark(100));
    events1.add(new EventOrWatermark(110));
    events1.add(new EventOrWatermark(Util.WATERMARK_PROGRESS,true));
    events1.add(new EventOrWatermark(120));
    events1.add(new EventOrWatermark(130));
    events1.add(new EventOrWatermark(Util.WATERMARK_PROGRESS * 2,true));
    events1.add(new EventOrWatermark(120));
    events1.add(new EventOrWatermark(130));
    events1.add(new EventOrWatermark(Util.WATERMARK_PROGRESS * 3,true));
    events1.add(new EventOrWatermark(140));
    events1.add(new EventOrWatermark(150));
  }

  private Pair<IRVertex, Map<String, Readable>> createSource(final List<EventOrWatermark> events) {
    final Readable readable = new TestUnboundedSourceReadable(events);
    final IRVertex sourceIRVertex = new TestUnboundedSourceVertex(Collections.singletonList(readable));
    final Map<String, Readable> vertexIdToReadable = new HashMap<>();
    vertexIdToReadable.put(sourceIRVertex.getId(), readable);
    return Pair.of(sourceIRVertex, vertexIdToReadable);
  }

  private void checkPipeInitSignal(final String srcTask,
                                   final String edge,
                                   final String dstTask,
                                   final List<TaskHandlingEvent> list) {
    assertEquals(
      // left
      new TaskControlMessage(
        TaskControlMessage.TaskControlMessageType.PIPE_INIT,
        masterSetupHelper.pipeIndexMap.get(Triple.of(srcTask, edge, dstTask)),
        masterSetupHelper.pipeIndexMap.get(Triple.of(dstTask, edge, srcTask)),
        dstTask,
        null),
      // right
      list.remove(0));
  }

  // [stage1]  [stage2]
  // Task1 -> Task2

  // [      stage 1      ]   [ stage 2]
  // (src) -> (op flatten) -> (op noemit)
  @Test
  public void testMultipleTaskExecutors() throws Exception {

    final Pair<Executor, Injector> pair1 = launchExecutor(2);
    final Pair<Executor, Injector> pair2 = launchExecutor(2);

    final int parallelism = 2;
    final TCPSourceGenerator sourceGenerator = new TCPSourceGenerator(parallelism);

    final TestDAGBuilder testDAGBuilder = new TestDAGBuilder(masterSetupHelper.planGenerator, parallelism);
    final PhysicalPlan plan = testDAGBuilder.generatePhysicalPlan(TestDAGBuilder.PlanType.TwoVertices);

    runtimeMaster.execute(plan, 1);

    Thread.sleep(5000);

    // 500 / 2 / 5 = 20 (key마다 20)
    for (int i = 0; i < 500; i++) {
      sourceGenerator.addEvent(i % 2, new EventOrWatermark(Pair.of(i % 5, 1)));
      sourceGenerator.addEvent(i % 2, new EventOrWatermark(Pair.of(i % 5, 1)));

      if ((i + 1) % 50 == 0) {
        sourceGenerator.addEvent(0, new EventOrWatermark((i+1) + 200, true));
        sourceGenerator.addEvent(1, new EventOrWatermark((i+1) + 250, true));
      }
    }

    Thread.sleep(2000);
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
