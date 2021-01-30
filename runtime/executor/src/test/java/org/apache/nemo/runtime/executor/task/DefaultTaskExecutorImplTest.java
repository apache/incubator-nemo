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
package org.apache.nemo.runtime.executor.task;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
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
import org.apache.nemo.common.Task;
import org.apache.nemo.runtime.executor.*;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.InputPipeRegister;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.statestore.StateStore;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.apache.nemo.runtime.executor.data.PipeManagerWorkerTest;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.nemo.runtime.executor.task.util.EventOrWatermark;
import org.apache.nemo.runtime.executor.task.util.StreamTransformNoEmit;
import org.apache.nemo.runtime.executor.task.util.TestUnboundedSourceReadable;
import org.apache.nemo.runtime.executor.task.util.TestUnboundedSourceVertex;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests {@link TaskExecutor}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({InputReader.class, OutputWriter.class, IntermediateDataIOFactory.class, BroadcastManagerWorker.class,
  TaskStateManager.class, StageEdge.class, PersistentConnectionToMasterMap.class, Stage.class, IREdge.class})
public final class DefaultTaskExecutorImplTest {
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
  private MasterSetupHelper masterSetupHelper;
  private StateStore stateStore;

  @After
  public void tearDown() throws Exception {
    masterSetupHelper.close();
  }

  @Before
  public void setUp() throws Exception {
    stageId = new AtomicInteger(1);
    runtimeEdgeToOutputData = new HashMap<>();

    masterSetupHelper = new MasterSetupHelper();

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
    final IRVertex sourceIRVertex = new TestUnboundedSourceVertex(
      (List<Readable>) Collections.singletonList(readable));
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

    // Stage setup
    final int stage1Parallelism = 1;
    final int stage2Parallelism = 1;
    final String stage1Id = "stage1";
    final String stage2Id = "stage2";
    final String executor1 = "executor1";
    final String executor2 = "executor2";

    final String task1Id = RuntimeIdManager.generateTaskId(stage1Id, 0, 0);
    final String task2Id = RuntimeIdManager.generateTaskId(stage2Id, 0, 0);


    masterSetupHelper.executorIds.add(executor1);
    masterSetupHelper.executorIds.add(executor2);

    masterSetupHelper.taskScheduledMap.put(task1Id, executor1);
    masterSetupHelper.taskScheduledMap.put(task2Id, executor2);

    final LinkedList<Watermark> emittedWatermarks = new LinkedList<>();
    final LinkedList<Object> emittedEvents = new LinkedList<>();


    final Pair<IRVertex, Map<String, Readable>> srcPair1 = createSource(events1);
    final Map<String, Readable> vertexIdToReadable1 = srcPair1.right();


    // stage 1 vertex
    final IRVertex sourceIRVertex = srcPair1.left();
    final OperatorVertex operatorVertex1 = new OperatorVertex(new FlattenTransform());
    operatorVertex1.setProperty(ParallelismProperty.of(stage1Parallelism));

    // stage 2 vertex
    final OperatorVertex operatorVertex2 = new OperatorVertex(
      new StreamTransformNoEmit(emittedWatermarks, emittedEvents));
    operatorVertex2.setProperty(ParallelismProperty.of(stage2Parallelism));

    // stage1 dag
    final RuntimeEdge innerEdge = createEdge(sourceIRVertex, operatorVertex1, "edge0");
    final DAG<IRVertex, RuntimeEdge<IRVertex>> stage1Dag =
      new DAGBuilder<IRVertex, RuntimeEdge<IRVertex>>()
        .addVertex(sourceIRVertex)
        .addVertex(operatorVertex1)
        .connectVertices(innerEdge)
        .buildWithoutSourceSinkCheck();

    final ExecutionPropertyMap<VertexExecutionProperty> stage1Properties =
      new ExecutionPropertyMap<>(stage1Id);
    stage1Properties.put(ParallelismProperty.of(stage1Parallelism), true);
    // TODO
    final List<Map<String, Readable>> vertexIdReadables = new LinkedList<>();

    final Stage stage1 = new Stage(stage1Id,
      IntStream.range(0, stage1Parallelism).boxed().collect(Collectors.toList()),
      stage1Dag,
      stage1Properties,
      vertexIdReadables);

    // stage2 dag
    final DAG<IRVertex, RuntimeEdge<IRVertex>> stage2Dag =
      new DAGBuilder<IRVertex, RuntimeEdge<IRVertex>>()
        .addVertex(operatorVertex2)
        .buildWithoutSourceSinkCheck();

    final ExecutionPropertyMap<VertexExecutionProperty> stage2Properties =
      new ExecutionPropertyMap<>(stage2Id);
    stage2Properties.put(ParallelismProperty.of(stage2Parallelism), true);
    // TODO
    final List<Map<String, Readable>> vertexIdReadables2 = new LinkedList<>();

    final Stage stage2 = new Stage(stage2Id,
      IntStream.range(0, stage2Parallelism).boxed().collect(Collectors.toList()),
      stage2Dag,
      stage2Properties,
      vertexIdReadables2);

    // stage1 outgling edge
    final ExecutionPropertyMap executionPropertyMap =
      ExecutionPropertyMap.of(mock(IREdge.class), CommunicationPatternProperty.Value.OneToOne);
    executionPropertyMap.put(DataStoreProperty.of(DataStoreProperty.Value.Pipe), true);

    // Stage edge
    final StageEdge s1ToS2 = new StageEdge("edge1",
      executionPropertyMap,
      operatorVertex1,
      operatorVertex2,
      stage1,
      stage2);

    masterSetupHelper.pipeIndexMap.put(Triple.of(task1Id, s1ToS2.getId(), task2Id), 1);
    masterSetupHelper.pipeIndexMap.put(Triple.of(task2Id, s1ToS2.getId(), task1Id), 2);

     final Task task1 =
      new Task(
        "testSourceVertexDataFetching",
        task1Id,
        TASK_EXECUTION_PROPERTY_MAP,
        new byte[0],
        Collections.emptyList(),
        Collections.singletonList(s1ToS2),
        vertexIdToReadable1);

     final Task task2 =
      new Task(
        "task2plan",
        task2Id,
        TASK_EXECUTION_PROPERTY_MAP,
        new byte[0],
        Collections.singletonList(s1ToS2),
        Collections.emptyList(),
        new HashMap<>());


    // Execute the task.
    final List<TaskHandlingEvent> inputHandlingQueue1 = new LinkedList<>();
    final List<TaskHandlingEvent> inputHandlingQueue2 = new LinkedList<>();
    final Pair<TaskExecutor, Injector> taskExecutor1Pair = getTaskExecutor(0, "executor1", task1, stage1Dag, inputHandlingQueue1);
    final Pair<TaskExecutor, Injector> taskExecutor2Pair = getTaskExecutor(1, "executor2", task2, stage2Dag, inputHandlingQueue2);

    // Wait for registering name server
    Thread.sleep(1000);

    taskExecutor1Pair.right().getInstance(ExecutorContextManagerMap.class).init();
    taskExecutor1Pair.right().getInstance(TaskScheduledMapWorker.class).init();

    taskExecutor2Pair.right().getInstance(ExecutorContextManagerMap.class).init();
    taskExecutor2Pair.right().getInstance(TaskScheduledMapWorker.class).init();

    Thread.sleep(500);
    // Check init message
    checkPipeInitSignal(task2Id, s1ToS2.getId(), task1Id, inputHandlingQueue1);
    taskExecutor1Pair.right().getInstance(PipeManagerWorker.class)
      .startOutputPipe(
        masterSetupHelper.pipeIndexMap.get(Triple.of(task1Id, s1ToS2.getId(), task2Id)),
        task1.getTaskId());

    checkPipeInitSignal(task1Id, s1ToS2.getId(), task2Id, inputHandlingQueue2);
    taskExecutor2Pair.right().getInstance(PipeManagerWorker.class)
      .startOutputPipe(
        masterSetupHelper.pipeIndexMap.get(Triple.of(task2Id, s1ToS2.getId(), task1Id)),
        task2.getTaskId());


    final TaskExecutor taskExecutor1 = taskExecutor1Pair.left();
    final TaskExecutor taskExecutor2 = taskExecutor2Pair.left();
    // Check the output.
    while (!taskExecutor1.isSourceAvailable()) { Thread.sleep(100); }

    taskExecutor1.handleSourceData();
    Thread.sleep(100);

    assertEquals(10, getValuesInput(inputHandlingQueue2.remove(0)));

    taskExecutor1.handleSourceData();
    Thread.sleep(100);
    assertEquals(11, getValuesInput(inputHandlingQueue2.remove(0)));

    taskExecutor1.handleSourceData();
    Thread.sleep(100);
    assertEquals(12, getValuesInput(inputHandlingQueue2.remove(0)));

    taskExecutor1.handleSourceData();
    Thread.sleep(100);
    assertEquals(new WatermarkWithIndex(new Watermark(Util.WATERMARK_PROGRESS), 0),
      inputHandlingQueue2.remove(0).getData());

    // Task Move
    // Task Move
    // Task Move
    // Task Move
    // Task Move
    // Task Move
    // Task Move
    // Task Move
    // Task Move
    // Task Move
    // Task Move
    // Task Move
    // Task Move
    // Task Move
  }

  private Object getValuesInput(final TaskHandlingEvent event) {
    return ((TimestampAndValue)event.getData()).value;
  }


  /**
   * This test emits data and watermark by emulating an unbounded source readable.
   */
  @Test()
  public void testSingleTaskExecutor() throws Exception {

    final Pair<IRVertex, Map<String, Readable>> srcPair = createSource(events1);
    final IRVertex sourceIRVertex = srcPair.left();
    final Map<String, Readable> vertexIdToReadable = srcPair.right();

    final LinkedList<Watermark> emittedWatermarks = new LinkedList<>();
    final LinkedList<Object> emittedEvents = new LinkedList<>();

    final Transform transform = new StreamTransformNoEmit(emittedWatermarks, emittedEvents);
    final OperatorVertex operatorVertex = new OperatorVertex(transform);
    operatorVertex.setProperty(ParallelismProperty.of(1));

    final RuntimeEdge innerEdge = createEdge(sourceIRVertex, operatorVertex, "edge1");
    final DAG<IRVertex, RuntimeEdge<IRVertex>> taskDag =
      new DAGBuilder<IRVertex, RuntimeEdge<IRVertex>>()
        .addVertex(sourceIRVertex)
        .addVertex(operatorVertex)
        .connectVertices(innerEdge)
        .buildWithoutSourceSinkCheck();

    // TAsk setup
    final String stage1Id = "stage1";
    final ExecutionPropertyMap<VertexExecutionProperty> stageProperties =
      new ExecutionPropertyMap<>(stage1Id);
    stageProperties.put(ParallelismProperty.of(1), true);
    final List<Map<String, Readable>> vertexIdReadables = new LinkedList<>();
    vertexIdReadables.add(vertexIdToReadable);

    final Stage stage1 = new Stage(stage1Id,
      Collections.singletonList(0),
      taskDag,
      stageProperties,
      vertexIdReadables);

    final Task task1 =
      new Task(
        "testSourceVertexDataFetching",
        RuntimeIdManager.generateTaskId(stage1Id, 0, 0),
        TASK_EXECUTION_PROPERTY_MAP,
        new byte[0],
        Collections.emptyList(),
        Collections.emptyList(),
        vertexIdToReadable);

    // Execute the task.
    final TaskExecutor taskExecutor = getTaskExecutor(0, "executor1", task1, taskDag, Collections.emptyList()).left();

    // Check the output.
    while (!taskExecutor.isSourceAvailable()) { Thread.sleep(100); }

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(10), emittedEvents);

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(10, 11), emittedEvents);

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(10, 11, 12), emittedEvents);
    assertEquals(0, emittedWatermarks.size());

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(new Watermark(Util.WATERMARK_PROGRESS)), emittedWatermarks);

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(10, 11, 12, 13), emittedEvents);

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(10, 11, 12, 13, 12), emittedEvents);

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(new Watermark(Util.WATERMARK_PROGRESS),
      new Watermark(Util.WATERMARK_PROGRESS * 2)), emittedWatermarks);
  }

  private List<Object> getValues(final List<Object> list) {
    return (List<Object>) list.stream()
      .map(f -> ((TimestampAndValue) f).value).collect(Collectors.toList());
  }

  private List<Object> getValues(final String edgeId) {
    return (List<Object>) runtimeEdgeToOutputData.get(edgeId).stream()
      .map(f -> ((TimestampAndValue) f).value).collect(Collectors.toList());
  }

  private RuntimeEdge<IRVertex> createEdge(final IRVertex src,
                                           final IRVertex dst,
                                           final String runtimeIREdgeId) {
    ExecutionPropertyMap<EdgeExecutionProperty> edgeProperties = new
      ExecutionPropertyMap<>(runtimeIREdgeId);
    edgeProperties.put(DataStoreProperty.of(DataStoreProperty.Value.Pipe));
    return new RuntimeEdge<>(runtimeIREdgeId, edgeProperties, src, dst);

  }

  private StageEdge mockStageEdgeFrom(final IRVertex irVertex,
                                      final String dstTaskId) {
    final IREdge edge = mock(IREdge.class);

    final ExecutionPropertyMap map = ExecutionPropertyMap.of(edge, CommunicationPatternProperty.Value.OneToOne);
    map.put(DataStoreProperty.of(DataStoreProperty.Value.Pipe), true);

    return new StageEdge("SEdge" + RUNTIME_EDGE_ID.getAndIncrement(),
        map,
        irVertex,
        new OperatorVertex(new StreamTransform()),
        mock(Stage.class),
        mock(Stage.class));
  }

  private StageEdge mockStageEdgeTo(final IRVertex irVertex) {
    final ExecutionPropertyMap executionPropertyMap =
      ExecutionPropertyMap.of(mock(IREdge.class), CommunicationPatternProperty.Value.OneToOne);
    return new StageEdge("runtime outgoing edge id",
      executionPropertyMap,
      new OperatorVertex(new StreamTransform()),
      irVertex,
      mock(Stage.class),
      mock(Stage.class));
  }

  /**
   * Represents the answer return a {@link OutputWriter},
   * which will stores the data to the map between task id and output data.
   */
  private class ChildTaskWriterAnswer implements Answer<OutputWriter> {
    @Override
    public OutputWriter answer(final InvocationOnMock invocationOnMock) throws Throwable {
      final Object[] args = invocationOnMock.getArguments();
      final RuntimeEdge runtimeEdge = (RuntimeEdge) args[1];
      final OutputWriter outputWriter = mock(OutputWriter.class);
      doAnswer(new Answer() {
        @Override
        public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
          final Object[] args = invocationOnMock.getArguments();
          final Object dataToWrite = args[0];
          runtimeEdgeToOutputData.computeIfAbsent(runtimeEdge.getId(), emptyTaskId -> new ArrayList<>());
          runtimeEdgeToOutputData.get(runtimeEdge.getId()).add(dataToWrite);
          return null;
        }
      }).when(outputWriter).write(any());
      return outputWriter;
    }
  }


  /**
   * Simple identity function for testing.
   * @param <T> input/output type.
   */
  private class StreamTransform<T> implements Transform<T, T> {
    private OutputCollector<T> outputCollector;

    @Override
    public void prepare(final Context context, final OutputCollector<T> outputCollector) {
      this.outputCollector = outputCollector;
    }

    @Override
    public void onWatermark(Watermark watermark) {
      outputCollector.emitWatermark(watermark);
    }

    @Override
    public void onData(final Object element) {
      outputCollector.emit((T) element);
    }

    @Override
    public void close() {
      // Do nothing.
    }
  }

  private Pair<TaskExecutor, Injector> getTaskExecutor(final long tid,
                                       final String executorId,
                                       final Task task,
                                       final DAG<IRVertex, RuntimeEdge<IRVertex>> taskDag,
                                       final List<TaskHandlingEvent> inputHandlingList) throws InjectionException {
    final Pair<PipeManagerWorker, Injector> pair = PipeManagerTestHelper
      .createPipeManagerWorker(executorId, masterSetupHelper.nameServer);

    final Injector injector = pair.right();
    final IntermediateDataIOFactory ioFactory = injector.getInstance(IntermediateDataIOFactory.class);

    // when(ioFactory.createPipeWriter(any(), any(), any())).thenAnswer(new ChildTaskWriterAnswer());

    final BroadcastManagerWorker bworker = mock(BroadcastManagerWorker.class);

    final MetricMessageSender ms = mock(MetricMessageSender.class);

    final PersistentConnectionToMasterMap pmap = injector.getInstance(PersistentConnectionToMasterMap.class);

    final SerializerManager serializerManager = injector.getInstance(SerializerManager.class);
    serializerManager.register("edge0", new NemoEventEncoderFactory(IntEncoderFactory.of()),
      new NemoEventDecoderFactory(IntDecoderFactory.of()));
    serializerManager.register("edge1", new NemoEventEncoderFactory(IntEncoderFactory.of()),
      new NemoEventDecoderFactory(IntDecoderFactory.of()));
    serializerManager.register("edge2", new NemoEventEncoderFactory(IntEncoderFactory.of()),
      new NemoEventDecoderFactory(IntDecoderFactory.of()));

    final ServerlessExecutorProvider provider = mock(ServerlessExecutorProvider.class);

    final EvalConf evalConf = TANG.newInjector().getInstance(EvalConf.class);

    final ExecutorService prepare = Executors.newSingleThreadExecutor();


    final ExecutorThreadQueue executorThreadQueue = new ExecutorThreadQueue() {

      @Override
      public void addEvent(TaskHandlingEvent event) {
        inputHandlingList.add(event);
      }

      @Override
      public boolean isEmpty() {
        return inputHandlingList.isEmpty();
      }
    };

    final InputPipeRegister pipeManagerWorker = pair.left();



    return Pair.of(new DefaultTaskExecutorImpl(
      tid,
      executorId,
      task,
      taskDag,
      ioFactory,
      bworker,
      ms,
      pmap,
      serializerManager,
      provider,
      evalConf,
      prepare,
      executorThreadQueue,
      pipeManagerWorker,
      stateStore), injector);
  }
}
