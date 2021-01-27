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

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.Stage;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.executor.MasterSetupHelper;
import org.apache.nemo.runtime.executor.MetricMessageSender;
import org.apache.nemo.runtime.executor.PipeManagerTestHelper;
import org.apache.nemo.runtime.executor.TaskStateManager;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.ExecutorThreadQueue;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.datatransfer.InputPipeRegister;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.executor.common.datatransfer.IteratorWithNumBytes;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.statestore.StateStore;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
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
  private static final int DATA_SIZE = 100;
  private static final ExecutionPropertyMap<VertexExecutionProperty> TASK_EXECUTION_PROPERTY_MAP
      = new ExecutionPropertyMap<>("TASK_EXECUTION_PROPERTY_MAP");
  private static final int SOURCE_PARALLELISM = 5;
  private static final int FIRST_ATTEMPT = 0;

  private Map<String, List> runtimeEdgeToOutputData;
  private AtomicInteger stageId;

  private final Tang TANG = Tang.Factory.getTang();

  private String generateTaskId() {
    return RuntimeIdManager.generateTaskId(
        RuntimeIdManager.generateStageId(stageId.getAndIncrement()), 0, FIRST_ATTEMPT);
  }

  // private MasterSetupHelper masterSetupHelper;

  @Before
  public void setUp() throws Exception {
    stageId = new AtomicInteger(1);
    // masterSetupHelper = new MasterSetupHelper();
    runtimeEdgeToOutputData = new HashMap<>();
  }

  /**
   * This test emits data and watermark by emulating an unbounded source readable.
   */
  @Test()
  public void testUnboundedSourceVertexDataFetching() throws Exception {
    final IRVertex sourceIRVertex = new TestUnboundedSourceVertex();
    final List<EventOrWatermark> events = new LinkedList<>();

    events.add(new EventOrWatermark(10));
    events.add(new EventOrWatermark(11));
    events.add(new EventOrWatermark(Util.WATERMARK_PROGRESS,true));
    events.add(new EventOrWatermark(12));
    events.add(new EventOrWatermark(13));
    events.add(new EventOrWatermark(Util.WATERMARK_PROGRESS * 2,true));
    events.add(new EventOrWatermark(12));
    events.add(new EventOrWatermark(13));
    events.add(new EventOrWatermark(Util.WATERMARK_PROGRESS * 3,true));
    events.add(new EventOrWatermark(14));
    events.add(new EventOrWatermark(15));

    final Readable readable = new TestUnboundedSourceReadable(events);

    final Map<String, Readable> vertexIdToReadable = new HashMap<>();
    vertexIdToReadable.put(sourceIRVertex.getId(), readable);
    final List<Watermark> emittedWatermarks = new LinkedList<>();

    final Transform transform = new StreamTransformNoWatermarkEmit(emittedWatermarks);
    final OperatorVertex operatorVertex = new OperatorVertex(transform);
    operatorVertex.setProperty(ParallelismProperty.of(1));

    final DAG<IRVertex, RuntimeEdge<IRVertex>> taskDag =
      new DAGBuilder<IRVertex, RuntimeEdge<IRVertex>>()
        .addVertex(sourceIRVertex)
        .addVertex(operatorVertex)
        .connectVertices(createEdge(sourceIRVertex, operatorVertex, "edge1"))
        .buildWithoutSourceSinkCheck();

    final StageEdge taskOutEdge = mockStageEdgeFrom(operatorVertex);
    final Task task =
      new Task(
        "testSourceVertexDataFetching",
        generateTaskId(),
        TASK_EXECUTION_PROPERTY_MAP,
        new byte[0],
        Collections.emptyList(),
        Collections.singletonList(taskOutEdge),
        vertexIdToReadable);

    // Execute the task.
    final TaskExecutor taskExecutor = getTaskExecutor(0, "executor1", task, taskDag);

    // Check the output.
    while (!taskExecutor.isSourceAvailable()) { Thread.sleep(100); }

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(10), getValues(taskOutEdge.getId()));

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(10, 11), getValues(taskOutEdge.getId()));

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(10, 11, 12), getValues(taskOutEdge.getId()));
    assertEquals(0, emittedWatermarks.size());

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(new Watermark(Util.WATERMARK_PROGRESS)), emittedWatermarks);

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(10, 11, 12, 13), getValues(taskOutEdge.getId()));

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(10, 11, 12, 13, 12), getValues(taskOutEdge.getId()));

    taskExecutor.handleSourceData();
    assertEquals(Arrays.asList(new Watermark(Util.WATERMARK_PROGRESS),
      new Watermark(Util.WATERMARK_PROGRESS * 2)), emittedWatermarks);
  }

  private List<Object> getValues(final String edgeId) {
    return (List<Object>) runtimeEdgeToOutputData.get(edgeId).stream()
      .map(f -> ((TimestampAndValue) f).value).collect(Collectors.toList());
  }

  private RuntimeEdge<IRVertex> createEdge(final IRVertex src,
                                           final IRVertex dst,
                                           final String runtimeIREdgeId) {
    ExecutionPropertyMap<EdgeExecutionProperty> edgeProperties = new ExecutionPropertyMap<>(runtimeIREdgeId);
    edgeProperties.put(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
    return new RuntimeEdge<>(runtimeIREdgeId, edgeProperties, src, dst);

  }

  private StageEdge mockStageEdgeFrom(final IRVertex irVertex) {
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
   * This transform does not emit watermark to OutputWriter
   * because OutputWriter currently does not support watermarks (TODO #245)
   * @param <T> type
   */
  private class StreamTransformNoWatermarkEmit<T> implements Transform<T, T> {
    private OutputCollector<T> outputCollector;
    private final List<Watermark> emittedWatermarks;

    StreamTransformNoWatermarkEmit(final List<Watermark> emittedWatermarks) {
      this.emittedWatermarks = emittedWatermarks;
    }

    @Override
    public void prepare(final Context context, final OutputCollector<T> outputCollector) {
      this.outputCollector = outputCollector;
    }

    @Override
    public void onWatermark(Watermark watermark) {
      emittedWatermarks.add(watermark);
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

  /**
   * Source vertex for unbounded source test.
   */
  private final class TestUnboundedSourceVertex extends SourceVertex {

    @Override
    public boolean isBounded() {
      return false;
    }

    @Override
    public List<Readable> getReadables(int desiredNumOfSplits) throws Exception {
      return null;
    }

    @Override
    public void clearInternalStates() {

    }

    @Override
    public IRVertex getClone() {
      return null;
    }
  }

  private final class EventOrWatermark {
    public Object event;
    public long watermark;
    private final boolean data;

    public EventOrWatermark(final Object event) {
      this.event = event;
      this.data = true;
    }

    public EventOrWatermark(final long watermark,
                            final boolean watermarked) {
      this.watermark = watermark;
      this.data = false;
    }

    public boolean isWatermark() {
      return !data;
    }
  }

  // This emulates unbounded source that throws NoSuchElementException
  // It reads current data until middle point and throws NoSuchElementException at the middle point.
  // It resumes the data reading after emitting a watermark, and finishes at the end of the data.
  private final class TestUnboundedSourceReadable implements Readable {
    final List<EventOrWatermark> elements;

    private long currWatermark = 0;

    public TestUnboundedSourceReadable(final List<EventOrWatermark> events) {
      this.elements = events;
    }

    @Override
    public boolean isAvailable() {
      return !elements.isEmpty() && !(elements.size() == 1 && elements.get(0).isWatermark());
    }

    @Override
    public void prepare() {

    }

    @Override
    public Object readCurrent() throws NoSuchElementException {
      while (true) {
        final EventOrWatermark e = elements.remove(0);
        if (e.isWatermark()) {
          currWatermark = e.watermark;
        } else {
          return new TimestampAndValue<>(System.currentTimeMillis(), e.event);
        }
      }
    }

    @Override
    public long readWatermark() {
      return currWatermark;
    }

    @Override
    public boolean isFinished() {
      return false;
    }

    @Override
    public List<String> getLocations() throws Exception {
      return null;
    }

    @Override
    public void close() throws IOException {
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


  /**
   * Simple conditional identity function for testing additional outputs.
   */
  private class RoutingTransform implements Transform<Integer, Integer> {
    private OutputCollector<Integer> outputCollector;
    private final Collection<String> additionalTags;

    public RoutingTransform(final Collection<String> additionalTags) {
      this.additionalTags = additionalTags;
    }

    @Override
    public void prepare(final Context context, OutputCollector<Integer> outputCollector) {
      this.outputCollector = outputCollector;
    }

    @Override
    public void onData(final Integer element) {
      final int i = element;
      if (i % 2 == 0) {
        // route to all main outputs. Invoked if user calls c.output(element)
        outputCollector.emit(i);
      } else {
        // route to all additional outputs. Invoked if user calls c.output(tupleTag, element)
        additionalTags.forEach(tag -> outputCollector.emit(tag, i));
      }
    }

    @Override
    public void onWatermark(Watermark watermark) {
      outputCollector.emitWatermark(watermark);
    }

    @Override
    public void close() {
      // Do nothing.
    }
  }

  /**
   * Gets a list of integer pair elements in range.
   * @param start value of the range (inclusive).
   * @param end   value of the range (exclusive).
   * @return the list of elements.
   */
  private List<Integer> getRangedNumList(final int start, final int end) {
    final List<Integer> numList = new ArrayList<>(end - start);
    IntStream.range(start, end).forEach(number -> numList.add(number));
    return numList;
  }

  private TaskExecutor getTaskExecutor(final long tid,
                                       final String executorId,
                                       final Task task,
                                       final DAG<IRVertex, RuntimeEdge<IRVertex>> taskDag) throws InjectionException {
    // final Pair<PipeManagerWorker, Injector> pair = PipeManagerTestHelper
    //  .createPipeManagerWorker("executor1", masterSetupHelper.nameServer);

    final IntermediateDataIOFactory ioFactory =
      mock(IntermediateDataIOFactory.class);

    when(ioFactory.createPipeWriter(any(), any(), any())).thenAnswer(new ChildTaskWriterAnswer());

    final BroadcastManagerWorker bworker = mock(BroadcastManagerWorker.class);

    final MetricMessageSender ms = mock(MetricMessageSender.class);

    final PersistentConnectionToMasterMap pmap = mock(PersistentConnectionToMasterMap.class);

    final SerializerManager serializerManager = mock(SerializerManager.class);
    when(serializerManager.getSerializer(any())).thenReturn(PipeManagerTestHelper.INT_SERIALIZER);

    final ServerlessExecutorProvider provider = mock(ServerlessExecutorProvider.class);

    final EvalConf evalConf = TANG.newInjector().getInstance(EvalConf.class);

    final ExecutorService prepare = Executors.newSingleThreadExecutor();

    final ExecutorThreadQueue executorThreadQueue = new ExecutorThreadQueue() {
      public final List<TaskHandlingEvent> list = new LinkedList<>();

      @Override
      public void addEvent(TaskHandlingEvent event) {
        list.add(event);
      }

      @Override
      public boolean isEmpty() {
        return list.isEmpty();
      }
    };

    final InputPipeRegister pipeManagerWorker = mock(InputPipeRegister.class);

    final StateStore stateStore = mock(StateStore.class);
    when(stateStore.containsState(any())).thenReturn(false);

    return new DefaultTaskExecutorImpl(
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
      stateStore);
  }
}
