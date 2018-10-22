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
package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.edge.executionproperty.BroadcastVariableIdProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.InMemorySourceVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.executor.MetricMessageSender;
import org.apache.nemo.runtime.executor.TaskStateManager;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.datatransfer.InputReader;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;
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
public final class TaskExecutorTest {
  private static final AtomicInteger RUNTIME_EDGE_ID = new AtomicInteger(0);
  private static final int DATA_SIZE = 100;
  private static final ExecutionPropertyMap<VertexExecutionProperty> TASK_EXECUTION_PROPERTY_MAP
      = new ExecutionPropertyMap<>("TASK_EXECUTION_PROPERTY_MAP");
  private static final int SOURCE_PARALLELISM = 5;
  private static final int FIRST_ATTEMPT = 0;

  private List<Integer> elements;
  private Map<String, List> runtimeEdgeToOutputData;
  private IntermediateDataIOFactory intermediateDataIOFactory;
  private BroadcastManagerWorker broadcastManagerWorker;
  private TaskStateManager taskStateManager;
  private MetricMessageSender metricMessageSender;
  private PersistentConnectionToMasterMap persistentConnectionToMasterMap;
  private AtomicInteger stageId;

  private String generateTaskId() {
    return RuntimeIdManager.generateTaskId(
        RuntimeIdManager.generateStageId(stageId.getAndIncrement()), 0, FIRST_ATTEMPT);
  }

  @Before
  public void setUp() throws Exception {
    elements = getRangedNumList(0, DATA_SIZE);
    stageId = new AtomicInteger(1);

    // Mock a TaskStateManager. It accumulates the state change into a list.
    taskStateManager = mock(TaskStateManager.class);

    // Mock a IntermediateDataIOFactory.
    runtimeEdgeToOutputData = new HashMap<>();
    intermediateDataIOFactory = mock(IntermediateDataIOFactory.class);
    when(intermediateDataIOFactory.createReader(anyInt(), any(), any())).then(new ParentTaskReaderAnswer());
    when(intermediateDataIOFactory.createWriter(any(), any(), any())).then(new ChildTaskWriterAnswer());

    // Mock a MetricMessageSender.
    metricMessageSender = mock(MetricMessageSender.class);
    doNothing().when(metricMessageSender).send(anyString(), anyString(), anyString(), any());
    doNothing().when(metricMessageSender).close();

    persistentConnectionToMasterMap = mock(PersistentConnectionToMasterMap.class);
    broadcastManagerWorker = mock(BroadcastManagerWorker.class);
  }

  private boolean checkEqualElements(final List<Integer> left, final List<Integer> right) {
    Collections.sort(left);
    Collections.sort(right);
    return left.equals(right);
  }

  /**
   * Test source vertex data fetching.
   */
  @Test(timeout=5000)
  public void testSourceVertexDataFetching() throws Exception {
    final IRVertex sourceIRVertex = new InMemorySourceVertex<>(elements);

    final Readable readable = new Readable() {
      @Override
      public Iterable read() throws IOException {
        return elements;
      }
      @Override
      public List<String> getLocations() {
        throw new UnsupportedOperationException();
      }
    };
    final Map<String, Readable> vertexIdToReadable = new HashMap<>();
    vertexIdToReadable.put(sourceIRVertex.getId(), readable);

    final DAG<IRVertex, RuntimeEdge<IRVertex>> taskDag =
        new DAGBuilder<IRVertex, RuntimeEdge<IRVertex>>()
            .addVertex(sourceIRVertex)
            .buildWithoutSourceSinkCheck();

    final StageEdge taskOutEdge = mockStageEdgeFrom(sourceIRVertex);
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
    final TaskExecutor taskExecutor = getTaskExecutor(task, taskDag);
    taskExecutor.execute();

    // Check the output.
    assertTrue(checkEqualElements(elements, runtimeEdgeToOutputData.get(taskOutEdge.getId())));
  }

  /**
   * Test parent task data fetching.
   */
  @Test(timeout=5000)
  public void testParentTaskDataFetching() throws Exception {
    final IRVertex vertex = new OperatorVertex(new RelayTransform());

    final DAG<IRVertex, RuntimeEdge<IRVertex>> taskDag = new DAGBuilder<IRVertex, RuntimeEdge<IRVertex>>()
        .addVertex(vertex)
        .buildWithoutSourceSinkCheck();

    final StageEdge taskOutEdge = mockStageEdgeFrom(vertex);
    final Task task = new Task(
        "testSourceVertexDataFetching",
        generateTaskId(),
        TASK_EXECUTION_PROPERTY_MAP,
        new byte[0],
        Collections.singletonList(mockStageEdgeTo(vertex)),
        Collections.singletonList(taskOutEdge),
        Collections.emptyMap());

    // Execute the task.
    final TaskExecutor taskExecutor = getTaskExecutor(task, taskDag);
    taskExecutor.execute();

    // Check the output.
    assertTrue(checkEqualElements(elements, runtimeEdgeToOutputData.get(taskOutEdge.getId())));
  }

  /**
   * The DAG of the task to test will looks like:
   * parent task -> task (vertex 1 -> task 2) -> child task
   *
   * The output data from task 1 will be split according to source parallelism through {@link ParentTaskReaderAnswer}.
   * Because of this, task 1 will process multiple partitions and emit data in multiple times also.
   * On the other hand, task 2 will receive the output data once and produce a single output.
   */
  @Test(timeout=5000)
  public void testTwoOperators() throws Exception {
    final IRVertex operatorIRVertex1 = new OperatorVertex(new RelayTransform());
    final IRVertex operatorIRVertex2 = new OperatorVertex(new RelayTransform());

    final String edgeId = "edge";
    final DAG<IRVertex, RuntimeEdge<IRVertex>> taskDag = new DAGBuilder<IRVertex, RuntimeEdge<IRVertex>>()
        .addVertex(operatorIRVertex1)
        .addVertex(operatorIRVertex2)
        .connectVertices(createEdge(operatorIRVertex1, operatorIRVertex2, edgeId))
        .buildWithoutSourceSinkCheck();

    final StageEdge taskOutEdge = mockStageEdgeFrom(operatorIRVertex2);
    final Task task = new Task(
        "testSourceVertexDataFetching",
        generateTaskId(),
        TASK_EXECUTION_PROPERTY_MAP,
        new byte[0],
        Collections.singletonList(mockStageEdgeTo(operatorIRVertex1)),
        Collections.singletonList(taskOutEdge),
        Collections.emptyMap());

    // Execute the task.
    final TaskExecutor taskExecutor = getTaskExecutor(task, taskDag);
    taskExecutor.execute();

    // Check the output.
    assertTrue(checkEqualElements(elements, runtimeEdgeToOutputData.get(taskOutEdge.getId())));
  }

  @Test(timeout=5000)
  public void testTwoOperatorsWithBroadcastVariable() {
    final Transform singleListTransform = new CreateSingleListTransform();

    final long broadcastId = 0;
    final IRVertex operatorIRVertex1 = new OperatorVertex(new RelayTransform());
    final IRVertex operatorIRVertex2 = new OperatorVertex(new BroadcastVariablePairingTransform(broadcastId));

    final String edgeId = "edge";
    final DAG<IRVertex, RuntimeEdge<IRVertex>> taskDag = new DAGBuilder<IRVertex, RuntimeEdge<IRVertex>>()
        .addVertex(operatorIRVertex1)
        .addVertex(operatorIRVertex2)
        .connectVertices(createEdge(operatorIRVertex1, operatorIRVertex2, edgeId))
        .buildWithoutSourceSinkCheck();

    final StageEdge taskOutEdge = mockStageEdgeFrom(operatorIRVertex2);

    final StageEdge broadcastInEdge = mockBroadcastVariableStageEdgeTo(
      new OperatorVertex(singleListTransform), operatorIRVertex2, broadcastId, elements);

    final Task task = new Task(
        "testSourceVertexDataFetching",
        generateTaskId(),
        TASK_EXECUTION_PROPERTY_MAP,
        new byte[0],
        Arrays.asList(mockStageEdgeTo(operatorIRVertex1), broadcastInEdge),
        Collections.singletonList(taskOutEdge),
        Collections.emptyMap());

    // Execute the task.
    final TaskExecutor taskExecutor = getTaskExecutor(task, taskDag);
    taskExecutor.execute();

    // Check the output.
    final List<Pair<List<Integer>, Integer>> pairs = runtimeEdgeToOutputData.get(taskOutEdge.getId());
    final List<Integer> values = pairs.stream().map(Pair::right).collect(Collectors.toList());
    assertTrue(checkEqualElements(elements, values));
    assertTrue(pairs.stream().map(Pair::left).allMatch(broadcastVar -> checkEqualElements(broadcastVar, values)));
  }

  /**
   * The DAG of the task to test looks like:
   * parent vertex 1 --+-- vertex 2 (main tag)
   *                   +-- vertex 3 (additional tag 1)
   *                   +-- vertex 4 (additional tag 2)
   *
   * emit(element) and emit(dstVertexId, element) used together. emit(element) routes results to main output children,
   * and emit(dstVertexId, element) routes results to corresponding additional output children.
   */
  @Test(timeout = 5000)
  public void testAdditionalOutputs() throws Exception {
    final IRVertex routerVertex = new OperatorVertex(new RoutingTransform());
    final IRVertex mainVertex= new OperatorVertex(new RelayTransform());
    final IRVertex bonusVertex1 = new OperatorVertex(new RelayTransform());
    final IRVertex bonusVertex2 = new OperatorVertex(new RelayTransform());

    final RuntimeEdge<IRVertex> edge1 = createEdge(routerVertex, mainVertex, "edge-1");
    final RuntimeEdge<IRVertex> edge2 = createEdge(routerVertex, bonusVertex1, "edge-2");
    final RuntimeEdge<IRVertex> edge3 = createEdge(routerVertex, bonusVertex2, "edge-3");

    edge2.getExecutionProperties().put(AdditionalOutputTagProperty.of("bonus1"));
    edge3.getExecutionProperties().put(AdditionalOutputTagProperty.of("bonus2"));

    final DAG<IRVertex, RuntimeEdge<IRVertex>> taskDag = new DAGBuilder<IRVertex, RuntimeEdge<IRVertex>>()
        .addVertex(routerVertex)
        .addVertex(mainVertex)
        .addVertex(bonusVertex1)
        .addVertex(bonusVertex2)
        .connectVertices(edge1)
        .connectVertices(edge2)
        .connectVertices(edge3)
        .buildWithoutSourceSinkCheck();

    final StageEdge outEdge1 = mockStageEdgeFrom(mainVertex);
    final StageEdge outEdge2 = mockStageEdgeFrom(bonusVertex1);
    final StageEdge outEdge3 = mockStageEdgeFrom(bonusVertex2);

    final Task task = new Task(
        "testAdditionalOutputs",
        generateTaskId(),
        TASK_EXECUTION_PROPERTY_MAP,
        new byte[0],
        Collections.singletonList(mockStageEdgeTo(routerVertex)),
        Arrays.asList(outEdge1, outEdge2, outEdge3),
        Collections.emptyMap());

    // Execute the task.
    final TaskExecutor taskExecutor = getTaskExecutor(task, taskDag);
    taskExecutor.execute();

    // Check the output.
    final List<Integer> mainOutputs = runtimeEdgeToOutputData.get(outEdge1.getId());
    final List<Integer> bonusOutputs1 = runtimeEdgeToOutputData.get(outEdge2.getId());
    final List<Integer> bonusOutputs2 = runtimeEdgeToOutputData.get(outEdge3.getId());

    List<Integer> even = elements.stream().filter(i -> i % 2 == 0).collect(Collectors.toList());
    List<Integer> odd = elements.stream().filter(i -> i % 2 != 0).collect(Collectors.toList());
    assertTrue(checkEqualElements(even, mainOutputs));
    assertTrue(checkEqualElements(odd, bonusOutputs1));
    assertTrue(checkEqualElements(odd, bonusOutputs2));
  }

  private RuntimeEdge<IRVertex> createEdge(final IRVertex src,
                                           final IRVertex dst,
                                           final String runtimeIREdgeId) {
    ExecutionPropertyMap<EdgeExecutionProperty> edgeProperties = new ExecutionPropertyMap<>(runtimeIREdgeId);
    edgeProperties.put(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
    return new RuntimeEdge<>(runtimeIREdgeId, edgeProperties, src, dst);

  }

  private StageEdge mockStageEdgeFrom(final IRVertex irVertex) {
    return new StageEdge("SEdge" + RUNTIME_EDGE_ID.getAndIncrement(),
        ExecutionPropertyMap.of(mock(IREdge.class), CommunicationPatternProperty.Value.OneToOne),
        irVertex,
        new OperatorVertex(new RelayTransform()),
        mock(Stage.class),
        mock(Stage.class));
  }

  private StageEdge mockStageEdgeTo(final IRVertex irVertex) {
    final ExecutionPropertyMap executionPropertyMap =
      ExecutionPropertyMap.of(mock(IREdge.class), CommunicationPatternProperty.Value.OneToOne);
    return new StageEdge("runtime outgoing edge id",
      executionPropertyMap,
      new OperatorVertex(new RelayTransform()),
      irVertex,
      mock(Stage.class),
      mock(Stage.class));
  }

  private StageEdge mockBroadcastVariableStageEdgeTo(final IRVertex srcVertex,
                                                     final IRVertex dstVertex,
                                                     final Serializable broadcastVariableId,
                                                     final Object broadcastVariable) {
    when(broadcastManagerWorker.get(broadcastVariableId)).thenReturn(broadcastVariable);

    final ExecutionPropertyMap executionPropertyMap =
      ExecutionPropertyMap.of(mock(IREdge.class), CommunicationPatternProperty.Value.OneToOne);
    executionPropertyMap.put(BroadcastVariableIdProperty.of(broadcastVariableId));
    return new StageEdge("runtime outgoing edge id",
      executionPropertyMap,
      srcVertex,
      dstVertex,
      mock(Stage.class),
      mock(Stage.class));
  }

  /**
   * Represents the answer return an inter-stage {@link InputReader},
   * which will have multiple iterable according to the source parallelism.
   */
  private class ParentTaskReaderAnswer implements Answer<InputReader> {
    @Override
    public InputReader answer(final InvocationOnMock invocationOnMock) throws Throwable {
      final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> inputFutures = new ArrayList<>(SOURCE_PARALLELISM);
      final int elementsPerSource = DATA_SIZE / SOURCE_PARALLELISM;
      for (int i = 0; i < SOURCE_PARALLELISM; i++) {
        inputFutures.add(CompletableFuture.completedFuture(
            DataUtil.IteratorWithNumBytes.of(elements.subList(i * elementsPerSource, (i + 1) * elementsPerSource)
                .iterator())));
      }
      final InputReader inputReader = mock(InputReader.class);
      final IRVertex srcVertex = (IRVertex) invocationOnMock.getArgument(1);
      when(inputReader.getSrcIrVertex()).thenReturn(srcVertex);
      when(inputReader.read()).thenReturn(inputFutures);
      when(inputReader.getSourceParallelism()).thenReturn(SOURCE_PARALLELISM);
      return inputReader;
    }
  }

  /**
   * Represents the answer return a {@link OutputWriter},
   * which will stores the data to the map between task id and output data.
   */
  private class ChildTaskWriterAnswer implements Answer<OutputWriter> {
    @Override
    public OutputWriter answer(final InvocationOnMock invocationOnMock) throws Throwable {
      final Object[] args = invocationOnMock.getArguments();
      final RuntimeEdge runtimeEdge = (RuntimeEdge) args[2];
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
  private class RelayTransform<T> implements Transform<T, T> {
    private OutputCollector<T> outputCollector;

    @Override
    public void prepare(final Context context, final OutputCollector<T> outputCollector) {
      this.outputCollector = outputCollector;
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
   * Creates a view.
   * @param <T> input type.
   */
  private class CreateSingleListTransform<T> implements Transform<T, List<T>> {
    private List<T> list;
    private OutputCollector<List<T>> outputCollector;

    @Override
    public void prepare(final Context context, final OutputCollector<List<T>> outputCollector) {
      this.list = new ArrayList<>();
      this.outputCollector = outputCollector;
    }

    @Override
    public void onData(final Object element) {
      list.add((T) element);
    }

    @Override
    public void close() {
      outputCollector.emit(list);
    }
  }

  /**
   * Pairs data element with a broadcast variable.
   * @param <T> input/output type.
   */
  private class BroadcastVariablePairingTransform<T> implements Transform<T, T> {
    private final Serializable broadcastVariableId;
    private Context context;
    private OutputCollector<T> outputCollector;

    public BroadcastVariablePairingTransform(final Serializable broadcastVariableId) {
      this.broadcastVariableId = broadcastVariableId;
    }

    @Override
    public void prepare(final Context context, final OutputCollector<T> outputCollector) {
      this.context = context;
      this.outputCollector = outputCollector;
    }

    @Override
    public void onData(final Object element) {
      final Object broadcastVariable = context.getBroadcastVariable(broadcastVariableId);
      outputCollector.emit((T) Pair.of(broadcastVariable, element));
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
    private Map<String, String> tagToVertex;

    @Override
    public void prepare(final Context context, OutputCollector<Integer> outputCollector) {
      this.outputCollector = outputCollector;
      this.tagToVertex = context.getTagToAdditionalChildren();
    }

    @Override
    public void onData(final Integer element) {
      final int i = element;
      if (i % 2 == 0) {
        // route to all main outputs. Invoked if user calls c.output(element)
        outputCollector.emit(i);
      } else {
        // route to all additional outputs. Invoked if user calls c.output(tupleTag, element)
        tagToVertex.values().forEach(vertex -> outputCollector.emit(vertex, i));
      }
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

  private TaskExecutor getTaskExecutor(final Task task, final DAG<IRVertex, RuntimeEdge<IRVertex>> taskDag) {
    return new TaskExecutor(task, taskDag, taskStateManager, intermediateDataIOFactory, broadcastManagerWorker,
      metricMessageSender, persistentConnectionToMasterMap);
  }
}
