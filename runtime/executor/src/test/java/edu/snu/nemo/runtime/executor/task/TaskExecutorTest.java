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
package edu.snu.nemo.runtime.executor.task;

import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.common.coder.Coder;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.InMemorySourceVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.executor.MetricMessageSender;
import edu.snu.nemo.runtime.executor.TaskStateManager;
import edu.snu.nemo.runtime.executor.data.DataUtil;
import edu.snu.nemo.runtime.executor.datatransfer.DataTransferFactory;
import edu.snu.nemo.runtime.executor.datatransfer.InputReader;
import edu.snu.nemo.runtime.executor.datatransfer.OutputWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests {@link TaskExecutor}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({InputReader.class, OutputWriter.class, DataTransferFactory.class,
    TaskStateManager.class, StageEdge.class})
public final class TaskExecutorTest {
  private static final int DATA_SIZE = 100;
  private static final String CONTAINER_TYPE = "CONTAINER_TYPE";
  private static final int SOURCE_PARALLELISM = 5;
  private List elements;
  private Map<String, List<Object>> vertexIdToOutputData;
  private DataTransferFactory dataTransferFactory;
  private TaskStateManager taskStateManager;
  private MetricMessageSender metricMessageSender;
  private AtomicInteger stageId;

  private String generateTaskId() {
    return RuntimeIdGenerator.generateTaskId(0,
        RuntimeIdGenerator.generateStageId(stageId.getAndIncrement()));
  }

  @Before
  public void setUp() throws Exception {
    elements = getRangedNumList(0, DATA_SIZE);
    stageId = new AtomicInteger(1);

    // Mock a TaskStateManager. It accumulates the state change into a list.
    taskStateManager = mock(TaskStateManager.class);

    // Mock a DataTransferFactory.
    vertexIdToOutputData = new HashMap<>();
    dataTransferFactory = mock(DataTransferFactory.class);
    when(dataTransferFactory.createReader(anyInt(), any(), any())).then(new ParentTaskReaderAnswer());
    when(dataTransferFactory.createWriter(any(), anyInt(), any(), any())).then(new ChildTaskWriterAnswer());

    // Mock a MetricMessageSender.
    metricMessageSender = mock(MetricMessageSender.class);
    doNothing().when(metricMessageSender).send(anyString(), anyString());
    doNothing().when(metricMessageSender).close();
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

    final Task task =
        new Task(
            "testSourceVertexDataFetching",
            generateTaskId(),
            0,
            CONTAINER_TYPE,
            new byte[0],
            Collections.emptyList(),
            Collections.singletonList(mockStageEdgeFrom(sourceIRVertex)),
            vertexIdToReadable);

    // Execute the task.
    final TaskExecutor taskExecutor = new TaskExecutor(
        task, taskDag, taskStateManager, dataTransferFactory, metricMessageSender);
    taskExecutor.execute();

    // Check the output.
    assertEquals(DATA_SIZE, vertexIdToOutputData.get(sourceIRVertex.getId()).size());
    assertEquals(elements.get(0), vertexIdToOutputData.get(sourceIRVertex.getId()).get(0));
  }

  /**
   * Test parent task data fetching.
   */
  @Test(timeout=5000)
  public void testParentTaskDataFetching() throws Exception {
    final IRVertex vertex = new OperatorVertex(new IdentityFunctionTransform());

    final DAG<IRVertex, RuntimeEdge<IRVertex>> taskDag = new DAGBuilder<IRVertex, RuntimeEdge<IRVertex>>()
        .addVertex(vertex)
        .buildWithoutSourceSinkCheck();

    final Task task = new Task(
        "testSourceVertexDataFetching",
        generateTaskId(),
        0,
        CONTAINER_TYPE,
        new byte[0],
        Collections.singletonList(mockStageEdgeTo(vertex)),
        Collections.singletonList(mockStageEdgeFrom(vertex)),
        Collections.emptyMap());

    // Execute the task.
    final TaskExecutor taskExecutor = new TaskExecutor(
        task, taskDag, taskStateManager, dataTransferFactory, metricMessageSender);
    taskExecutor.execute();

    // Check the output.
    assertEquals(100, vertexIdToOutputData.get(vertex.getId()).size());
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
    final IRVertex operatorIRVertex1 = new OperatorVertex(new IdentityFunctionTransform());
    final IRVertex operatorIRVertex2 = new OperatorVertex(new IdentityFunctionTransform());

    final DAG<IRVertex, RuntimeEdge<IRVertex>> taskDag = new DAGBuilder<IRVertex, RuntimeEdge<IRVertex>>()
        .addVertex(operatorIRVertex1)
        .addVertex(operatorIRVertex2)
        .connectVertices(createEdge(operatorIRVertex1, operatorIRVertex2))
        .buildWithoutSourceSinkCheck();

    final Task task = new Task(
        "testSourceVertexDataFetching",
        generateTaskId(),
        0,
        CONTAINER_TYPE,
        new byte[0],
        Collections.singletonList(mockStageEdgeTo(operatorIRVertex1)),
        Collections.singletonList(mockStageEdgeFrom(operatorIRVertex2)),
        Collections.emptyMap());

    // Execute the task.
    final TaskExecutor taskExecutor = new TaskExecutor(
        task, taskDag, taskStateManager, dataTransferFactory, metricMessageSender);
    taskExecutor.execute();

    // Check the output.
    assertEquals(100, vertexIdToOutputData.get(operatorIRVertex2.getId()).size());
  }

  private RuntimeEdge<IRVertex> createEdge(final IRVertex src, final IRVertex dst) {
    final String runtimeIREdgeId = "Runtime edge between operator tasks";
    final Coder coder = Coder.DUMMY_CODER;
    ExecutionPropertyMap edgeProperties = new ExecutionPropertyMap(runtimeIREdgeId);
    edgeProperties.put(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
    return new RuntimeEdge<>(runtimeIREdgeId, edgeProperties, src, dst, coder);

  }

  private StageEdge mockStageEdgeFrom(final IRVertex irVertex) {
    final StageEdge edge = mock(StageEdge.class);
    when(edge.getSrcVertex()).thenReturn(irVertex);
    return edge;
  }

  private StageEdge mockStageEdgeTo(final IRVertex irVertex) {
    final StageEdge edge = mock(StageEdge.class);
    when(edge.getDstVertex()).thenReturn(irVertex);
    return edge;
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
      when(inputReader.read()).thenReturn(inputFutures);
      when(inputReader.isSideInputReader()).thenReturn(false);
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
      final IRVertex vertex = (IRVertex) args[0];
      final OutputWriter outputWriter = mock(OutputWriter.class);
      doAnswer(new Answer() {
        @Override
        public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
          final Object[] args = invocationOnMock.getArguments();
          final Object dataToWrite = args[0];
          vertexIdToOutputData.computeIfAbsent(vertex.getId(), emptyTaskId -> new ArrayList<>());
          vertexIdToOutputData.get(vertex.getId()).add(dataToWrite);
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
  private class IdentityFunctionTransform<T> implements Transform<T, T> {
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
   * Gets a list of integer pair elements in range.
   * @param start value of the range (inclusive).
   * @param end   value of the range (exclusive).
   * @return the list of elements.
   */
  private List getRangedNumList(final int start, final int end) {
    final List numList = new ArrayList<>(end - start);
    IntStream.range(start, end).forEach(number -> numList.add(number));
    return numList;
  }
}
