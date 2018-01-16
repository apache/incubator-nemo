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
package edu.snu.onyx.tests.runtime.executor;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.common.ir.Pipe;
import edu.snu.onyx.common.ir.Reader;
import edu.snu.onyx.common.ir.vertex.transform.Transform;
import edu.snu.onyx.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.onyx.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.common.plan.RuntimeEdge;
import edu.snu.onyx.runtime.common.plan.physical.*;
import edu.snu.onyx.runtime.common.state.TaskState;
import edu.snu.onyx.runtime.executor.TaskGroupExecutor;
import edu.snu.onyx.runtime.executor.TaskGroupStateManager;
import edu.snu.onyx.runtime.executor.datatransfer.DataTransferFactory;
import edu.snu.onyx.runtime.executor.datatransfer.InputReader;
import edu.snu.onyx.runtime.executor.datatransfer.OutputWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static edu.snu.onyx.tests.runtime.RuntimeTestUtil.getRangedNumList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link TaskGroupExecutor}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({InputReader.class, OutputWriter.class, DataTransferFactory.class,
    TaskGroupStateManager.class, PhysicalStageEdge.class})
public final class TaskGroupExecutorTest {
  private static final int DATA_SIZE = 100;
  private static final String CONTAINER_TYPE = "CONTAINER_TYPE";
  private static final int SOURCE_PARALLELISM = 5;
  private List elements;
  private Map<String, List<Iterable>> taskIdToOutputData;
  private DataTransferFactory dataTransferFactory;
  private TaskGroupStateManager taskGroupStateManager;
  private Map<String, List<TaskState.State>> taskIdToStateList;
  private List<TaskState.State> expectedTaskStateList;

  @Before
  public void setUp() throws Exception {
    elements = getRangedNumList(0, DATA_SIZE);
    taskIdToStateList = new HashMap<>();
    expectedTaskStateList = new ArrayList<>();
    expectedTaskStateList.add(TaskState.State.EXECUTING);
    expectedTaskStateList.add(TaskState.State.COMPLETE);

    // Mock a TaskGroupStateManager. It accumulates the state change into a list.
    taskGroupStateManager = mock(TaskGroupStateManager.class);

    // Mock a DataTransferFactory.
    taskIdToOutputData = new HashMap<>();
    dataTransferFactory = mock(DataTransferFactory.class);
    when(dataTransferFactory.createLocalReader(any(), any())).then(new IntraStageReaderAnswer());
    when(dataTransferFactory.createReader(any(), any(), any())).then(new InterStageReaderAnswer());
    when(dataTransferFactory.createLocalWriter(any(), any())).then(new WriterAnswer());
    when(dataTransferFactory.createWriter(any(), any(), any())).then(new WriterAnswer());
  }

  /**
   * Test the {@link BoundedSourceTask} processing in {@link TaskGroupExecutor}.
   */
  @Test(timeout=2000)
  public void testSourceTask() throws Exception {
    // Create a task group only having a source task.
    final IRVertex sourceIRVertex = new SimpleIRVertex();
    final String sourceIrVertexId = sourceIRVertex.getId();

    final String sourceTaskId = RuntimeIdGenerator.generateTaskId();
    final String taskGroupId = RuntimeIdGenerator.generateTaskGroupId();
    final String stageId = RuntimeIdGenerator.generateStageId(0);

    final Reader sourceReader = new Reader() {
      @Override
      public Iterator read() throws Exception {
        return elements.iterator();
      }
    };
    final BoundedSourceTask<Integer> boundedSourceTask =
        new BoundedSourceTask<>(sourceTaskId, sourceIrVertexId, 0, sourceReader, taskGroupId);

    final DAG<Task, RuntimeEdge<Task>> taskDag =
        new DAGBuilder<Task, RuntimeEdge<Task>>().addVertex(boundedSourceTask).build();
    final TaskGroup sourceTaskGroup = new TaskGroup(taskGroupId, stageId, 0, taskDag, CONTAINER_TYPE);
    final PhysicalStageEdge stageOutEdge = mock(PhysicalStageEdge.class);
    when(stageOutEdge.getSrcVertex()).thenReturn(sourceIRVertex);

    // Execute the task group.
    final TaskGroupExecutor taskGroupExecutor = new TaskGroupExecutor(
        sourceTaskGroup, taskGroupStateManager, Collections.emptyList(), Collections.singletonList(stageOutEdge),
        dataTransferFactory);
    taskGroupExecutor.execute();

    // Check the output.
    assertEquals(1, taskIdToOutputData.get(sourceTaskId).size());
    assertEquals(elements, taskIdToOutputData.get(sourceTaskId).get(0));
    // Check the state transition.
    taskIdToStateList.forEach((taskId, taskStateList) -> assertEquals(expectedTaskStateList, taskStateList));
  }

  /**
   * Test the {@link OperatorTask} processing in {@link TaskGroupExecutor}.
   *
   * The {@link TaskGroup} to test will looks like:
   * operator task 1 -> operator task 2
   *
   * The output data from upstream stage will be split
   * according to source parallelism through {@link InterStageReaderAnswer}.
   * Because of this, the operator task 1 will process multiple partitions and emit data in multiple times also.
   * On the other hand, operator task 2 will receive the output data once and produce a single output.
   */
  @Test//(timeout=2000)
  public void testOperatorTask() throws Exception {
    // Create a task group with two operator tasks.
    final IRVertex operatorIRVertex1 = new SimpleIRVertex();
    final IRVertex operatorIRVertex2 = new SimpleIRVertex();
    final String operatorIRVertexId1 = operatorIRVertex1.getId();
    final String operatorIRVertexId2 = operatorIRVertex2.getId();
    final String runtimeIREdgeId = "Runtime edge between operator tasks";

    final String operatorTaskId1 = RuntimeIdGenerator.generateTaskId();
    final String operatorTaskId2 = RuntimeIdGenerator.generateTaskId();
    final String taskGroupId = RuntimeIdGenerator.generateTaskGroupId();
    final String stageId = RuntimeIdGenerator.generateStageId(1);

    final OperatorTask operatorTask1 =
        new OperatorTask(operatorTaskId1, operatorIRVertexId1, 0, new SimpleTransform(), taskGroupId);
    final OperatorTask operatorTask2 =
        new OperatorTask(operatorTaskId2, operatorIRVertexId2, 0, new SimpleTransform(), taskGroupId);

    final Coder coder = mock(Coder.class);
    ExecutionPropertyMap edgeProperties = new ExecutionPropertyMap(runtimeIREdgeId);
    edgeProperties.put(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
    final DAG<Task, RuntimeEdge<Task>> taskDag = new DAGBuilder<Task, RuntimeEdge<Task>>()
        .addVertex(operatorTask1)
        .addVertex(operatorTask2)
        .connectVertices(new RuntimeEdge<Task>(
            runtimeIREdgeId, edgeProperties, operatorTask1, operatorTask2, coder))
        .build();
    final TaskGroup operatorTaskGroup = new TaskGroup(taskGroupId, stageId, 0, taskDag, CONTAINER_TYPE);
    final PhysicalStageEdge stageInEdge = mock(PhysicalStageEdge.class);
    when(stageInEdge.getDstVertex()).thenReturn(operatorIRVertex1);
    final PhysicalStageEdge stageOutEdge = mock(PhysicalStageEdge.class);
    when(stageOutEdge.getSrcVertex()).thenReturn(operatorIRVertex2);

    // Execute the task group.
    final TaskGroupExecutor taskGroupExecutor = new TaskGroupExecutor(
        operatorTaskGroup, taskGroupStateManager, Collections.singletonList(stageInEdge),
        Collections.singletonList(stageOutEdge), dataTransferFactory);
    taskGroupExecutor.execute();

    // Check the output.
    assertEquals(SOURCE_PARALLELISM, taskIdToOutputData.get(operatorTaskId1).size()); // Multiple output emission.
    final List<Iterable> outputs = taskIdToOutputData.get(operatorTaskId1);
    final List concatStreamBase = new ArrayList<>();
    Stream<Object> concatStream = concatStreamBase.stream();
    for (int srcIdx = 0; srcIdx < SOURCE_PARALLELISM; srcIdx++) {
      concatStream = Stream.concat(concatStream, StreamSupport.stream(outputs.get(srcIdx).spliterator(), false));
    }
    assertEquals(elements, concatStream.collect(Collectors.toList()));
    assertEquals(1, taskIdToOutputData.get(operatorTaskId2).size());
    assertEquals(elements, taskIdToOutputData.get(operatorTaskId2).get(0));
    // Check the state transition.
    taskIdToStateList.forEach((taskId, taskStateList) -> assertEquals(expectedTaskStateList, taskStateList));
  }

  /**
   * Represents the answer return an intra-stage {@link InputReader},
   * which will have a single iterable from the upstream task.
   */
  private class IntraStageReaderAnswer implements Answer<InputReader> {
    @Override
    public InputReader answer(final InvocationOnMock invocationOnMock) throws Throwable {
      // Read the data.
      final List<CompletableFuture<Iterator>> inputFutures = new ArrayList<>();
      inputFutures.add(CompletableFuture.completedFuture(elements.iterator()));
      final InputReader inputReader = mock(InputReader.class);
      when(inputReader.read()).thenReturn(inputFutures);
      when(inputReader.isSideInputReader()).thenReturn(false);
      when(inputReader.getSourceParallelism()).thenReturn(1);
      return inputReader;
    }
  }

  /**
   * Represents the answer return an inter-stage {@link InputReader},
   * which will have multiple iterable according to the source parallelism.
   */
  private class InterStageReaderAnswer implements Answer<InputReader> {
    @Override
    public InputReader answer(final InvocationOnMock invocationOnMock) throws Throwable {
      final List<CompletableFuture<Iterator>> inputFutures = new ArrayList<>(SOURCE_PARALLELISM);
      final int elementsPerSource = DATA_SIZE / SOURCE_PARALLELISM;
      for (int i = 0; i < SOURCE_PARALLELISM; i++) {
        inputFutures.add(CompletableFuture.completedFuture(
            elements.subList(i * elementsPerSource, (i + 1) * elementsPerSource).iterator()));
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
  private class WriterAnswer implements Answer<OutputWriter> {
    @Override
    public OutputWriter answer(final InvocationOnMock invocationOnMock) throws Throwable {
      final Object[] args = invocationOnMock.getArguments();
      final Task dstTask = (Task) args[0];
      final OutputWriter outputWriter = mock(OutputWriter.class);
      doAnswer(new Answer() {
        @Override
        public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
          final Object[] args = invocationOnMock.getArguments();
          final Iterable dataToWrite = (Iterable) args[0];
          taskIdToOutputData.computeIfAbsent(dstTask.getId(), emptyTaskId -> new ArrayList<>());
          taskIdToOutputData.get(dstTask.getId()).add(dataToWrite);
          return null;
        }
      }).when(outputWriter).write(any());
      return outputWriter;
    }
  }

  /**
   * Simple {@link IRVertex} for testing.
   */
  private class SimpleIRVertex extends IRVertex {
    @Override
    public IRVertex getClone() {
      return null; // Not used.
    }
  }

  /**
   * Simple {@link Transform} for testing.
   * @param <T> input/output type.
   */
  private class SimpleTransform<T> implements Transform<T, T> {
    private Pipe<T> outputCollector;

    @Override
    public void prepare(final Context context, final Pipe<T> outputCollector) {
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
}
