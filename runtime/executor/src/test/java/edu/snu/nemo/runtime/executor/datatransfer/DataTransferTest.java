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
package edu.snu.nemo.runtime.executor.datatransfer;

import edu.snu.nemo.common.DataSkewMetricFactory;
import edu.snu.nemo.common.HashRange;
import edu.snu.nemo.common.KeyRange;
import edu.snu.nemo.common.coder.*;
import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.*;
import edu.snu.nemo.common.ir.executionproperty.VertexExecutionProperty;
import edu.snu.nemo.common.ir.vertex.SourceVertex;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ScheduleGroupProperty;
import edu.snu.nemo.common.test.EmptyComponents;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageParameters;
import edu.snu.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import edu.snu.nemo.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.nemo.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.executor.Executor;
import edu.snu.nemo.runtime.executor.data.BlockManagerWorker;
import edu.snu.nemo.runtime.executor.data.SerializerManager;
import edu.snu.nemo.runtime.master.*;
import edu.snu.nemo.runtime.master.eventhandler.UpdatePhysicalPlanEventHandler;
import edu.snu.nemo.runtime.master.resource.ContainerManager;
import edu.snu.nemo.runtime.master.scheduler.ExecutorRegistry;
import edu.snu.nemo.runtime.master.scheduler.*;
import org.apache.commons.io.FileUtils;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static edu.snu.nemo.common.dag.DAG.EMPTY_DAG_DIRECTORY;
import static edu.snu.nemo.runtime.common.RuntimeTestUtil.getRangedNumList;
import static edu.snu.nemo.runtime.common.RuntimeTestUtil.flatten;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link InputReader} and {@link OutputWriter}.
 *
 * Execute {@code mvn test -Dtest=DataTransferTest -Dio.netty.leakDetectionLevel=paranoid}
 * to run the test with leakage reports for netty {@link io.netty.util.ReferenceCounted} objects.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PubSubEventHandlerWrapper.class, UpdatePhysicalPlanEventHandler.class, MetricMessageHandler.class,
    SourceVertex.class, ClientRPC.class, MetricManagerMaster.class})
public final class DataTransferTest {
  private static final String EXECUTOR_ID_PREFIX = "Executor";
  private static final InterTaskDataStoreProperty.Value MEMORY_STORE =
      InterTaskDataStoreProperty.Value.MemoryStore;
  private static final InterTaskDataStoreProperty.Value SER_MEMORY_STORE =
      InterTaskDataStoreProperty.Value.SerializedMemoryStore;
  private static final InterTaskDataStoreProperty.Value LOCAL_FILE_STORE =
      InterTaskDataStoreProperty.Value.LocalFileStore;
  private static final InterTaskDataStoreProperty.Value REMOTE_FILE_STORE =
      InterTaskDataStoreProperty.Value.GlusterFileStore;
  private static final String TMP_LOCAL_FILE_DIRECTORY = "./tmpLocalFiles";
  private static final String TMP_REMOTE_FILE_DIRECTORY = "./tmpRemoteFiles";
  private static final int PARALLELISM_TEN = 10;
  private static final String EDGE_PREFIX_TEMPLATE = "Dummy(%d)";
  private static final AtomicInteger TEST_INDEX = new AtomicInteger(0);
  private static final EncoderFactory ENCODER_FACTORY =
      PairEncoderFactory.of(IntEncoderFactory.of(), IntEncoderFactory.of());
  private static final DecoderFactory DECODER_FACTORY =
      PairDecoderFactory.of(IntDecoderFactory.of(), IntDecoderFactory.of());
  private static final Tang TANG = Tang.Factory.getTang();

  private BlockManagerMaster master;
  private BlockManagerWorker worker1;
  private DataTransferFactory transferFactory;
  private BlockManagerWorker worker2;
  private HashMap<BlockManagerWorker, SerializerManager> serializerManagers = new HashMap<>();

  @Before
  public void setUp() throws InjectionException {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(JobConf.ScheduleSerThread.class, "1")
        .build();
    final Injector baseInjector = Tang.Factory.getTang().newInjector(configuration);
    baseInjector.bindVolatileInstance(EvaluatorRequestor.class, mock(EvaluatorRequestor.class));
    final Injector dispatcherInjector = LocalMessageDispatcher.forkInjector(baseInjector);
    final Injector injector = LocalMessageEnvironment.forkInjector(dispatcherInjector,
        MessageEnvironment.MASTER_COMMUNICATION_ID);

    injector.bindVolatileInstance(PubSubEventHandlerWrapper.class, mock(PubSubEventHandlerWrapper.class));
    injector.bindVolatileInstance(UpdatePhysicalPlanEventHandler.class, mock(UpdatePhysicalPlanEventHandler.class));
    final AtomicInteger executorCount = new AtomicInteger(0);
    injector.bindVolatileInstance(ClientRPC.class, mock(ClientRPC.class));
    injector.bindVolatileInstance(MetricManagerMaster.class, mock(MetricManagerMaster.class));
    injector.bindVolatileInstance(MetricMessageHandler.class, mock(MetricMessageHandler.class));
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, EMPTY_DAG_DIRECTORY);

    // Necessary for wiring up the message environments
    injector.getInstance(RuntimeMaster.class);
    final BlockManagerMaster master = injector.getInstance(BlockManagerMaster.class);

    final Injector nameClientInjector = createNameClientInjector();
    nameClientInjector.bindVolatileParameter(JobConf.JobId.class, "data transfer test");

    this.master = master;
    final Pair<BlockManagerWorker, DataTransferFactory> pair1 = createWorker(
        EXECUTOR_ID_PREFIX + executorCount.getAndIncrement(), dispatcherInjector, nameClientInjector);
    this.worker1 = pair1.left();
    this.transferFactory = pair1.right();
    this.worker2 = createWorker(EXECUTOR_ID_PREFIX + executorCount.getAndIncrement(), dispatcherInjector,
        nameClientInjector).left();
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(TMP_LOCAL_FILE_DIRECTORY));
    FileUtils.deleteDirectory(new File(TMP_REMOTE_FILE_DIRECTORY));
  }

  private Pair<BlockManagerWorker, DataTransferFactory> createWorker(
      final String executorId,
      final Injector dispatcherInjector,
      final Injector nameClientInjector) throws InjectionException {
    final Injector messageEnvironmentInjector = LocalMessageEnvironment.forkInjector(dispatcherInjector, executorId);
    final MessageEnvironment messageEnvironment = messageEnvironmentInjector.getInstance(MessageEnvironment.class);
    final PersistentConnectionToMasterMap conToMaster = messageEnvironmentInjector
        .getInstance(PersistentConnectionToMasterMap.class);
    final Configuration executorConfiguration = TANG.newConfigurationBuilder()
        .bindNamedParameter(JobConf.ExecutorId.class, executorId)
        .bindNamedParameter(MessageParameters.SenderId.class, executorId)
        .build();
    final Injector injector = nameClientInjector.forkInjector(executorConfiguration);
    injector.bindVolatileInstance(MessageEnvironment.class, messageEnvironment);
    injector.bindVolatileInstance(PersistentConnectionToMasterMap.class, conToMaster);
    injector.bindVolatileParameter(JobConf.FileDirectory.class, TMP_LOCAL_FILE_DIRECTORY);
    injector.bindVolatileParameter(JobConf.GlusterVolumeDirectory.class, TMP_REMOTE_FILE_DIRECTORY);
    final BlockManagerWorker blockManagerWorker;
    final SerializerManager serializerManager;
    final DataTransferFactory dataTransferFactory;
    try {
      blockManagerWorker = injector.getInstance(BlockManagerWorker.class);
      serializerManager = injector.getInstance(SerializerManager.class);
      serializerManagers.put(blockManagerWorker, serializerManager);
      dataTransferFactory = injector.getInstance(DataTransferFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }

    // Unused, but necessary for wiring up the message environments
    injector.getInstance(Executor.class);

    return Pair.of(blockManagerWorker, dataTransferFactory);
  }

  private Injector createNameClientInjector() {
    try {
      final Configuration configuration = TANG.newConfigurationBuilder()
          .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
          .build();
      final Injector injector = TANG.newInjector(configuration);
      final LocalAddressProvider localAddressProvider = injector.getInstance(LocalAddressProvider.class);
      final NameServer nameServer = injector.getInstance(NameServer.class);
      final Configuration nameClientConfiguration = NameResolverConfiguration.CONF
          .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, localAddressProvider.getLocalAddress())
          .set(NameResolverConfiguration.NAME_SERVICE_PORT, nameServer.getPort())
          .build();
      return injector.forkInjector(nameClientConfiguration);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testWriteAndRead() throws Exception {
    // test OneToOne same worker
    writeAndRead(worker1, worker1, DataCommunicationPatternProperty.Value.OneToOne, MEMORY_STORE);

    // test OneToOne different worker
    writeAndRead(worker1, worker2, DataCommunicationPatternProperty.Value.OneToOne, MEMORY_STORE);

    // test OneToMany same worker
    writeAndRead(worker1, worker1, DataCommunicationPatternProperty.Value.BroadCast, MEMORY_STORE);

    // test OneToMany different worker
    writeAndRead(worker1, worker2, DataCommunicationPatternProperty.Value.BroadCast, MEMORY_STORE);

    // test ManyToMany same worker
    writeAndRead(worker1, worker1, DataCommunicationPatternProperty.Value.Shuffle, MEMORY_STORE);

    // test ManyToMany different worker
    writeAndRead(worker1, worker2, DataCommunicationPatternProperty.Value.Shuffle, MEMORY_STORE);

    // test ManyToMany same worker
    writeAndRead(worker1, worker1, DataCommunicationPatternProperty.Value.Shuffle, SER_MEMORY_STORE);

    // test ManyToMany different worker
    writeAndRead(worker1, worker2, DataCommunicationPatternProperty.Value.Shuffle, SER_MEMORY_STORE);

    // test ManyToMany same worker (local file)
    writeAndRead(worker1, worker1, DataCommunicationPatternProperty.Value.Shuffle, LOCAL_FILE_STORE);

    // test ManyToMany different worker (local file)
    writeAndRead(worker1, worker2, DataCommunicationPatternProperty.Value.Shuffle, LOCAL_FILE_STORE);

    // test ManyToMany same worker (remote file)
    writeAndRead(worker1, worker1, DataCommunicationPatternProperty.Value.Shuffle, REMOTE_FILE_STORE);

    // test ManyToMany different worker (remote file)
    writeAndRead(worker1, worker2, DataCommunicationPatternProperty.Value.Shuffle, REMOTE_FILE_STORE);

    // test OneToOne same worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker1, DataCommunicationPatternProperty.Value.OneToOne, MEMORY_STORE);

    // test OneToOne different worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker2, DataCommunicationPatternProperty.Value.OneToOne, MEMORY_STORE);

    // test OneToMany same worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker1, DataCommunicationPatternProperty.Value.BroadCast, MEMORY_STORE);

    // test OneToMany different worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker2, DataCommunicationPatternProperty.Value.BroadCast, MEMORY_STORE);

    // test ManyToMany same worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker1, DataCommunicationPatternProperty.Value.Shuffle, MEMORY_STORE);

    // test ManyToMany different worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker2, DataCommunicationPatternProperty.Value.Shuffle, MEMORY_STORE);

    // test ManyToMany same worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker1, DataCommunicationPatternProperty.Value.Shuffle, SER_MEMORY_STORE);

    // test ManyToMany different worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker2, DataCommunicationPatternProperty.Value.Shuffle, SER_MEMORY_STORE);

    // test ManyToMany same worker (local file) with duplicate data
    writeAndReadWithDuplicateData(worker1, worker1, DataCommunicationPatternProperty.Value.Shuffle, LOCAL_FILE_STORE);

    // test ManyToMany different worker (local file) with duplicate data
    writeAndReadWithDuplicateData(worker1, worker2, DataCommunicationPatternProperty.Value.Shuffle, LOCAL_FILE_STORE);

    // test ManyToMany same worker (remote file) with duplicate data
    writeAndReadWithDuplicateData(worker1, worker1, DataCommunicationPatternProperty.Value.Shuffle, REMOTE_FILE_STORE);

    // test ManyToMany different worker (remote file) with duplicate data
    writeAndReadWithDuplicateData(worker1, worker2, DataCommunicationPatternProperty.Value.Shuffle, REMOTE_FILE_STORE);
  }

  private void writeAndRead(final BlockManagerWorker sender,
                            final BlockManagerWorker receiver,
                            final DataCommunicationPatternProperty.Value commPattern,
                            final InterTaskDataStoreProperty.Value store) throws RuntimeException {
    final int testIndex = TEST_INDEX.getAndIncrement();
    final String edgeId = String.format(EDGE_PREFIX_TEMPLATE, testIndex);
    final Pair<IRVertex, IRVertex> verticesPair = setupVertices(edgeId, sender, receiver);
    final IRVertex srcVertex = verticesPair.left();
    final IRVertex dstVertex = verticesPair.right();

    // Edge setup
    final IREdge dummyIREdge = new IREdge(commPattern, srcVertex, dstVertex);
    dummyIREdge.setProperty(KeyExtractorProperty.of((element -> element)));
    dummyIREdge.setProperty(DataCommunicationPatternProperty.of(commPattern));
    dummyIREdge.setProperty(PartitionerProperty.of(PartitionerProperty.Value.HashPartitioner));
    dummyIREdge.setProperty(InterTaskDataStoreProperty.of(store));
    dummyIREdge.setProperty(UsedDataHandlingProperty.of(UsedDataHandlingProperty.Value.Keep));
    dummyIREdge.setProperty(EncoderProperty.of(ENCODER_FACTORY));
    dummyIREdge.setProperty(DecoderProperty.of(DECODER_FACTORY));
    if (dummyIREdge.getPropertyValue(DataCommunicationPatternProperty.class).get()
        .equals(DataCommunicationPatternProperty.Value.Shuffle)) {
      final int parallelism = dstVertex.getPropertyValue(ParallelismProperty.class).get();
      final Map<Integer, KeyRange> metric = new HashMap<>();
      for (int i = 0; i < parallelism; i++) {
        metric.put(i, HashRange.of(i, i + 1, false));
      }
      dummyIREdge.setProperty(DataSkewMetricProperty.of(new DataSkewMetricFactory(metric)));
    }
    final ExecutionPropertyMap edgeProperties = dummyIREdge.getExecutionProperties();
    final RuntimeEdge dummyEdge;

    final IRVertex srcMockVertex = mock(IRVertex.class);
    final IRVertex dstMockVertex = mock(IRVertex.class);
    final Stage srcStage = setupStages("srcStage-" + testIndex);
    final Stage dstStage = setupStages("dstStage-" + testIndex);
    dummyEdge = new StageEdge(edgeId, edgeProperties, srcMockVertex, dstMockVertex,
        srcStage, dstStage, false);

    // Initialize states in Master
    srcStage.getTaskIds().forEach(srcTaskId -> {
      final String blockId = RuntimeIdGenerator.generateBlockId(
          edgeId, RuntimeIdGenerator.getIndexFromTaskId(srcTaskId));
      master.initializeState(blockId, srcTaskId);
      master.onProducerTaskScheduled(srcTaskId);
    });

    // Write
    final List<List> dataWrittenList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      final List dataWritten = getRangedNumList(0, PARALLELISM_TEN);
      final OutputWriter writer = transferFactory.createWriter(srcVertex, srcTaskIndex, dstVertex, dummyEdge);
      dataWritten.iterator().forEachRemaining(writer::write);
      writer.close();
      dataWrittenList.add(dataWritten);
    });

    // Read
    final List<List> dataReadList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(dstTaskIndex -> {
      final InputReader reader =
          new InputReader(dstTaskIndex, srcVertex, dummyEdge, receiver);

      if (DataCommunicationPatternProperty.Value.OneToOne.equals(commPattern)) {
        assertEquals(1, reader.getSourceParallelism());
      } else {
        assertEquals(PARALLELISM_TEN, reader.getSourceParallelism());
      }

      final List dataRead = new ArrayList<>();
      try {
        InputReader.combineFutures(reader.read()).forEachRemaining(dataRead::add);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      dataReadList.add(dataRead);
    });

    // Compare (should be the same)
    final List flattenedWrittenData = flatten(dataWrittenList);
    final List flattenedReadData = flatten(dataReadList);
    if (DataCommunicationPatternProperty.Value.BroadCast.equals(commPattern)) {
      final List broadcastedWrittenData = new ArrayList<>();
      IntStream.range(0, PARALLELISM_TEN).forEach(i -> broadcastedWrittenData.addAll(flattenedWrittenData));
      assertEquals(broadcastedWrittenData.size(), flattenedReadData.size());
      flattenedReadData.forEach(rData -> assertTrue(broadcastedWrittenData.remove(rData)));
    } else {
      assertEquals(flattenedWrittenData.size(), flattenedReadData.size());
      flattenedReadData.forEach(rData -> assertTrue(flattenedWrittenData.remove(rData)));
    }
  }

  private void writeAndReadWithDuplicateData(final BlockManagerWorker sender,
                                             final BlockManagerWorker receiver,
                                             final DataCommunicationPatternProperty.Value commPattern,
                                             final InterTaskDataStoreProperty.Value store) throws RuntimeException {
    final int testIndex = TEST_INDEX.getAndIncrement();
    final int testIndex2 = TEST_INDEX.getAndIncrement();
    final String edgeId = String.format(EDGE_PREFIX_TEMPLATE, testIndex);
    final String edgeId2 = String.format(EDGE_PREFIX_TEMPLATE, testIndex2);
    final Pair<IRVertex, IRVertex> verticesPair = setupVertices(edgeId, edgeId2, sender, receiver);
    final IRVertex srcVertex = verticesPair.left();
    final IRVertex dstVertex = verticesPair.right();

    // Edge setup
    final IREdge dummyIREdge = new IREdge(commPattern, srcVertex, dstVertex);
    dummyIREdge.setProperty(EncoderProperty.of(ENCODER_FACTORY));
    dummyIREdge.setProperty(DecoderProperty.of(DECODER_FACTORY));
    dummyIREdge.setProperty(KeyExtractorProperty.of((element -> element)));
    dummyIREdge.setProperty(DataCommunicationPatternProperty.of(commPattern));
    dummyIREdge.setProperty(PartitionerProperty.of(PartitionerProperty.Value.HashPartitioner));
    dummyIREdge.setProperty(DuplicateEdgeGroupProperty.of(new DuplicateEdgeGroupPropertyValue("dummy")));
    final Optional<DuplicateEdgeGroupPropertyValue> duplicateDataProperty
        = dummyIREdge.getPropertyValue(DuplicateEdgeGroupProperty.class);
    duplicateDataProperty.get().setRepresentativeEdgeId(edgeId);
    duplicateDataProperty.get().setGroupSize(2);
    dummyIREdge.setProperty(InterTaskDataStoreProperty.of(store));
    dummyIREdge.setProperty(UsedDataHandlingProperty.of(UsedDataHandlingProperty.Value.Keep));
    if (dummyIREdge.getPropertyValue(DataCommunicationPatternProperty.class).get()
        .equals(DataCommunicationPatternProperty.Value.Shuffle)) {
      final int parallelism = dstVertex.getPropertyValue(ParallelismProperty.class).get();
      final Map<Integer, KeyRange> metric = new HashMap<>();
      for (int i = 0; i < parallelism; i++) {
        metric.put(i, HashRange.of(i, i + 1, false));
      }
      dummyIREdge.setProperty(DataSkewMetricProperty.of(new DataSkewMetricFactory(metric)));
    }
    final RuntimeEdge dummyEdge, dummyEdge2;
    final ExecutionPropertyMap edgeProperties = dummyIREdge.getExecutionProperties();

    final IRVertex srcMockVertex = mock(IRVertex.class);
    final IRVertex dstMockVertex = mock(IRVertex.class);
    final Stage srcStage = setupStages("srcStage-" + testIndex);
    final Stage dstStage = setupStages("dstStage-" + testIndex);
    dummyEdge = new StageEdge(edgeId, edgeProperties, srcMockVertex, dstMockVertex,
        srcStage, dstStage, false);
    final IRVertex dstMockVertex2 = mock(IRVertex.class);
    final Stage dstStage2 = setupStages("dstStage-" + testIndex2);
    dummyEdge2 = new StageEdge(edgeId2, edgeProperties, srcMockVertex, dstMockVertex2,
        srcStage, dstStage, false);
    // Initialize states in Master
    srcStage.getTaskIds().forEach(srcTaskId -> {
      final String blockId = RuntimeIdGenerator.generateBlockId(
          edgeId, RuntimeIdGenerator.getIndexFromTaskId(srcTaskId));
      master.initializeState(blockId, srcTaskId);
      final String blockId2 = RuntimeIdGenerator.generateBlockId(
          edgeId2, RuntimeIdGenerator.getIndexFromTaskId(srcTaskId));
      master.initializeState(blockId2, srcTaskId);
      master.onProducerTaskScheduled(srcTaskId);
    });

    // Write
    final List<List> dataWrittenList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      final List dataWritten = getRangedNumList(0, PARALLELISM_TEN);
      final OutputWriter writer = transferFactory.createWriter(srcVertex, srcTaskIndex, dstVertex, dummyEdge);
      dataWritten.iterator().forEachRemaining(writer::write);
      writer.close();
      dataWrittenList.add(dataWritten);

      final OutputWriter writer2 = transferFactory.createWriter(srcVertex, srcTaskIndex, dstVertex, dummyEdge2);
      dataWritten.iterator().forEachRemaining(writer2::write);
      writer2.close();
    });

    // Read
    final List<List> dataReadList = new ArrayList<>();
    final List<List> dataReadList2 = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(dstTaskIndex -> {
      final InputReader reader =
          new InputReader(dstTaskIndex, srcVertex, dummyEdge, receiver);
      final InputReader reader2 =
          new InputReader(dstTaskIndex, srcVertex, dummyEdge2, receiver);

      if (DataCommunicationPatternProperty.Value.OneToOne.equals(commPattern)) {
        assertEquals(1, reader.getSourceParallelism());
      } else {
        assertEquals(PARALLELISM_TEN, reader.getSourceParallelism());
      }

      if (DataCommunicationPatternProperty.Value.OneToOne.equals(commPattern)) {
        assertEquals(1, reader2.getSourceParallelism());
      } else {
        assertEquals(PARALLELISM_TEN, reader2.getSourceParallelism());
      }

      final List dataRead = new ArrayList<>();
      try {
        InputReader.combineFutures(reader.read()).forEachRemaining(dataRead::add);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      dataReadList.add(dataRead);

      final List dataRead2 = new ArrayList<>();
      try {
        InputReader.combineFutures(reader2.read()).forEachRemaining(dataRead2::add);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      dataReadList2.add(dataRead2);
    });

    // Compare (should be the same)
    final List flattenedWrittenData = flatten(dataWrittenList);
    final List flattenedWrittenData2 = flatten(dataWrittenList);
    final List flattenedReadData = flatten(dataReadList);
    final List flattenedReadData2 = flatten(dataReadList2);
    if (DataCommunicationPatternProperty.Value.BroadCast.equals(commPattern)) {
      final List broadcastedWrittenData = new ArrayList<>();
      final List broadcastedWrittenData2 = new ArrayList<>();
      IntStream.range(0, PARALLELISM_TEN).forEach(i -> broadcastedWrittenData.addAll(flattenedWrittenData));
      IntStream.range(0, PARALLELISM_TEN).forEach(i -> broadcastedWrittenData2.addAll(flattenedWrittenData2));
      assertEquals(broadcastedWrittenData.size(), flattenedReadData.size());
      flattenedReadData.forEach(rData -> assertTrue(broadcastedWrittenData.remove(rData)));
      flattenedReadData2.forEach(rData -> assertTrue(broadcastedWrittenData2.remove(rData)));
    } else {
      assertEquals(flattenedWrittenData.size(), flattenedReadData.size());
      flattenedReadData.forEach(rData -> assertTrue(flattenedWrittenData.remove(rData)));
      flattenedReadData2.forEach(rData -> assertTrue(flattenedWrittenData2.remove(rData)));
    }
  }

  private Pair<IRVertex, IRVertex> setupVertices(final String edgeId,
                                                 final BlockManagerWorker sender,
                                                 final BlockManagerWorker receiver) {
    serializerManagers.get(sender).register(edgeId, ENCODER_FACTORY, DECODER_FACTORY);
    serializerManagers.get(receiver).register(edgeId, ENCODER_FACTORY, DECODER_FACTORY);

    // Src setup
    final SourceVertex srcVertex = new EmptyComponents.EmptySourceVertex("Source");
    final ExecutionPropertyMap srcVertexProperties = srcVertex.getExecutionProperties();
    srcVertexProperties.put(ParallelismProperty.of(PARALLELISM_TEN));

    // Dst setup
    final SourceVertex dstVertex = new EmptyComponents.EmptySourceVertex("Destination");
    final ExecutionPropertyMap dstVertexProperties = dstVertex.getExecutionProperties();
    dstVertexProperties.put(ParallelismProperty.of(PARALLELISM_TEN));

    return Pair.of(srcVertex, dstVertex);
  }

  private Pair<IRVertex, IRVertex> setupVertices(final String edgeId,
                                                 final String edgeId2,
                                                 final BlockManagerWorker sender,
                                                 final BlockManagerWorker receiver) {
    serializerManagers.get(sender).register(edgeId, ENCODER_FACTORY, DECODER_FACTORY);
    serializerManagers.get(receiver).register(edgeId, ENCODER_FACTORY, DECODER_FACTORY);
    serializerManagers.get(sender).register(edgeId2, ENCODER_FACTORY, DECODER_FACTORY);
    serializerManagers.get(receiver).register(edgeId2, ENCODER_FACTORY, DECODER_FACTORY);

    // Src setup
    final SourceVertex srcVertex = new EmptyComponents.EmptySourceVertex("Source");
    final ExecutionPropertyMap srcVertexProperties = srcVertex.getExecutionProperties();
    srcVertexProperties.put(ParallelismProperty.of(PARALLELISM_TEN));

    // Dst setup
    final SourceVertex dstVertex = new EmptyComponents.EmptySourceVertex("Destination");
    final ExecutionPropertyMap dstVertexProperties = dstVertex.getExecutionProperties();
    dstVertexProperties.put(ParallelismProperty.of(PARALLELISM_TEN));

    return Pair.of(srcVertex, dstVertex);
  }

  private Stage setupStages(final String stageId) {
    final DAG<IRVertex, RuntimeEdge<IRVertex>> emptyDag = new DAGBuilder<IRVertex, RuntimeEdge<IRVertex>>().build();

    final ExecutionPropertyMap<VertexExecutionProperty> stageExecutionProperty = new ExecutionPropertyMap<>(stageId);
    stageExecutionProperty.put(ParallelismProperty.of(PARALLELISM_TEN));
    stageExecutionProperty.put(ScheduleGroupProperty.of(0));
    return new Stage(stageId, emptyDag, stageExecutionProperty, Collections.emptyList());
  }
}
