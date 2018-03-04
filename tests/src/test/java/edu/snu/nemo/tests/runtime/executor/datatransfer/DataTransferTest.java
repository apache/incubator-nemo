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
package edu.snu.nemo.tests.runtime.executor.datatransfer;

import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.*;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.SourceVertex;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.compiler.optimizer.examples.EmptyComponents;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.coder.Coder;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.compiler.frontend.beam.coder.BeamCoder;
import edu.snu.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageParameters;
import edu.snu.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import edu.snu.nemo.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.nemo.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalStage;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.nemo.runtime.common.plan.physical.Task;
import edu.snu.nemo.runtime.executor.Executor;
import edu.snu.nemo.runtime.executor.MetricManagerWorker;
import edu.snu.nemo.runtime.executor.data.BlockManagerWorker;
import edu.snu.nemo.runtime.executor.data.SerializerManager;
import edu.snu.nemo.runtime.executor.datatransfer.DataTransferFactory;
import edu.snu.nemo.runtime.executor.datatransfer.InputReader;
import edu.snu.nemo.runtime.executor.datatransfer.OutputWriter;
import edu.snu.nemo.runtime.master.MetricMessageHandler;
import edu.snu.nemo.runtime.master.BlockManagerMaster;
import edu.snu.nemo.runtime.master.eventhandler.UpdatePhysicalPlanEventHandler;
import edu.snu.nemo.runtime.master.RuntimeMaster;
import edu.snu.nemo.runtime.master.resource.ContainerManager;
import edu.snu.nemo.runtime.master.scheduler.ExecutorRegistry;
import edu.snu.nemo.runtime.master.scheduler.*;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static edu.snu.nemo.common.dag.DAG.EMPTY_DAG_DIRECTORY;
import static edu.snu.nemo.tests.runtime.RuntimeTestUtil.flatten;
import static edu.snu.nemo.tests.runtime.RuntimeTestUtil.getRangedNumList;
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
    SourceVertex.class})
public final class DataTransferTest {
  private static final String EXECUTOR_ID_PREFIX = "Executor";
  private static final int EXECUTOR_CAPACITY = 1;
  private static final int MAX_SCHEDULE_ATTEMPT = 2;
  private static final int SCHEDULE_TIMEOUT = 1000;
  private static final DataStoreProperty.Value MEMORY_STORE = DataStoreProperty.Value.MemoryStore;
  private static final DataStoreProperty.Value SER_MEMORY_STORE = DataStoreProperty.Value.SerializedMemoryStore;
  private static final DataStoreProperty.Value LOCAL_FILE_STORE = DataStoreProperty.Value.LocalFileStore;
  private static final DataStoreProperty.Value REMOTE_FILE_STORE = DataStoreProperty.Value.GlusterFileStore;
  private static final String TMP_LOCAL_FILE_DIRECTORY = "./tmpLocalFiles";
  private static final String TMP_REMOTE_FILE_DIRECTORY = "./tmpRemoteFiles";
  private static final int PARALLELISM_TEN = 10;
  private static final String EDGE_PREFIX_TEMPLATE = "Dummy(%d)";
  private static final AtomicInteger TEST_INDEX = new AtomicInteger(0);
  private static final Coder CODER = new BeamCoder(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));
  private static final Tang TANG = Tang.Factory.getTang();
  private static final int HASH_RANGE_MULTIPLIER = 10;

  private BlockManagerMaster master;
  private BlockManagerWorker worker1;
  private BlockManagerWorker worker2;
  private HashMap<BlockManagerWorker, SerializerManager> serializerManagers = new HashMap<>();

  @Before
  public void setUp() throws InjectionException {
    final LocalMessageDispatcher messageDispatcher = new LocalMessageDispatcher();
    final LocalMessageEnvironment messageEnvironment =
        new LocalMessageEnvironment(MessageEnvironment.MASTER_COMMUNICATION_ID, messageDispatcher);
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(JobConf.ScheduleSerThread.class, "1")
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    injector.bindVolatileInstance(EvaluatorRequestor.class, mock(EvaluatorRequestor.class));
    injector.bindVolatileInstance(MessageEnvironment.class, messageEnvironment);
    final ContainerManager containerManager = injector.getInstance(ContainerManager.class);

    final MetricMessageHandler metricMessageHandler = mock(MetricMessageHandler.class);
    final PubSubEventHandlerWrapper pubSubEventHandler = mock(PubSubEventHandlerWrapper.class);
    final UpdatePhysicalPlanEventHandler updatePhysicalPlanEventHandler = mock(UpdatePhysicalPlanEventHandler.class);
    final SchedulingPolicy schedulingPolicy = new RoundRobinSchedulingPolicy(
        injector.getInstance(ExecutorRegistry.class), SCHEDULE_TIMEOUT);
    final PendingTaskGroupQueue taskGroupQueue = new SingleJobTaskGroupQueue();
    final SchedulerRunner schedulerRunner = new SchedulerRunner(schedulingPolicy, taskGroupQueue);
    final Scheduler scheduler =
        new BatchSingleJobScheduler(schedulingPolicy, schedulerRunner, taskGroupQueue, master,
            pubSubEventHandler, updatePhysicalPlanEventHandler);
    final AtomicInteger executorCount = new AtomicInteger(0);

    // Necessary for wiring up the message environments
    final RuntimeMaster runtimeMaster =
        new RuntimeMaster(scheduler, containerManager, master,
            metricMessageHandler, messageEnvironment, EMPTY_DAG_DIRECTORY);

    final Injector injector1 = Tang.Factory.getTang().newInjector();
    injector1.bindVolatileInstance(MessageEnvironment.class, messageEnvironment);
    injector1.bindVolatileInstance(RuntimeMaster.class, runtimeMaster);
    final BlockManagerMaster master = injector1.getInstance(BlockManagerMaster.class);

    final Injector injector2 = createNameClientInjector();
    injector2.bindVolatileParameter(JobConf.JobId.class, "data transfer test");

    this.master = master;
    this.worker1 = createWorker(EXECUTOR_ID_PREFIX + executorCount.getAndIncrement(), messageDispatcher,
        injector2);
    this.worker2 = createWorker(EXECUTOR_ID_PREFIX + executorCount.getAndIncrement(), messageDispatcher,
        injector2);
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(TMP_LOCAL_FILE_DIRECTORY));
    FileUtils.deleteDirectory(new File(TMP_REMOTE_FILE_DIRECTORY));
  }

  private BlockManagerWorker createWorker(final String executorId, final LocalMessageDispatcher messageDispatcher,
                                          final Injector nameClientInjector) {
    final LocalMessageEnvironment messageEnvironment = new LocalMessageEnvironment(executorId, messageDispatcher);
    final PersistentConnectionToMasterMap conToMaster = new PersistentConnectionToMasterMap(messageEnvironment);
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
    final MetricManagerWorker metricManagerWorker;
    final SerializerManager serializerManager;
    try {
      blockManagerWorker = injector.getInstance(BlockManagerWorker.class);
      metricManagerWorker =  injector.getInstance(MetricManagerWorker.class);
      serializerManager = injector.getInstance(SerializerManager.class);
      serializerManagers.put(blockManagerWorker, serializerManager);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }

    // Unused, but necessary for wiring up the message environments
    final Executor executor = new Executor(
        executorId,
        EXECUTOR_CAPACITY,
        conToMaster,
        messageEnvironment,
        serializerManager,
        new DataTransferFactory(HASH_RANGE_MULTIPLIER, blockManagerWorker),
        metricManagerWorker);
    injector.bindVolatileInstance(Executor.class, executor);

    return blockManagerWorker;
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
                            final DataStoreProperty.Value store) throws RuntimeException {
    final int testIndex = TEST_INDEX.getAndIncrement();
    final String edgeId = String.format(EDGE_PREFIX_TEMPLATE, testIndex);
    final Pair<IRVertex, IRVertex> verticesPair = setupVertices(edgeId, sender, receiver);
    final IRVertex srcVertex = verticesPair.left();
    final IRVertex dstVertex = verticesPair.right();

    // Edge setup
    final IREdge dummyIREdge = new IREdge(commPattern, srcVertex, dstVertex, CODER);
    dummyIREdge.setProperty(KeyExtractorProperty.of((element -> element)));
    final ExecutionPropertyMap edgeProperties = dummyIREdge.getExecutionProperties();
    edgeProperties.put(DataCommunicationPatternProperty.of(commPattern));
    edgeProperties.put(PartitionerProperty.of(PartitionerProperty.Value.HashPartitioner));

    edgeProperties.put(DataStoreProperty.of(store));
    edgeProperties.put(UsedDataHandlingProperty.of(UsedDataHandlingProperty.Value.Keep));
    final RuntimeEdge dummyEdge;

    final IRVertex srcMockVertex = mock(IRVertex.class);
    final IRVertex dstMockVertex = mock(IRVertex.class);
    final PhysicalStage srcStage = setupStages("srcStage-" + testIndex);
    final PhysicalStage dstStage = setupStages("dstStage-" + testIndex);
    dummyEdge = new PhysicalStageEdge(edgeId, edgeProperties, srcMockVertex, dstMockVertex,
        srcStage, dstStage, CODER, false);
    // Initialize states in Master
    srcStage.getTaskGroupIds().forEach(srcTaskGroupId -> {
      final String blockId = RuntimeIdGenerator.generateBlockId(
          edgeId, RuntimeIdGenerator.getIndexFromTaskGroupId(srcTaskGroupId));
      master.initializeState(blockId, srcTaskGroupId);
      master.onProducerTaskGroupScheduled(srcTaskGroupId);
    });

    // Write
    final List<List> dataWrittenList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      final List dataWritten = getRangedNumList(0, PARALLELISM_TEN);
      final OutputWriter writer = new OutputWriter(HASH_RANGE_MULTIPLIER, srcTaskIndex, srcVertex.getId(), dstVertex,
          dummyEdge, sender);
      writer.write(dataWritten);
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
                                             final DataStoreProperty.Value store) throws RuntimeException {
    final int testIndex = TEST_INDEX.getAndIncrement();
    final int testIndex2 = TEST_INDEX.getAndIncrement();
    final String edgeId = String.format(EDGE_PREFIX_TEMPLATE, testIndex);
    final String edgeId2 = String.format(EDGE_PREFIX_TEMPLATE, testIndex2);
    final Pair<IRVertex, IRVertex> verticesPair = setupVertices(edgeId, edgeId2, sender, receiver);
    final IRVertex srcVertex = verticesPair.left();
    final IRVertex dstVertex = verticesPair.right();

    // Edge setup
    final IREdge dummyIREdge = new IREdge(commPattern, srcVertex, dstVertex, CODER);
    dummyIREdge.setProperty(KeyExtractorProperty.of((element -> element)));
    final ExecutionPropertyMap edgeProperties = dummyIREdge.getExecutionProperties();
    edgeProperties.put(DataCommunicationPatternProperty.of(commPattern));
    edgeProperties.put(PartitionerProperty.of(PartitionerProperty.Value.HashPartitioner));
    edgeProperties.put(DuplicateEdgeGroupProperty.of(new DuplicateEdgeGroupPropertyValue("dummy")));
    final DuplicateEdgeGroupPropertyValue duplicateDataProperty = edgeProperties.get(ExecutionProperty.Key.DuplicateEdgeGroup);
    duplicateDataProperty.setRepresentativeEdgeId(edgeId);
    duplicateDataProperty.setGroupSize(2);

    edgeProperties.put(DataStoreProperty.of(store));
    edgeProperties.put(UsedDataHandlingProperty.of(UsedDataHandlingProperty.Value.Keep));
    final RuntimeEdge dummyEdge, dummyEdge2;

    final IRVertex srcMockVertex = mock(IRVertex.class);
    final IRVertex dstMockVertex = mock(IRVertex.class);
    final PhysicalStage srcStage = setupStages("srcStage-" + testIndex);
    final PhysicalStage dstStage = setupStages("dstStage-" + testIndex);
    dummyEdge = new PhysicalStageEdge(edgeId, edgeProperties, srcMockVertex, dstMockVertex,
        srcStage, dstStage, CODER, false);
    final IRVertex dstMockVertex2 = mock(IRVertex.class);
    final PhysicalStage dstStage2 = setupStages("dstStage-" + testIndex2);
    dummyEdge2 = new PhysicalStageEdge(edgeId2, edgeProperties, srcMockVertex, dstMockVertex2,
        srcStage, dstStage2, CODER, false);
    // Initialize states in Master
    srcStage.getTaskGroupIds().forEach(srcTaskGroupId -> {
      final String blockId = RuntimeIdGenerator.generateBlockId(
          edgeId, RuntimeIdGenerator.getIndexFromTaskGroupId(srcTaskGroupId));
      master.initializeState(blockId, srcTaskGroupId);
      final String blockId2 = RuntimeIdGenerator.generateBlockId(
          edgeId2, RuntimeIdGenerator.getIndexFromTaskGroupId(srcTaskGroupId));
      master.initializeState(blockId2, srcTaskGroupId);
      master.onProducerTaskGroupScheduled(srcTaskGroupId);
    });

    // Write
    final List<List> dataWrittenList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      final List dataWritten = getRangedNumList(0, PARALLELISM_TEN);
      final OutputWriter writer = new OutputWriter(HASH_RANGE_MULTIPLIER, srcTaskIndex, srcVertex.getId(), dstVertex,
          dummyEdge, sender);
      writer.write(dataWritten);
      writer.close();
      dataWrittenList.add(dataWritten);

      final OutputWriter writer2 = new OutputWriter(HASH_RANGE_MULTIPLIER, srcTaskIndex, srcVertex.getId(), dstVertex,
          dummyEdge2, sender);
      writer2.write(dataWritten);
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
    serializerManagers.get(sender).register(edgeId, CODER, new ExecutionPropertyMap(""));
    serializerManagers.get(receiver).register(edgeId, CODER, new ExecutionPropertyMap(""));

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
    serializerManagers.get(sender).register(edgeId, CODER, new ExecutionPropertyMap(""));
    serializerManagers.get(receiver).register(edgeId, CODER, new ExecutionPropertyMap(""));
    serializerManagers.get(sender).register(edgeId2, CODER, new ExecutionPropertyMap(""));
    serializerManagers.get(receiver).register(edgeId2, CODER, new ExecutionPropertyMap(""));

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

  private PhysicalStage setupStages(final String stageId) {
    final DAG<Task, RuntimeEdge<Task>> emptyDag = new DAGBuilder<Task, RuntimeEdge<Task>>().build();

    return new PhysicalStage(stageId, emptyDag, PARALLELISM_TEN, 0, "Not_used", Collections.emptyList());
  }
}
