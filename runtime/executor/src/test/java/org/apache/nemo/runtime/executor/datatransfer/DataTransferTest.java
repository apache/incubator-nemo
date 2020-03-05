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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.commons.io.FileUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.coder.*;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ScheduleGroupProperty;
import org.apache.nemo.common.test.EmptyComponents;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.message.ClientRPC;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageParameters;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.message.local.LocalMessageDispatcher;
import org.apache.nemo.runtime.common.message.local.LocalMessageEnvironment;
import org.apache.nemo.runtime.common.plan.PlanRewriter;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.Executor;
import org.apache.nemo.runtime.executor.MetricManagerWorker;
import org.apache.nemo.runtime.executor.TestUtil;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.master.BlockManagerMaster;
import org.apache.nemo.runtime.master.RuntimeMaster;
import org.apache.nemo.runtime.master.metric.MetricManagerMaster;
import org.apache.nemo.runtime.master.metric.MetricMessageHandler;
import org.apache.nemo.runtime.master.scheduler.BatchScheduler;
import org.apache.nemo.runtime.master.scheduler.Scheduler;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.nemo.common.dag.DAG.EMPTY_DAG_DIRECTORY;
import static org.apache.nemo.runtime.common.RuntimeTestUtil.flatten;
import static org.apache.nemo.runtime.common.RuntimeTestUtil.getRangedNumList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link InputReader} and {@link OutputWriter}.
 * <p>
 * Execute {@code mvn test -Dtest=DataTransferTest -Dio.netty.leakDetectionLevel=paranoid}
 * to run the test with leakage reports for netty {@link io.netty.util.ReferenceCounted} objects.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PubSubEventHandlerWrapper.class, MetricMessageHandler.class,
  SourceVertex.class, ClientRPC.class, MetricManagerMaster.class})
public final class DataTransferTest {
  private static final String EXECUTOR_ID_PREFIX = "Executor";
  private static final DataStoreProperty.Value MEMORY_STORE =
    DataStoreProperty.Value.MEMORY_STORE;
  private static final DataStoreProperty.Value SER_MEMORY_STORE =
    DataStoreProperty.Value.SERIALIZED_MEMORY_STORE;
  private static final DataStoreProperty.Value LOCAL_FILE_STORE =
    DataStoreProperty.Value.LOCAL_FILE_STORE;
  private static final DataStoreProperty.Value REMOTE_FILE_STORE =
    DataStoreProperty.Value.GLUSTER_FILE_STORE;
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
  private IntermediateDataIOFactory transferFactory;
  private BlockManagerWorker worker2;
  private HashMap<BlockManagerWorker, SerializerManager> serializerManagers = new HashMap<>();
  private MetricManagerWorker metricMessageSender;

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

    final PlanRewriter planRewriter = mock(PlanRewriter.class);
    injector.bindVolatileInstance(PlanRewriter.class, planRewriter);

    injector.bindVolatileInstance(PubSubEventHandlerWrapper.class, mock(PubSubEventHandlerWrapper.class));
    final AtomicInteger executorCount = new AtomicInteger(0);
    injector.bindVolatileInstance(ClientRPC.class, mock(ClientRPC.class));
    injector.bindVolatileInstance(MetricManagerMaster.class, mock(MetricManagerMaster.class));
    injector.bindVolatileInstance(MetricMessageHandler.class, mock(MetricMessageHandler.class));
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, EMPTY_DAG_DIRECTORY);
    injector.bindVolatileParameter(JobConf.JobId.class, "jobId");

    // Necessary for wiring up the message environments
    injector.bindVolatileInstance(Scheduler.class, injector.getInstance(BatchScheduler.class));
    injector.getInstance(RuntimeMaster.class);
    final BlockManagerMaster master = injector.getInstance(BlockManagerMaster.class);
    final MetricManagerWorker metricMessageSender = injector.getInstance(MetricManagerWorker.class);

    final Injector nameClientInjector = createNameClientInjector();
    nameClientInjector.bindVolatileParameter(JobConf.JobId.class, "data transfer test");

    this.master = master;
    final Pair<BlockManagerWorker, IntermediateDataIOFactory> pair1 = createWorker(
      EXECUTOR_ID_PREFIX + executorCount.getAndIncrement(), dispatcherInjector, nameClientInjector);
    this.worker1 = pair1.left();
    this.transferFactory = pair1.right();
    this.worker2 = createWorker(EXECUTOR_ID_PREFIX + executorCount.getAndIncrement(), dispatcherInjector,
      nameClientInjector).left();

    this.metricMessageSender = metricMessageSender;
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(TMP_LOCAL_FILE_DIRECTORY));
    FileUtils.deleteDirectory(new File(TMP_REMOTE_FILE_DIRECTORY));
  }

  private Pair<BlockManagerWorker, IntermediateDataIOFactory> createWorker(
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
      .bindNamedParameter(JobConf.ExecutorMemoryMb.class, "640")
      .bindNamedParameter(JobConf.MaxOffheapRatio.class, "0.2")
      .build();
    final Injector injector = nameClientInjector.forkInjector(executorConfiguration);
    injector.bindVolatileInstance(MessageEnvironment.class, messageEnvironment);
    injector.bindVolatileInstance(PersistentConnectionToMasterMap.class, conToMaster);
    injector.bindVolatileParameter(JobConf.FileDirectory.class, TMP_LOCAL_FILE_DIRECTORY);
    injector.bindVolatileParameter(JobConf.GlusterVolumeDirectory.class, TMP_REMOTE_FILE_DIRECTORY);
    final BlockManagerWorker blockManagerWorker;
    final SerializerManager serializerManager;
    final IntermediateDataIOFactory intermediateDataIOFactory;
    try {
      blockManagerWorker = injector.getInstance(BlockManagerWorker.class);
      serializerManager = injector.getInstance(SerializerManager.class);
      serializerManagers.put(blockManagerWorker, serializerManager);
      intermediateDataIOFactory = injector.getInstance(IntermediateDataIOFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }

    // Unused, but necessary for wiring up the message environments
    injector.getInstance(Executor.class);

    return Pair.of(blockManagerWorker, intermediateDataIOFactory);
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
  public void testWriteAndRead() {
    // test OneToOne same worker
    writeAndRead(worker1, worker1, CommunicationPatternProperty.Value.ONE_TO_ONE, MEMORY_STORE);

    // test OneToOne different worker
    writeAndRead(worker1, worker2, CommunicationPatternProperty.Value.ONE_TO_ONE, MEMORY_STORE);

    // test OneToMany same worker
    writeAndRead(worker1, worker1, CommunicationPatternProperty.Value.BROADCAST, MEMORY_STORE);

    // test OneToMany different worker
    writeAndRead(worker1, worker2, CommunicationPatternProperty.Value.BROADCAST, MEMORY_STORE);

    // test ManyToMany same worker
    writeAndRead(worker1, worker1, CommunicationPatternProperty.Value.SHUFFLE, MEMORY_STORE);

    // test ManyToMany different worker
    writeAndRead(worker1, worker2, CommunicationPatternProperty.Value.SHUFFLE, MEMORY_STORE);

    // test ManyToMany same worker
    writeAndRead(worker1, worker1, CommunicationPatternProperty.Value.SHUFFLE, SER_MEMORY_STORE);

    // test ManyToMany different worker
    writeAndRead(worker1, worker2, CommunicationPatternProperty.Value.SHUFFLE, SER_MEMORY_STORE);

    // test ManyToMany same worker (local file)
    writeAndRead(worker1, worker1, CommunicationPatternProperty.Value.SHUFFLE, LOCAL_FILE_STORE);

    // test ManyToMany different worker (local file)
    writeAndRead(worker1, worker2, CommunicationPatternProperty.Value.SHUFFLE, LOCAL_FILE_STORE);

    // test ManyToMany same worker (remote file)
    writeAndRead(worker1, worker1, CommunicationPatternProperty.Value.SHUFFLE, REMOTE_FILE_STORE);

    // test ManyToMany different worker (remote file)
    writeAndRead(worker1, worker2, CommunicationPatternProperty.Value.SHUFFLE, REMOTE_FILE_STORE);

    // test OneToOne same worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker1, CommunicationPatternProperty.Value.ONE_TO_ONE, MEMORY_STORE);

    // test OneToOne different worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker2, CommunicationPatternProperty.Value.ONE_TO_ONE, MEMORY_STORE);

    // test OneToMany same worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker1, CommunicationPatternProperty.Value.BROADCAST, MEMORY_STORE);

    // test OneToMany different worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker2, CommunicationPatternProperty.Value.BROADCAST, MEMORY_STORE);

    // test ManyToMany same worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker1, CommunicationPatternProperty.Value.SHUFFLE, MEMORY_STORE);

    // test ManyToMany different worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker2, CommunicationPatternProperty.Value.SHUFFLE, MEMORY_STORE);

    // test ManyToMany same worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker1, CommunicationPatternProperty.Value.SHUFFLE, SER_MEMORY_STORE);

    // test ManyToMany different worker with duplicate data
    writeAndReadWithDuplicateData(worker1, worker2, CommunicationPatternProperty.Value.SHUFFLE, SER_MEMORY_STORE);

    // test ManyToMany same worker (local file) with duplicate data
    writeAndReadWithDuplicateData(worker1, worker1, CommunicationPatternProperty.Value.SHUFFLE, LOCAL_FILE_STORE);

    // test ManyToMany different worker (local file) with duplicate data
    writeAndReadWithDuplicateData(worker1, worker2, CommunicationPatternProperty.Value.SHUFFLE, LOCAL_FILE_STORE);

    // test ManyToMany same worker (remote file) with duplicate data
    writeAndReadWithDuplicateData(worker1, worker1, CommunicationPatternProperty.Value.SHUFFLE, REMOTE_FILE_STORE);

    // test ManyToMany different worker (remote file) with duplicate data
    writeAndReadWithDuplicateData(worker1, worker2, CommunicationPatternProperty.Value.SHUFFLE, REMOTE_FILE_STORE);
  }

  private void writeAndRead(final BlockManagerWorker sender,
                            final BlockManagerWorker receiver,
                            final CommunicationPatternProperty.Value commPattern,
                            final DataStoreProperty.Value store) throws RuntimeException {
    final int testIndex = TEST_INDEX.getAndIncrement();
    final String edgeId = String.format(EDGE_PREFIX_TEMPLATE, testIndex);
    final Pair<IRVertex, IRVertex> verticesPair = setupVertices(edgeId, sender, receiver);
    final IRVertex srcVertex = verticesPair.left();
    final IRVertex dstVertex = verticesPair.right();

    // Edge setup
    final IREdge dummyIREdge = new IREdge(commPattern, srcVertex, dstVertex);
    dummyIREdge.setProperty(KeyExtractorProperty.of(element -> element));
    dummyIREdge.setProperty(CommunicationPatternProperty.of(commPattern));
    dummyIREdge.setProperty(PartitionerProperty.of(PartitionerProperty.Type.HASH));
    dummyIREdge.setProperty(DataStoreProperty.of(store));
    dummyIREdge.setProperty(DataPersistenceProperty.of(DataPersistenceProperty.Value.KEEP));
    dummyIREdge.setProperty(EncoderProperty.of(ENCODER_FACTORY));
    dummyIREdge.setProperty(DecoderProperty.of(DECODER_FACTORY));
    final ExecutionPropertyMap edgeProperties = dummyIREdge.getExecutionProperties();
    final RuntimeEdge dummyEdge;

    final Stage srcStage = setupStages("srcStage" + testIndex);
    final Stage dstStage = setupStages("dstStage" + testIndex);
    dummyEdge = new StageEdge(edgeId, edgeProperties, srcVertex, dstVertex, srcStage, dstStage);

    // Initialize states in Master
    TestUtil.generateTaskIds(srcStage).forEach(srcTaskId -> {
      final String blockId = RuntimeIdManager.generateBlockId(edgeId, srcTaskId);
      master.onProducerTaskScheduled(srcTaskId, Collections.singleton(blockId));
    });

    // Write
    final List<List> dataWrittenList = new ArrayList<>();
    TestUtil.generateTaskIds(srcStage).forEach(srcTaskId -> {
      final List dataWritten = getRangedNumList(0, PARALLELISM_TEN);
      final OutputWriter writer = transferFactory.createWriter(srcTaskId, dummyEdge);
      dataWritten.iterator().forEachRemaining(writer::write);
      writer.close();
      dataWrittenList.add(dataWritten);
    });

    // Read
    final List<List> dataReadList = new ArrayList<>();
    TestUtil.generateTaskIds(dstStage).forEach(dstTaskId -> {
      final InputReader reader =
        new BlockInputReader(dstTaskId, srcVertex, dummyEdge, receiver, metricMessageSender);
      assertEquals(PARALLELISM_TEN, InputReader.getSourceParallelism(reader));

      final List dataRead = new ArrayList<>();
      try {
        combineFutures(reader.read()).forEachRemaining(dataRead::add);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      dataReadList.add(dataRead);
    });

    // Compare (should be the same)
    final List flattenedWrittenData = flatten(dataWrittenList);
    final List flattenedReadData = flatten(dataReadList);
    if (CommunicationPatternProperty.Value.BROADCAST.equals(commPattern)) {
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
                                             final CommunicationPatternProperty.Value commPattern,
                                             final DataStoreProperty.Value store) throws RuntimeException {
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
    dummyIREdge.setProperty(KeyExtractorProperty.of(element -> element));
    dummyIREdge.setProperty(CommunicationPatternProperty.of(commPattern));
    dummyIREdge.setProperty(PartitionerProperty.of(PartitionerProperty.Type.HASH));
    dummyIREdge.setProperty(DuplicateEdgeGroupProperty.of(new DuplicateEdgeGroupPropertyValue("dummy")));
    final Optional<DuplicateEdgeGroupPropertyValue> duplicateDataProperty
      = dummyIREdge.getPropertyValue(DuplicateEdgeGroupProperty.class);
    duplicateDataProperty.get().setRepresentativeEdgeId(edgeId);
    duplicateDataProperty.get().setGroupSize(2);
    dummyIREdge.setProperty(DataStoreProperty.of(store));
    dummyIREdge.setProperty(DataPersistenceProperty.of(DataPersistenceProperty.Value.KEEP));
    final RuntimeEdge dummyEdge, dummyEdge2;
    final ExecutionPropertyMap edgeProperties = dummyIREdge.getExecutionProperties();

    final Stage srcStage = setupStages("srcStage" + testIndex);
    final Stage dstStage = setupStages("dstStage" + testIndex);
    dummyEdge = new StageEdge(edgeId, edgeProperties, srcVertex, dstVertex, srcStage, dstStage);
    dummyEdge2 = new StageEdge(edgeId2, edgeProperties, srcVertex, dstVertex, srcStage, dstStage);
    // Initialize states in Master
    TestUtil.generateTaskIds(srcStage).forEach(srcTaskId -> {
      final String blockId = RuntimeIdManager.generateBlockId(edgeId, srcTaskId);
      final String blockId2 = RuntimeIdManager.generateBlockId(edgeId2, srcTaskId);
      master.onProducerTaskScheduled(srcTaskId, Stream.of(blockId, blockId2).collect(Collectors.toSet()));
    });

    // Write
    final List<List> dataWrittenList = new ArrayList<>();
    TestUtil.generateTaskIds(srcStage).forEach(srcTaskId -> {
      final List dataWritten = getRangedNumList(0, PARALLELISM_TEN);
      final OutputWriter writer = transferFactory.createWriter(srcTaskId, dummyEdge);
      dataWritten.iterator().forEachRemaining(writer::write);
      writer.close();
      dataWrittenList.add(dataWritten);

      final OutputWriter writer2 = transferFactory.createWriter(srcTaskId, dummyEdge2);
      dataWritten.iterator().forEachRemaining(writer2::write);
      writer2.close();
    });

    // Read
    final List<List> dataReadList = new ArrayList<>();
    final List<List> dataReadList2 = new ArrayList<>();

    TestUtil.generateTaskIds(dstStage).forEach(dstTaskId -> {
      final InputReader reader =
        new BlockInputReader(dstTaskId, srcVertex, dummyEdge, receiver, metricMessageSender);
      final InputReader reader2 =
        new BlockInputReader(dstTaskId, srcVertex, dummyEdge2, receiver, metricMessageSender);

      assertEquals(PARALLELISM_TEN, InputReader.getSourceParallelism(reader));

      assertEquals(PARALLELISM_TEN, InputReader.getSourceParallelism(reader));

      final List dataRead = new ArrayList<>();
      try {
        combineFutures(reader.read()).forEachRemaining(dataRead::add);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      dataReadList.add(dataRead);

      final List dataRead2 = new ArrayList<>();
      try {
        combineFutures(reader2.read()).forEachRemaining(dataRead2::add);
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
    if (CommunicationPatternProperty.Value.BROADCAST.equals(commPattern)) {
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
    srcVertex.setProperty(ParallelismProperty.of(PARALLELISM_TEN));

    // Dst setup
    final SourceVertex dstVertex = new EmptyComponents.EmptySourceVertex("Destination");
    dstVertex.setProperty(ParallelismProperty.of(PARALLELISM_TEN));

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
    srcVertex.setProperty(ParallelismProperty.of(PARALLELISM_TEN));

    // Dst setup
    final SourceVertex dstVertex = new EmptyComponents.EmptySourceVertex("Destination");
    dstVertex.setProperty(ParallelismProperty.of(PARALLELISM_TEN));

    return Pair.of(srcVertex, dstVertex);
  }

  private Stage setupStages(final String stageId) {
    final DAG<IRVertex, RuntimeEdge<IRVertex>> emptyDag = new DAGBuilder<IRVertex, RuntimeEdge<IRVertex>>().build();

    final ExecutionPropertyMap<VertexExecutionProperty> stageExecutionProperty = new ExecutionPropertyMap<>(stageId);
    stageExecutionProperty.put(ParallelismProperty.of(PARALLELISM_TEN));
    stageExecutionProperty.put(ScheduleGroupProperty.of(0));
    return new Stage(
      stageId,
      IntStream.range(0, PARALLELISM_TEN).boxed().collect(Collectors.toList()),
      emptyDag,
      stageExecutionProperty,
      Collections.emptyList());
  }

  /**
   * Combine the given list of futures.
   *
   * @param futures to combine.
   * @return the combined iterable of elements.
   * @throws ExecutionException   when fail to get results from futures.
   * @throws InterruptedException when interrupted during getting results from futures.
   */
  private Iterator combineFutures(final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures)
    throws ExecutionException, InterruptedException {
    final List concatStreamBase = new ArrayList<>();
    Stream<Object> concatStream = concatStreamBase.stream();
    for (int srcTaskIdx = 0; srcTaskIdx < futures.size(); srcTaskIdx++) {
      final Iterator dataFromATask = futures.get(srcTaskIdx).get();
      final Iterable iterable = () -> dataFromATask;
      concatStream = Stream.concat(concatStream, StreamSupport.stream(iterable.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList()).iterator();
  }
}

