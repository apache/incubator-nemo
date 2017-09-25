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
package edu.snu.vortex.runtime.executor.datatransfer;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.common.Pair;
import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.common.coder.BeamCoder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.vortex.common.PubSubEventHandlerWrapper;
import edu.snu.vortex.compiler.ir.executionproperty.edge.DataCommunicationPatternProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.DataStoreProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.PartitioningProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.WriteOptimizationProperty;
import edu.snu.vortex.compiler.ir.executionproperty.vertex.ParallelismProperty;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.vortex.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.vortex.runtime.common.message.ncs.NcsParameters;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.executor.Executor;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMaster;
import edu.snu.vortex.runtime.executor.data.*;
import edu.snu.vortex.runtime.common.metric.PeriodicMetricSender;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.Broadcast;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.DataCommunicationPattern;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.OneToOne;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.ScatterGather;
import edu.snu.vortex.runtime.executor.datatransfer.partitioning.Hash;
import edu.snu.vortex.runtime.master.PartitionManagerMaster;
import edu.snu.vortex.runtime.master.eventhandler.UpdatePhysicalPlanEventHandler;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import edu.snu.vortex.runtime.master.resource.ContainerManager;
import edu.snu.vortex.runtime.master.scheduler.*;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.commons.io.FileUtils;
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

import static edu.snu.vortex.common.dag.DAG.EMPTY_DAG_DIRECTORY;
import static edu.snu.vortex.runtime.RuntimeTestUtil.flatten;
import static edu.snu.vortex.runtime.RuntimeTestUtil.getRangedNumList;
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
@PrepareForTest({PubSubEventHandlerWrapper.class, UpdatePhysicalPlanEventHandler.class})
public final class DataTransferTest {
  private static final String EXECUTOR_ID_PREFIX = "Executor";
  private static final int EXECUTOR_CAPACITY = 1;
  private static final int MAX_SCHEDULE_ATTEMPT = 2;
  private static final int SCHEDULE_TIMEOUT = 1000;
  private static final Class<? extends PartitionStore> MEMORY_STORE = MemoryStore.class;
  private static final Class<? extends PartitionStore> LOCAL_FILE_STORE = LocalFileStore.class;
  private static final Class<? extends PartitionStore> REMOTE_FILE_STORE = GlusterFileStore.class;
  private static final String TMP_LOCAL_FILE_DIRECTORY = "./tmpLocalFiles";
  private static final String TMP_REMOTE_FILE_DIRECTORY = "./tmpRemoteFiles";
  private static final int PARALLELISM_TEN = 10;
  private static final String EDGE_PREFIX_TEMPLATE = "Dummy(%d)";
  private static final AtomicInteger TEST_INDEX = new AtomicInteger(0);
  private static final String TASKGROUP_PREFIX_TEMPLATE = "DummyTG(%d)_";
  private static final Coder CODER = new BeamCoder(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));
  private static final Tang TANG = Tang.Factory.getTang();
  private static final int HASH_RANGE_MULTIPLIER = 10;
  private static final int I_FILE_DATA_SIZE = 1000;

  private PartitionManagerMaster master;
  private PartitionManagerWorker worker1;
  private PartitionManagerWorker worker2;

  @Before
  public void setUp() throws InjectionException {
    final LocalMessageDispatcher messageDispatcher = new LocalMessageDispatcher();
    final LocalMessageEnvironment messageEnvironment =
        new LocalMessageEnvironment(MessageEnvironment.MASTER_COMMUNICATION_ID, messageDispatcher);
    final ContainerManager containerManager = new ContainerManager(null,
                                                                    messageEnvironment);
    final PubSubEventHandlerWrapper pubSubEventHandler = mock(PubSubEventHandlerWrapper.class);
    final UpdatePhysicalPlanEventHandler updatePhysicalPlanEventHandler = mock(UpdatePhysicalPlanEventHandler.class);
    final Scheduler scheduler =
        new BatchScheduler(master, new RoundRobinSchedulingPolicy(containerManager, SCHEDULE_TIMEOUT),
            new PendingTaskGroupPriorityQueue(), pubSubEventHandler);
    final AtomicInteger executorCount = new AtomicInteger(0);

    final Injector injector1 = Tang.Factory.getTang().newInjector();
    final PartitionManagerMaster master = injector1.getInstance(PartitionManagerMaster.class);
    // Unused, but necessary for wiring up the message environments
    final RuntimeMaster runtimeMaster =
        new RuntimeMaster(scheduler, containerManager, messageEnvironment, master,
            updatePhysicalPlanEventHandler, EMPTY_DAG_DIRECTORY, MAX_SCHEDULE_ATTEMPT);
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

  private PartitionManagerWorker createWorker(final String executorId, final LocalMessageDispatcher messageDispatcher,
                                              final Injector nameClientInjector) {
    final LocalMessageEnvironment messageEnvironment = new LocalMessageEnvironment(executorId, messageDispatcher);
    final PersistentConnectionToMaster conToMaster = new PersistentConnectionToMaster(messageEnvironment);
    final Configuration executorConfiguration = TANG.newConfigurationBuilder()
        .bindNamedParameter(JobConf.ExecutorId.class, executorId)
        .bindNamedParameter(NcsParameters.SenderId.class, executorId)
        .build();
    final Injector injector = nameClientInjector.forkInjector(executorConfiguration);
    injector.bindVolatileInstance(MessageEnvironment.class, messageEnvironment);
    injector.bindVolatileInstance(PersistentConnectionToMaster.class, conToMaster);
    injector.bindVolatileParameter(JobConf.FileDirectory.class, TMP_LOCAL_FILE_DIRECTORY);
    injector.bindVolatileParameter(JobConf.GlusterVolumeDirectory.class, TMP_REMOTE_FILE_DIRECTORY);
    final PartitionManagerWorker partitionManagerWorker;
    final PeriodicMetricSender periodicMetricSender;
    try {
      partitionManagerWorker = injector.getInstance(PartitionManagerWorker.class);
      periodicMetricSender =  injector.getInstance(PeriodicMetricSender.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }

    // Unused, but necessary for wiring up the message environments
    final Executor executor = new Executor(
        executorId,
        EXECUTOR_CAPACITY,
        conToMaster,
        messageEnvironment,
        partitionManagerWorker,
        new DataTransferFactory(HASH_RANGE_MULTIPLIER, partitionManagerWorker),
        periodicMetricSender);
    injector.bindVolatileInstance(Executor.class, executor);

    return partitionManagerWorker;
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
    writeAndRead(worker1, worker1, OneToOne.class, MEMORY_STORE);

    // test OneToOne different worker
    writeAndRead(worker1, worker2, OneToOne.class, MEMORY_STORE);

    // test OneToMany same worker
    writeAndRead(worker1, worker1, Broadcast.class, MEMORY_STORE);

    // test OneToMany different worker
    writeAndRead(worker1, worker2, Broadcast.class, MEMORY_STORE);

    // test ManyToMany same worker
    writeAndRead(worker1, worker1, ScatterGather.class, MEMORY_STORE);

    // test ManyToMany different worker
    writeAndRead(worker1, worker2, ScatterGather.class, MEMORY_STORE);

    // test ManyToMany same worker (local file)
    writeAndRead(worker1, worker1, ScatterGather.class, LOCAL_FILE_STORE);

    // test ManyToMany different worker (local file)
    writeAndRead(worker1, worker2, ScatterGather.class, LOCAL_FILE_STORE);

    // test ManyToMany same worker (remote file)
    writeAndRead(worker1, worker1, ScatterGather.class, REMOTE_FILE_STORE);

    // test ManyToMany different worker (remote file)
    writeAndRead(worker1, worker2, ScatterGather.class, REMOTE_FILE_STORE);
  }

  @Test
  public void testIFileWriteAndRead() throws Exception {
    // test ManyToMany same worker (remote file)
    iFileWriteAndRead(worker1, worker1, REMOTE_FILE_STORE);

    // test ManyToMany different worker (remote file)
    iFileWriteAndRead(worker1, worker2, REMOTE_FILE_STORE);
  }

  private void writeAndRead(final PartitionManagerWorker sender,
                            final PartitionManagerWorker receiver,
                            final Class<? extends DataCommunicationPattern> commPattern,
                            final Class<? extends PartitionStore> store) throws RuntimeException {
    final int testIndex = TEST_INDEX.getAndIncrement();
    final String edgeId = String.format(EDGE_PREFIX_TEMPLATE, testIndex);
    final String taskGroupPrefix = String.format(TASKGROUP_PREFIX_TEMPLATE, testIndex);
    final Pair<IRVertex, IRVertex> verticesPair = setupVertices(edgeId, sender, receiver);
    final IRVertex srcVertex = verticesPair.left();
    final IRVertex dstVertex = verticesPair.right();

    // Edge setup
    final IREdge dummyIREdge = new IREdge(IREdge.Type.OneToOne, srcVertex, dstVertex, CODER);
    final ExecutionPropertyMap edgeProperties = dummyIREdge.getExecutionProperties();
    edgeProperties.put(DataCommunicationPatternProperty.of(commPattern));
    edgeProperties.put(PartitioningProperty.of(Hash.class));
    edgeProperties.put(DataStoreProperty.of(store));
    final RuntimeEdge<IRVertex> dummyEdge
        = new RuntimeEdge<>(edgeId, edgeProperties, srcVertex, dstVertex, CODER);

    // Initialize states in Master
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      if (commPattern.equals(ScatterGather.class)) {
        IntStream.range(0, PARALLELISM_TEN).forEach(dstTaskIndex -> {
          final String partitionId = RuntimeIdGenerator.generatePartitionId(edgeId, srcTaskIndex, dstTaskIndex);
          master.initializeState(partitionId, Collections.singleton(srcTaskIndex),
              Collections.singleton(taskGroupPrefix + srcTaskIndex));
        });
      } else {
        final String partitionId = RuntimeIdGenerator.generatePartitionId(edgeId, srcTaskIndex);
        master.initializeState(partitionId, Collections.singleton(srcTaskIndex),
            Collections.singleton(taskGroupPrefix + srcTaskIndex));
      }
      master.onProducerTaskGroupScheduled(taskGroupPrefix + srcTaskIndex);
    });

    // Write
    final List<List<Element>> dataWrittenList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      final List<Element> dataWritten = getRangedNumList(0, PARALLELISM_TEN);
      final OutputWriter writer = new OutputWriter(HASH_RANGE_MULTIPLIER, srcTaskIndex, srcVertex.getId(), dstVertex,
          dummyEdge, sender);
      writer.write(dataWritten);
      dataWrittenList.add(dataWritten);
    });

    // Read
    final List<List<Element>> dataReadList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(dstTaskIndex -> {
      final InputReader reader =
          new InputReader(dstTaskIndex, taskGroupPrefix + dstTaskIndex, srcVertex, dummyEdge, receiver);
      final List<Element> dataRead = new ArrayList<>();
      try {
        InputReader.combineFutures(reader.read()).forEach(dataRead::add);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      dataReadList.add(dataRead);
    });

    // Compare (should be the same)
    final List<Element> flattenedWrittenData = flatten(dataWrittenList);
    final List<Element> flattenedReadData = flatten(dataReadList);
    if (commPattern.equals(Broadcast.class)) {
      final List<Element> broadcastedWrittenData = new ArrayList<>();
      IntStream.range(0, PARALLELISM_TEN).forEach(i -> broadcastedWrittenData.addAll(flattenedWrittenData));
      assertEquals(broadcastedWrittenData.size(), flattenedReadData.size());
      flattenedReadData.forEach(rData -> assertTrue(broadcastedWrittenData.remove(rData)));
    } else {
      assertEquals(flattenedWrittenData.size(), flattenedReadData.size());
      flattenedReadData.forEach(rData -> assertTrue(flattenedWrittenData.remove(rData)));
    }
  }

  /**
   * Tests I-File write and read between 10 writers - 10 readers.
   * 10 I-Files will be constructed by 10 writers concurrently.
   */
  private void iFileWriteAndRead(final PartitionManagerWorker sender,
                                 final PartitionManagerWorker receiver,
                                 final Class<? extends PartitionStore> store) throws RuntimeException {
    final int testIndex = TEST_INDEX.getAndIncrement();
    final String edgeId = String.format(EDGE_PREFIX_TEMPLATE, testIndex);
    final String taskGroupPrefix = String.format(TASKGROUP_PREFIX_TEMPLATE, testIndex);
    final Pair<IRVertex, IRVertex> verticesPair = setupVertices(edgeId, sender, receiver);
    final IRVertex srcVertex = verticesPair.left();
    final IRVertex dstVertex = verticesPair.right();

    // Edge setup
    final IREdge dummyIREdge = new IREdge(IREdge.Type.ScatterGather, srcVertex, dstVertex, CODER);
    final ExecutionPropertyMap edgeProperties = dummyIREdge.getExecutionProperties();
    edgeProperties.put(PartitioningProperty.of(Hash.class));
    edgeProperties.put(DataStoreProperty.of(store));
    edgeProperties.put(WriteOptimizationProperty.of(WriteOptimizationProperty.IFILE_WRITE));
    final RuntimeEdge<IRVertex> dummyEdge
        = new RuntimeEdge<>(edgeId, edgeProperties, srcVertex, dstVertex, CODER);

    // Initialize the states of the I-File partitions in Master.
    final Set<String> taskGroupIds = new HashSet<>();
    final Set<Integer> producerTaskIndices = new HashSet<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      taskGroupIds.add(taskGroupPrefix + srcTaskIndex);
      producerTaskIndices.add(srcTaskIndex);
    });
    IntStream.range(0, PARALLELISM_TEN).forEach(dstTaskIndex -> {
      final String partitionId = RuntimeIdGenerator.generatePartitionId(edgeId, dstTaskIndex);
      master.initializeState(partitionId, producerTaskIndices, taskGroupIds);
    });
    taskGroupIds.forEach(master::onProducerTaskGroupScheduled);

    // Write
    final List<List<Element>> dataWrittenList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      final List<Element> dataWritten = getRangedNumList(0, I_FILE_DATA_SIZE);
      final OutputWriter writer = new OutputWriter(HASH_RANGE_MULTIPLIER, srcTaskIndex, srcVertex.getId(), dstVertex,
          dummyEdge, sender);
      writer.write(dataWritten);
      dataWrittenList.add(dataWritten);
    });

    // Read
    final List<List<Element>> dataReadList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(dstTaskIndex -> {
      final InputReader reader =
          new InputReader(dstTaskIndex, taskGroupPrefix + dstTaskIndex, srcVertex, dummyEdge, receiver);
      final List<Element> dataRead = new ArrayList<>();
      try {
        InputReader.combineFutures(reader.read()).forEach(dataRead::add);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      dataReadList.add(dataRead);
    });

    // Compare (should be the same)
    final List<Element> flattenedWrittenData = flatten(dataWrittenList);
    final List<Element> flattenedReadData = flatten(dataReadList);
    System.out.println(flattenedWrittenData + "\n" + flattenedReadData);
    assertEquals(flattenedWrittenData.size(), flattenedReadData.size());
    flattenedReadData.forEach(rData -> assertTrue(flattenedWrittenData.remove(rData)));
  }

  private Pair<IRVertex, IRVertex> setupVertices(final String edgeId,
                                                 final PartitionManagerWorker sender,
                                                 final PartitionManagerWorker receiver) {
    sender.registerCoder(edgeId, CODER);
    receiver.registerCoder(edgeId, CODER);

    // Src setup
    final BoundedSource s = mock(BoundedSource.class);
    final BoundedSourceVertex srcVertex = new BoundedSourceVertex<>(s);
    final ExecutionPropertyMap srcVertexProperties = srcVertex.getExecutionProperties();
    srcVertexProperties.put(ParallelismProperty.of(PARALLELISM_TEN));

    // Dst setup
    final BoundedSourceVertex dstVertex = new BoundedSourceVertex<>(s);
    final ExecutionPropertyMap dstVertexProperties = dstVertex.getExecutionProperties();
    dstVertexProperties.put(ParallelismProperty.of(PARALLELISM_TEN));

    return Pair.of(srcVertex, dstVertex);
  }
}