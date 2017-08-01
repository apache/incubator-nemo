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
import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.frontend.beam.BeamElement;
import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.common.coder.BeamCoder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.compiler.ir.attribute.AttributeMap;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.vortex.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.vortex.runtime.common.message.ncs.NcsParameters;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.executor.Executor;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMaster;
import edu.snu.vortex.runtime.executor.data.PartitionManagerWorker;
import edu.snu.vortex.runtime.master.PartitionManagerMaster;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import edu.snu.vortex.runtime.master.resource.ContainerManager;
import edu.snu.vortex.runtime.master.scheduler.BatchScheduler;
import edu.snu.vortex.runtime.master.scheduler.PendingTaskGroupPriorityQueue;
import edu.snu.vortex.runtime.master.scheduler.RoundRobinSchedulingPolicy;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.values.KV;
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
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static edu.snu.vortex.common.dag.DAG.EMPTY_DAG_DIRECTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link InputReader} and {@link OutputWriter}.
 */
public final class DataTransferTest {
  private static final String EXECUTOR_ID_PREFIX = "Executor";
  private static final int EXECUTOR_CAPACITY = 1;
  private static final int MAX_SCHEDULE_ATTEMPT = 2;
  private static final int SCHEDULE_TIMEOUT = 1000;
  private static final Attribute STORE = Attribute.Memory;
  private static final Attribute FILE_STORE = Attribute.LocalFile;
  private static final String TMP_FILE_DIRECTORY = "./tmpFiles";
  private static final int PARALLELISM_TEN = 10;
  private static final String EDGE_PREFIX_TEMPLATE = "Dummy(%d)";
  private static final AtomicInteger TEST_INDEX = new AtomicInteger(0);
  private static final String TASKGROUP_PREFIX_TEMPLATE = "DummyTG(%d)_";
  private static final Coder CODER = new BeamCoder(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));
  private static final Tang TANG = Tang.Factory.getTang();
  private static final int HASH_RANGE_MULTIPLIER = 10;

  private PartitionManagerMaster master;
  private PartitionManagerWorker worker1;
  private PartitionManagerWorker worker2;

  @Before
  public void setUp() {
    final LocalMessageDispatcher messageDispatcher = new LocalMessageDispatcher();
    final LocalMessageEnvironment messageEnvironment =
        new LocalMessageEnvironment(MessageEnvironment.MASTER_COMMUNICATION_ID, messageDispatcher);
    final ContainerManager containerManager = new ContainerManager(null, messageEnvironment);
    final Scheduler scheduler =
        new BatchScheduler(master,
            new RoundRobinSchedulingPolicy(containerManager, SCHEDULE_TIMEOUT), new PendingTaskGroupPriorityQueue());
    final AtomicInteger executorCount = new AtomicInteger(0);
    final PartitionManagerMaster master = new PartitionManagerMaster();

    // Unused, but necessary for wiring up the message environments
    final RuntimeMaster runtimeMaster = new RuntimeMaster(scheduler, containerManager,
        messageEnvironment, master, EMPTY_DAG_DIRECTORY, MAX_SCHEDULE_ATTEMPT);

    final Injector injector = createNameClientInjector();

    this.master = master;
    this.worker1 = createWorker(EXECUTOR_ID_PREFIX + executorCount.getAndIncrement(), messageDispatcher,
        injector);
    this.worker2 = createWorker(EXECUTOR_ID_PREFIX + executorCount.getAndIncrement(), messageDispatcher,
        injector);
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
    injector.bindVolatileParameter(JobConf.FileDirectory.class, TMP_FILE_DIRECTORY);
    final PartitionManagerWorker partitionManagerWorker;
    try {
      partitionManagerWorker = injector.getInstance(PartitionManagerWorker.class);
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
        new DataTransferFactory(HASH_RANGE_MULTIPLIER, partitionManagerWorker));
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
    writeAndRead(worker1, worker1, Attribute.OneToOne, STORE);

    // test OneToOne different worker
    writeAndRead(worker1, worker2, Attribute.OneToOne, STORE);

    // test OneToMany same worker
    writeAndRead(worker1, worker1, Attribute.Broadcast, STORE);

    // test OneToMany different worker
    writeAndRead(worker1, worker2, Attribute.Broadcast, STORE);

    // test ManyToMany same worker
    writeAndRead(worker1, worker1, Attribute.ScatterGather, STORE);

    // test ManyToMany different worker
    writeAndRead(worker1, worker2, Attribute.ScatterGather, STORE);

    // test ManyToMany same worker (file)
    writeAndRead(worker1, worker1, Attribute.ScatterGather, FILE_STORE);

    // test ManyToMany different worker (file)
    writeAndRead(worker1, worker2, Attribute.ScatterGather, FILE_STORE);

    // Cleanup
    FileUtils.deleteDirectory(new File(TMP_FILE_DIRECTORY));
  }

  private void writeAndRead(final PartitionManagerWorker sender,
                            final PartitionManagerWorker receiver,
                            final Attribute commPattern,
                            final Attribute store) throws RuntimeException {
    final int testIndex = TEST_INDEX.getAndIncrement();
    final String edgeId = String.format(EDGE_PREFIX_TEMPLATE, testIndex);
    final String taskGroupPrefix = String.format(TASKGROUP_PREFIX_TEMPLATE, testIndex);
    sender.registerCoder(edgeId, CODER);
    receiver.registerCoder(edgeId, CODER);

    // Src setup
    final BoundedSource s = mock(BoundedSource.class);
    final BoundedSourceVertex srcVertex = new BoundedSourceVertex<>(s);
    final AttributeMap srcVertexAttributes = srcVertex.getAttributes();
    srcVertexAttributes.put(Attribute.IntegerKey.Parallelism, PARALLELISM_TEN);

    // Dst setup
    final BoundedSourceVertex dstVertex = new BoundedSourceVertex<>(s);
    final AttributeMap dstVertexAttributes = dstVertex.getAttributes();
    dstVertexAttributes.put(Attribute.IntegerKey.Parallelism, PARALLELISM_TEN);

    // Edge setup
    final IREdge dummyIREdge = new IREdge(IREdge.Type.OneToOne, srcVertex, dstVertex, CODER);
    final AttributeMap edgeAttributes = dummyIREdge.getAttributes();
    edgeAttributes.put(Attribute.Key.CommunicationPattern, commPattern);
    edgeAttributes.put(Attribute.Key.Partitioning, Attribute.Hash);
    edgeAttributes.put(Attribute.Key.ChannelDataPlacement, store);
    final RuntimeEdge<IRVertex> dummyEdge
        = new RuntimeEdge<>(edgeId, edgeAttributes, srcVertex, dstVertex, CODER);

    // Initialize states in Master
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      if (commPattern == Attribute.ScatterGather) {
        IntStream.range(0, PARALLELISM_TEN).forEach(dstTaskIndex -> {
          master.initializeState(edgeId, srcTaskIndex, dstTaskIndex, taskGroupPrefix + srcTaskIndex);
        });
      } else {
        master.initializeState(edgeId, srcTaskIndex, taskGroupPrefix + srcTaskIndex);
      }
      master.onProducerTaskGroupScheduled(taskGroupPrefix + srcTaskIndex);
    });

    // Write
    final List<List<Element>> dataWrittenList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      final List<Element> dataWritten = getListOfZeroToNine();
      final OutputWriter writer = new OutputWriter(HASH_RANGE_MULTIPLIER, srcTaskIndex, srcVertex.getId(), dstVertex,
          dummyEdge, sender);
      writer.write(dataWritten);
      dataWrittenList.add(dataWritten);
    });

    // Read
    final List<List<Element>> dataReadList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(dstTaskIndex -> {
      final InputReader reader = new InputReader(dstTaskIndex, srcVertex, dummyEdge, receiver);
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
    if (commPattern == Attribute.Broadcast) {
      final List<Element> broadcastedWrittenData = new ArrayList<>();
      IntStream.range(0, PARALLELISM_TEN).forEach(i -> broadcastedWrittenData.addAll(flattenedWrittenData));
      assertEquals(broadcastedWrittenData.size(), flattenedReadData.size());
      flattenedReadData.forEach(rData -> assertTrue(broadcastedWrittenData.remove(rData)));
    } else {
      assertEquals(flattenedWrittenData.size(), flattenedReadData.size());
      flattenedReadData.forEach(rData -> assertTrue(flattenedWrittenData.remove(rData)));
    }
  }

  private List<Element> getListOfZeroToNine() {
    final List<Element> dummy = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(number -> dummy.add(new BeamElement<>(KV.of(number, number))));
    return dummy;
  }

  private List<Element> flatten(final List<List<Element>> listOfList) {
    return listOfList.stream().flatMap(list -> list.stream()).collect(Collectors.toList());
  }
}