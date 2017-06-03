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
import edu.snu.vortex.compiler.frontend.Coder;
import edu.snu.vortex.compiler.frontend.beam.BeamElement;
import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.frontend.beam.coder.BeamCoder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeAttributeMap;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.vortex.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.vortex.runtime.common.message.ncs.NcsParameters;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeBoundedSourceVertex;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex;
import edu.snu.vortex.runtime.executor.Executor;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMaster;
import edu.snu.vortex.runtime.executor.block.BlockManagerWorker;
import edu.snu.vortex.runtime.master.BlockManagerMaster;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import edu.snu.vortex.runtime.master.resource.ContainerManager;
import edu.snu.vortex.runtime.master.scheduler.BatchScheduler;
import edu.snu.vortex.runtime.master.scheduler.PendingTaskGroupQueue;
import edu.snu.vortex.runtime.master.scheduler.RoundRobinSchedulingPolicy;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;
import edu.snu.vortex.utils.dag.DAG;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.values.KV;
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

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static edu.snu.vortex.utils.dag.DAG.EMPTY_DAG_DIRECTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link InputReader} and {@link OutputWriter}.
 */
public final class DataTransferTest {
  private static final String EXECUTOR_ID_PREFIX = "Executor";
  private static final int EXECUTOR_CAPACITY = 1;
  private static final int SCHEDULE_TIMEOUT = 1000;
  private static final RuntimeAttribute STORE = RuntimeAttribute.Local;
  private static final int PARALLELISM_TEN = 10;
  private static final String EDGE_ID = "Dummy";
  private static final Coder CODER = new BeamCoder(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));
  private static final Tang TANG = Tang.Factory.getTang();

  private BlockManagerMaster master;
  private BlockManagerWorker worker1;
  private BlockManagerWorker worker2;

  @Before
  public void setUp() {
    final LocalMessageDispatcher messageDispatcher = new LocalMessageDispatcher();
    final LocalMessageEnvironment messageEnvironment =
        new LocalMessageEnvironment(MessageEnvironment.MASTER_COMMUNICATION_ID, messageDispatcher);
    final ContainerManager containerManager = new ContainerManager(null, messageEnvironment);
    final Scheduler scheduler =
        new BatchScheduler(
            new RoundRobinSchedulingPolicy(containerManager, SCHEDULE_TIMEOUT), new PendingTaskGroupQueue());
    final AtomicInteger executorCount = new AtomicInteger(0);
    final BlockManagerMaster master = new BlockManagerMaster();

    // Unused, but necessary for wiring up the message environments
    final RuntimeMaster runtimeMaster = new RuntimeMaster(scheduler, containerManager,
        messageEnvironment, master, EMPTY_DAG_DIRECTORY);

    final Injector injector = createNameClientInjector();

    this.master = master;
    this.worker1 = createWorker(EXECUTOR_ID_PREFIX + executorCount.getAndIncrement(), messageDispatcher,
        injector);
    this.worker2 = createWorker(EXECUTOR_ID_PREFIX + executorCount.getAndIncrement(), messageDispatcher,
        injector);
  }

  private BlockManagerWorker createWorker(final String executorId, final LocalMessageDispatcher messageDispatcher,
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
    final BlockManagerWorker blockManagerWorker;
    try {
      blockManagerWorker = injector.getInstance(BlockManagerWorker.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
    blockManagerWorker.registerCoder(EDGE_ID, CODER);

    // Unused, but necessary for wiring up the message environments
    final Executor executor = new Executor(
        executorId,
        EXECUTOR_CAPACITY,
        conToMaster,
        messageEnvironment,
        blockManagerWorker,
        new DataTransferFactory(blockManagerWorker));
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
  public void testOneToOneSameWorker() {
    writeAndRead(worker1, worker1, RuntimeAttribute.OneToOne);
  }

  @Test
  public void testOneToOneDifferentWorker() {
    writeAndRead(worker1, worker2, RuntimeAttribute.OneToOne);
  }

  @Test
  public void testOneToManySameWorker() {
    writeAndRead(worker1, worker1, RuntimeAttribute.Broadcast);
  }

  @Test
  public void testOneToManyDifferentWorker() {
    writeAndRead(worker1, worker2, RuntimeAttribute.Broadcast);
  }

  @Test
  public void testManyToManySameWorker() {
    writeAndRead(worker1, worker1, RuntimeAttribute.ScatterGather);
  }

  @Test
  public void testManyToManyDifferentWorker() {
    writeAndRead(worker1, worker2, RuntimeAttribute.ScatterGather);
  }

  private void writeAndRead(final BlockManagerWorker sender,
                            final BlockManagerWorker receiver,
                            final RuntimeAttribute commPattern) {
    // Src setup
    final RuntimeAttributeMap srcVertexAttributes = new RuntimeAttributeMap();
    srcVertexAttributes.put(RuntimeAttribute.IntegerKey.Parallelism, PARALLELISM_TEN);

    final BoundedSource s = mock(BoundedSource.class);
    final BoundedSourceVertex v1 = new BoundedSourceVertex<>(s);
    final RuntimeVertex srcVertex = new RuntimeBoundedSourceVertex(v1, srcVertexAttributes);

    // Dst setup
    final RuntimeAttributeMap dstVertexAttributes = new RuntimeAttributeMap();
    dstVertexAttributes.put(RuntimeAttribute.IntegerKey.Parallelism, PARALLELISM_TEN);
    final BoundedSourceVertex v2 = new BoundedSourceVertex<>(s);
    final RuntimeVertex dstVertex = new RuntimeBoundedSourceVertex(v2, dstVertexAttributes);

    // Edge setup
    final RuntimeAttributeMap edgeAttributes = new RuntimeAttributeMap();
    edgeAttributes.put(RuntimeAttribute.Key.CommPattern, commPattern);
    edgeAttributes.put(RuntimeAttribute.Key.Partition, RuntimeAttribute.Hash);
    edgeAttributes.put(RuntimeAttribute.Key.BlockStore, STORE);
    final RuntimeEdge<RuntimeVertex> dummyEdge
        = new RuntimeEdge<>(EDGE_ID, edgeAttributes, srcVertex, dstVertex, CODER);

    // Initialize states in Master
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      if (commPattern == RuntimeAttribute.ScatterGather) {
        IntStream.range(0, PARALLELISM_TEN).forEach(dstTaskIndex ->
            master.initializeState(EDGE_ID, srcTaskIndex, dstTaskIndex));
      } else {
        master.initializeState(EDGE_ID, srcTaskIndex);
      }
    });

    // Write
    final List<List<Element>> dataWrittenList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(srcTaskIndex -> {
      final List<Element> dataWritten = getListOfZeroToNine();
      final OutputWriter writer = new OutputWriter(srcTaskIndex, dstVertex, dummyEdge, sender);
      writer.write(dataWritten);
      dataWrittenList.add(dataWritten);
    });

    // Read
    final List<List<Element>> dataReadList = new ArrayList<>();
    IntStream.range(0, PARALLELISM_TEN).forEach(dstTaskIndex -> {
      final InputReader reader = new InputReader(dstTaskIndex, srcVertex, dummyEdge, receiver);
      final List<Element> dataRead = new ArrayList<>();
      reader.read().forEach(dataRead::add);
      dataReadList.add(dataRead);
    });

    // Compare (should be the same)
    final List<Element> flattenedWrittenData = flatten(dataWrittenList);
    final List<Element> flattenedReadData = flatten(dataReadList);
    if (commPattern == RuntimeAttribute.Broadcast) {
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