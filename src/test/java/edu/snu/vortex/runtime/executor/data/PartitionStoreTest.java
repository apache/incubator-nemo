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
package edu.snu.vortex.runtime.executor.data;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.common.coder.BeamCoder;
import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.frontend.beam.BeamElement;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.vortex.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.vortex.runtime.common.state.PartitionState;
import edu.snu.vortex.runtime.master.PartitionManagerMaster;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.FileUtils;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static edu.snu.vortex.runtime.RuntimeTestUtil.flatten;
import static edu.snu.vortex.runtime.RuntimeTestUtil.getRangedNumList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests write and read for {@link PartitionStore}s.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PartitionManagerWorker.class, PartitionManagerMaster.class, RuntimeMaster.class})
public final class PartitionStoreTest {
  private static final String TMP_FILE_DIRECTORY = "./tmpFiles";
  private static final Coder CODER = new BeamCoder(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));
  private PartitionManagerMaster partitionManagerMaster;
  private LocalMessageDispatcher messageDispatcher;
  // Variables for scatter and gather test
  private static final int NUM_WRITE_TASKS = 3;
  private static final int NUM_READ_TASKS = 3;
  private static final int DATA_SIZE = 1000;
  private List<String> partitionIdList;
  private List<Block> partitionBlockList;
  // Variables for concurrent read test
  private static final int NUM_CONC_READ_TASKS = 10;
  private static final int CONC_READ_DATA_SIZE = 1000;
  private String concPartitionId;
  private Block concPartitionBlock;
  // Variables for scatter and gather in range test
  private static final int NUM_WRITE_HASH_TASKS = 2;
  private static final int NUM_READ_HASH_TASKS = 3;
  private static final int HASH_DATA_SIZE = 1000;
  private static final int HASH_RANGE = 4;
  private List<String> hashedPartitionIdList;
  private List<List<Block>> hashedPartitionBlockList;
  private List<HashRange> readHashRangeList;
  private List<List<Iterable<Element>>> expectedDataInRange;
  // Variables for concurrent write test
  private static final int NUM_CONC_WRITE_TASKS = 2;
  private static final int CONC_WRITE_DATA_IN_BLOCK = 100;
  private static final int CONC_WRITE_BLOCK_NUM = 10;
  private static List<Element> concWriteBlocks;
  private static String concWritePartitionId;

  /**
   * Generates the ids and the data which will be used for the partition store tests.
   */
  @Before
  public void setUp() throws Exception {
    messageDispatcher = new LocalMessageDispatcher();
    final LocalMessageEnvironment messageEnvironment =
        new LocalMessageEnvironment(MessageEnvironment.MASTER_COMMUNICATION_ID, messageDispatcher);
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(MessageEnvironment.class, messageEnvironment);
    partitionManagerMaster = injector.getInstance(PartitionManagerMaster.class);
    // Following part is for for the scatter and gather test.
    final int numPartitions = NUM_WRITE_TASKS * NUM_READ_TASKS;
    final List<String> writeTaskIdList = new ArrayList<>(NUM_WRITE_TASKS);
    final List<String> readTaskIdList = new ArrayList<>(NUM_READ_TASKS);
    partitionIdList = new ArrayList<>(numPartitions);
    partitionBlockList = new ArrayList<>(numPartitions);

    // Generates the ids of the tasks to be used.
    IntStream.range(0, NUM_WRITE_TASKS).forEach(
        number -> writeTaskIdList.add(RuntimeIdGenerator.generateTaskId()));
    IntStream.range(0, NUM_READ_TASKS).forEach(
        number -> readTaskIdList.add(RuntimeIdGenerator.generateTaskId()));

    // Generates the ids and the data of the partitions to be used.
    IntStream.range(0, NUM_WRITE_TASKS).forEach(writeTaskCount ->
        IntStream.range(0, NUM_READ_TASKS).forEach(readTaskCount -> {
          final int currentNum = partitionIdList.size();
          final String partitionId = RuntimeIdGenerator.generatePartitionId(
              RuntimeIdGenerator.generateRuntimeEdgeId(String.valueOf(currentNum)), writeTaskCount, readTaskCount);
          partitionIdList.add(partitionId);
          partitionManagerMaster.initializeState(partitionId, Collections.singleton(writeTaskCount),
              Collections.singleton("Unused"));
          partitionManagerMaster.onPartitionStateChanged(
              partitionId, PartitionState.State.SCHEDULED, null, null);
          partitionBlockList.add(new Block(getRangedNumList(currentNum * DATA_SIZE, (currentNum + 1) * DATA_SIZE)));
        }));

    // Following part is for the concurrent read test.
    final String writeTaskId = RuntimeIdGenerator.generateTaskId();
    final List<String> concReadTaskIdList = new ArrayList<>(NUM_CONC_READ_TASKS);

    // Generates the ids and the data to be used.
    concPartitionId = RuntimeIdGenerator.generatePartitionId(
        RuntimeIdGenerator.generateRuntimeEdgeId("concurrent read"), NUM_WRITE_TASKS + NUM_READ_TASKS + 1);
    partitionManagerMaster.initializeState(concPartitionId, Collections.singleton(0),
        Collections.singleton("Unused"));
    partitionManagerMaster.onPartitionStateChanged(
        concPartitionId, PartitionState.State.SCHEDULED, null, null);
    IntStream.range(0, NUM_CONC_READ_TASKS).forEach(
        number -> concReadTaskIdList.add(RuntimeIdGenerator.generateTaskId()));
    concPartitionBlock = new Block(getRangedNumList(0, CONC_READ_DATA_SIZE));

    // Following part is for the scatter and gather in hash range test
    final int numHashedPartitions = NUM_WRITE_HASH_TASKS;
    final List<String> writeHashTaskIdList = new ArrayList<>(NUM_WRITE_HASH_TASKS);
    final List<String> readHashTaskIdList = new ArrayList<>(NUM_READ_HASH_TASKS);
    readHashRangeList = new ArrayList<>(NUM_READ_HASH_TASKS);
    hashedPartitionIdList = new ArrayList<>(numHashedPartitions);
    hashedPartitionBlockList = new ArrayList<>(numHashedPartitions);
    expectedDataInRange = new ArrayList<>(NUM_READ_HASH_TASKS);

    // Generates the ids of the tasks to be used.
    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(
        number -> writeHashTaskIdList.add(RuntimeIdGenerator.generateTaskId()));
    IntStream.range(0, NUM_READ_HASH_TASKS).forEach(
        number -> readHashTaskIdList.add(RuntimeIdGenerator.generateTaskId()));

    // Generates the ids and the data of the partitions to be used.
    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writeTaskCount -> {
      final String partitionId = RuntimeIdGenerator.generatePartitionId(
          RuntimeIdGenerator.generateRuntimeEdgeId("scatter gather in range"),
          NUM_WRITE_TASKS + NUM_READ_TASKS + 1 + writeTaskCount);
      hashedPartitionIdList.add(partitionId);
      partitionManagerMaster.initializeState(partitionId, Collections.singleton(writeTaskCount),
          Collections.singleton("Unused"));
      partitionManagerMaster.onPartitionStateChanged(
          partitionId, PartitionState.State.SCHEDULED, null, null);
      final List<Block> hashedPartition = new ArrayList<>(HASH_RANGE);
      // Generates the data having each hash value.
      IntStream.range(0, HASH_RANGE).forEach(hashValue ->
        hashedPartition.add(new Block(hashValue, getFixedKeyRangedNumList(
            hashValue,
            writeTaskCount * HASH_DATA_SIZE * HASH_RANGE + hashValue * HASH_DATA_SIZE,
            writeTaskCount * HASH_DATA_SIZE * HASH_RANGE + (hashValue + 1) * HASH_DATA_SIZE))));
      hashedPartitionBlockList.add(hashedPartition);
    });

    // Generates the range of hash value to read for each read task.
    final int smallDataRangeEnd = 1 + NUM_READ_HASH_TASKS - NUM_WRITE_HASH_TASKS;
    readHashRangeList.add(HashRange.of(0, smallDataRangeEnd));
    IntStream.range(0, NUM_READ_HASH_TASKS - 1).forEach(readTaskNumber -> {
      readHashRangeList.add(HashRange.of(smallDataRangeEnd + readTaskNumber, smallDataRangeEnd + readTaskNumber + 1));
    });

    // Generates the expected result of hash range retrieval for each read task.
    IntStream.range(0, NUM_READ_HASH_TASKS).forEach(readTaskNumber -> {
      final HashRange hashRange = readHashRangeList.get(readTaskNumber);
      final List<Iterable<Element>> expectedRangeBlocks = new ArrayList<>(NUM_WRITE_HASH_TASKS);
      IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writeTaskNumber -> {
        final List<Iterable<Element>> appendingList = new ArrayList<>();
        IntStream.range(hashRange.rangeStartInclusive(), hashRange.rangeEndExclusive()).forEach(hashVal ->
            appendingList.add(hashedPartitionBlockList.get(writeTaskNumber).get(hashVal).getData()));
        final List<Element> concatStreamBase = new ArrayList<>();
        Stream<Element> concatStream = concatStreamBase.stream();
        for (final Iterable<Element> data : appendingList) {
          concatStream = Stream.concat(concatStream, StreamSupport.stream(data.spliterator(), false));
        }
        expectedRangeBlocks.add(concatStream.collect(Collectors.toList()));
      });
      expectedDataInRange.add(expectedRangeBlocks);
    });

    // Following part is for the concurrent write test
    concWriteBlocks = getRangedNumList(0, CONC_WRITE_DATA_IN_BLOCK);
    concWritePartitionId = RuntimeIdGenerator.generatePartitionId("Concurrent write test edge", 0);
    partitionManagerMaster.initializeState(concWritePartitionId, Collections.singleton(NUM_CONC_WRITE_TASKS),
        Collections.singleton("Unused"));
    partitionManagerMaster.onPartitionStateChanged(
        concWritePartitionId, PartitionState.State.SCHEDULED, null, null);
  }

  /**
   * Test {@link MemoryStore}.
   */
  @Test(timeout = 10000)
  public void testMemoryStore() throws Exception {
    final PartitionStore memoryStore = Tang.Factory.getTang().newInjector().getInstance(MemoryStore.class);
    scatterGather(memoryStore, memoryStore);
    concurrentRead(memoryStore, memoryStore);
    scatterGatherInHashRange(memoryStore, memoryStore);
    concurrentWrite(memoryStore, memoryStore);
  }

  /**
   * Test {@link LocalFileStore}.
   */
  @Test(timeout = 10000)
  public void testLocalFileStore() throws Exception {
    final PartitionManagerWorker worker = mock(PartitionManagerWorker.class);
    when(worker.getCoder(any())).thenReturn(CODER);
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.FileDirectory.class, TMP_FILE_DIRECTORY);
    injector.bindVolatileInstance(PartitionManagerWorker.class, worker);

    final PartitionStore localFileStore = injector.getInstance(LocalFileStore.class);
    scatterGather(localFileStore, localFileStore);
    concurrentRead(localFileStore, localFileStore);
    scatterGatherInHashRange(localFileStore, localFileStore);
    concurrentWrite(localFileStore, localFileStore);
    FileUtils.deleteDirectory(new File(TMP_FILE_DIRECTORY));
  }

  /**
   * Test {@link GlusterFileStore}.
   * Actually, we cannot create a virtual GFS volume in here.
   * Instead, this test mimics the GFS circumstances by doing the read and write on separate file stores.
   */
  @Test(timeout = 10000)
  public void testGlusterFileStore() throws Exception {
    final PartitionManagerWorker pmw = mock(PartitionManagerWorker.class);
    when(pmw.getCoder(any())).thenReturn(CODER);

    // Mimic the metadata server with local message handler.
    /*final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(PartitionManagerMaster.class, partitionManagerMaster);
    final LocalMessageEnvironment metaserverMessageEnvironment =
        new LocalMessageEnvironment(MessageEnvironment.MASTER_COMMUNICATION_ID, messageDispatcher);
    metaserverMessageEnvironment.setupListener(
        MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID,
        new LocalMetadataServerMessageReceiver(partitionManagerMaster));*/

    final RemoteFileStore writerSideRemoteFileStore =
        createGlusterFileStore("writer", pmw);
    final RemoteFileStore readerSideRemoteFileStore =
        createGlusterFileStore("reader", pmw);

    scatterGather(writerSideRemoteFileStore, readerSideRemoteFileStore);
    concurrentRead(writerSideRemoteFileStore, readerSideRemoteFileStore);
    scatterGatherInHashRange(writerSideRemoteFileStore, readerSideRemoteFileStore);
    concurrentWrite(writerSideRemoteFileStore, readerSideRemoteFileStore);
    FileUtils.deleteDirectory(new File(TMP_FILE_DIRECTORY));
  }

  private GlusterFileStore createGlusterFileStore(final String executorId,
                                                  final PartitionManagerWorker worker)
      throws InjectionException {
    final LocalMessageEnvironment localMessageEnvironment =
        new LocalMessageEnvironment(executorId, messageDispatcher);
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.GlusterVolumeDirectory.class, TMP_FILE_DIRECTORY);
    injector.bindVolatileParameter(JobConf.JobId.class, "GFS test");
    injector.bindVolatileParameter(JobConf.ExecutorId.class, executorId);
    injector.bindVolatileInstance(PartitionManagerWorker.class, worker);
    injector.bindVolatileInstance(MessageEnvironment.class, localMessageEnvironment);
    return injector.getInstance(GlusterFileStore.class);
  }

  /**
   * Tests scatter and gather for {@link PartitionStore}s.
   * Assumes following circumstances:
   * Task 1 (write)->         (read)-> Task 4
   * Task 2 (write)-> shuffle (read)-> Task 5
   * Task 3 (write)->         (read)-> Task 6
   * It checks that each writer and reader does not throw any exception
   * and the read data is identical with written data (including the order).
   */
  private void scatterGather(final PartitionStore writerSideStore,
                             final PartitionStore readerSideStore) {
    final ExecutorService writeExecutor = Executors.newFixedThreadPool(NUM_WRITE_TASKS);
    final ExecutorService readExecutor = Executors.newFixedThreadPool(NUM_READ_TASKS);
    final List<Future<Boolean>> writeFutureList = new ArrayList<>(NUM_WRITE_TASKS);
    final List<Future<Boolean>> readFutureList = new ArrayList<>(NUM_READ_TASKS);
    final long startNano = System.nanoTime();

    // Write concurrently
    IntStream.range(0, NUM_WRITE_TASKS).forEach(writeTaskCount ->
        writeFutureList.add(writeExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              IntStream.range(writeTaskCount * NUM_READ_TASKS, (writeTaskCount + 1) * NUM_READ_TASKS).forEach(
                  partitionNumber -> {
                    final String partitionId = partitionIdList.get(partitionNumber);
                    writerSideStore.putToPartition(partitionId,
                        Collections.singleton(partitionBlockList.get(partitionNumber)), false);
                    writerSideStore.commitPartition(partitionId);
                    partitionManagerMaster.onPartitionStateChanged(partitionId, PartitionState.State.COMMITTED,
                        "Writer side of the scatter gather edge", writeTaskCount);
                  });
              return true;
            } catch (final Exception e) {
              e.printStackTrace();
              return false;
            }
          }
        })));

    // Wait each writer to success
    IntStream.range(0, NUM_WRITE_TASKS).forEach(writer -> {
      try {
        assertTrue(writeFutureList.get(writer).get());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
    final long writeEndNano = System.nanoTime();

    // Read concurrently and check whether the result is equal to the input
    IntStream.range(0, NUM_READ_TASKS).forEach(readTaskCount ->
        readFutureList.add(readExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              IntStream.range(0, NUM_WRITE_TASKS).forEach(
                  writeTaskNumber -> {
                    final int partitionNumber = writeTaskNumber * NUM_READ_TASKS + readTaskCount;
                    final Optional<Iterable<Element>> optionalData =
                        readerSideStore.getFromPartition(partitionIdList.get(partitionNumber), HashRange.all());
                    if (!optionalData.isPresent()) {
                      throw new RuntimeException("The result of retrieveData(" +
                          partitionIdList.get(partitionNumber) + ") is empty");
                    }
                    assertEquals(partitionBlockList.get(partitionNumber).getData(), optionalData.get());

                    final boolean exist = readerSideStore.removePartition(partitionIdList.get(partitionNumber));
                    if (!exist) {
                      throw new RuntimeException("The result of removePartition(" +
                          partitionIdList.get(partitionNumber) + ") is false");
                    }
                  });
              return true;
            } catch (final Exception e) {
              e.printStackTrace();
              return false;
            }
          }
        })));

    // Wait each reader to success
    IntStream.range(0, NUM_READ_TASKS).forEach(reader -> {
      try {
        assertTrue(readFutureList.get(reader).get());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
    final long readEndNano = System.nanoTime();

    writeExecutor.shutdown();
    readExecutor.shutdown();

    System.out.println(
        "Scatter and gather - write time in millis: " + (writeEndNano - startNano) / 1000000 +
            ", Read time in millis: " + (readEndNano - writeEndNano) / 1000000 + " in store " +
            writerSideStore.getClass().toString());
  }

  /**
   * Tests concurrent read for {@link PartitionStore}s.
   * Assumes following circumstances:
   *                                             -> Task 2
   * Task 1 (write)-> broadcast (concurrent read)-> ...
   *                                             -> Task 11
   * It checks that each writer and reader does not throw any exception
   * and the read data is identical with written data (including the order).
   */
  private void concurrentRead(final PartitionStore writerSideStore,
                              final PartitionStore readerSideStore) {
    final ExecutorService writeExecutor = Executors.newSingleThreadExecutor();
    final ExecutorService readExecutor = Executors.newFixedThreadPool(NUM_CONC_READ_TASKS);
    final Future<Boolean> writeFuture;
    final List<Future<Boolean>> readFutureList = new ArrayList<>(NUM_CONC_READ_TASKS);
    final long startNano = System.nanoTime();

    // Write a partition
    writeFuture = writeExecutor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        try {
          writerSideStore.putToPartition(concPartitionId, Collections.singleton(concPartitionBlock), false);
          writerSideStore.commitPartition(concPartitionId);
          partitionManagerMaster.onPartitionStateChanged(
              concPartitionId, PartitionState.State.COMMITTED, "Writer side of the concurrent read edge", 0);
          return true;
        } catch (final Exception e) {
          e.printStackTrace();
          return false;
        }
      }
    });

    // Wait the writer to success
    try {
      assertTrue(writeFuture.get());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    final long writeEndNano = System.nanoTime();

    // Read the single partition concurrently and check whether the result is equal to the input
    IntStream.range(0, NUM_CONC_READ_TASKS).forEach(readTaskCount ->
        readFutureList.add(readExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              final Optional<Iterable<Element>> optionalData =
                  readerSideStore.getFromPartition(concPartitionId, HashRange.all());
              if (!optionalData.isPresent()) {
                throw new RuntimeException("The result of retrieveData(" +
                    concPartitionId + ") is empty");
              }
              assertEquals(concPartitionBlock.getData(), optionalData.get());
              return true;
            } catch (final Exception e) {
              e.printStackTrace();
              return false;
            }
          }
        })));

    // Wait each reader to success
    IntStream.range(0, NUM_CONC_READ_TASKS).forEach(reader -> {
      try {
        assertTrue(readFutureList.get(reader).get());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Remove the partition
    final boolean exist = writerSideStore.removePartition(concPartitionId);
    if (!exist) {
      throw new RuntimeException("The result of removePartition(" + concPartitionId + ") is false");
    }
    final long readEndNano = System.nanoTime();

    writeExecutor.shutdown();
    readExecutor.shutdown();

    System.out.println(
        "Concurrent read - write time in millis: " + (writeEndNano - startNano) / 1000000 +
            ", Read time in millis: " + (readEndNano - writeEndNano) / 1000000 + " in store " +
            writerSideStore.getClass().toString());
  }

  /**
   * Tests scatter and gather in hash range for {@link PartitionStore}s.
   * Assumes following circumstances:
   * Task 1 (write (hash 0~3))->         (read (hash 0~1))-> Task 3
   * Task 2 (write (hash 0~3))-> shuffle (read (hash 2))-> Task 4
   *                                     (read (hash 3))-> Task 5
   * It checks that each writer and reader does not throw any exception
   * and the read data is identical with written data (including the order).
   */
  private void scatterGatherInHashRange(final PartitionStore writerSideStore,
                                        final PartitionStore readerSideStore) {
    final ExecutorService writeExecutor = Executors.newFixedThreadPool(NUM_WRITE_HASH_TASKS);
    final ExecutorService readExecutor = Executors.newFixedThreadPool(NUM_READ_HASH_TASKS);
    final List<Future<Boolean>> writeFutureList = new ArrayList<>(NUM_WRITE_HASH_TASKS);
    final List<Future<Boolean>> readFutureList = new ArrayList<>(NUM_READ_HASH_TASKS);
    final long startNano = System.nanoTime();

    // Write concurrently
    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writeTaskCount ->
        writeFutureList.add(writeExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              final String partitionId = hashedPartitionIdList.get(writeTaskCount);
              writerSideStore.putToPartition(partitionId,
                  hashedPartitionBlockList.get(writeTaskCount), false);
              writerSideStore.commitPartition(partitionId);
              partitionManagerMaster.onPartitionStateChanged(partitionId, PartitionState.State.COMMITTED,
                  "Writer side of the scatter gather in hash range edge", writeTaskCount);
              return true;
            } catch (final Exception e) {
              e.printStackTrace();
              return false;
            }
          }
        })));

    // Wait each writer to success
    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writer -> {
      try {
        assertTrue(writeFutureList.get(writer).get());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
    final long writeEndNano = System.nanoTime();

    // Read concurrently and check whether the result is equal to the expected data
    IntStream.range(0, NUM_READ_HASH_TASKS).forEach(readTaskCount ->
        readFutureList.add(readExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(
                  writeTaskNumber -> {
                    final HashRange hashRangeToRetrieve = readHashRangeList.get(readTaskCount);
                    final Optional<Iterable<Element>> optionalData = readerSideStore.getFromPartition(
                        hashedPartitionIdList.get(writeTaskNumber), hashRangeToRetrieve);
                    if (!optionalData.isPresent()) {
                      throw new RuntimeException("The result of get partition" +
                          hashedPartitionIdList.get(writeTaskNumber) + " in range " + hashRangeToRetrieve.toString() +
                          " is empty");
                    }
                    assertEquals(
                        expectedDataInRange.get(readTaskCount).get(writeTaskNumber), optionalData.get());
                  });
              return true;
            } catch (final Exception e) {
              e.printStackTrace();
              return false;
            }
          }
        })));

    // Wait each reader to success
    IntStream.range(0, NUM_READ_HASH_TASKS).forEach(reader -> {
      try {
        assertTrue(readFutureList.get(reader).get());
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
    final long readEndNano = System.nanoTime();

    // Remove stored partitions
    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writer -> {
      final boolean exist = writerSideStore.removePartition(hashedPartitionIdList.get(writer));
      if (!exist) {
        throw new RuntimeException("The result of removePartition(" +
            hashedPartitionIdList.get(writer) + ") is false");
      }
    });

    writeExecutor.shutdown();
    readExecutor.shutdown();

    System.out.println(
        "Scatter and gather in hash range - write time in millis: " + (writeEndNano - startNano) / 1000000 +
            ", Read time in millis: " + (readEndNano - writeEndNano) / 1000000 + " in store " +
            writerSideStore.getClass().toString());
  }

  /**
   * Tests concurrent write for a store.
   * Assumes following circumstances:
   * Task 1 (write)-> partition (read)-> Task 3
   * Task 2 (write)->
   * It checks that each writer and reader does not throw any exception,
   * the read data is identical with written data, and the written data blocks are consistent.
   */
  private void concurrentWrite(final PartitionStore writerSideStore,
                               final PartitionStore readerSideStore) {
    final ExecutorService writeExecutor = Executors.newFixedThreadPool(NUM_CONC_WRITE_TASKS);
    final ExecutorService readExecutor = Executors.newFixedThreadPool(1);
    final List<Future<Boolean>> writeFutureList = new ArrayList<>(NUM_CONC_WRITE_TASKS);
    final long startNano = System.nanoTime();

    // Write concurrently.
    IntStream.range(0, NUM_CONC_WRITE_TASKS).forEach(writeTaskCount ->
        writeFutureList.add(writeExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              final List<Block> blockToAppend = new ArrayList<>(CONC_WRITE_BLOCK_NUM);
              IntStream.range(0, CONC_WRITE_BLOCK_NUM).forEach(blockIdx ->
                  blockToAppend.add(new Block(blockIdx, concWriteBlocks)));
              writerSideStore.putToPartition(concWritePartitionId, blockToAppend, false);
              partitionManagerMaster.onPartitionStateChanged(concWritePartitionId, PartitionState.State.COMMITTED,
                  "Writer side of the concurrent write edge", writeTaskCount);
              return true;
            } catch (final Exception e) {
              e.printStackTrace();
              return false;
            }
          }
        })));

    // Wait each writer to success.
    IntStream.range(0, NUM_CONC_WRITE_TASKS).forEach(writer -> {
      try {
        assertTrue(writeFutureList.get(writer).get());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
    final long writeEndNano = System.nanoTime();
    writerSideStore.commitPartition(concWritePartitionId);

    // Read and check whether the result is proper.
    final Future<Boolean> readFuture = readExecutor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        try {
          final Optional<Iterable<Element>> optionalData =
              readerSideStore.getFromPartition(concWritePartitionId, HashRange.all());
          if (!optionalData.isPresent()) {
            throw new RuntimeException("The result of retrieveData(" + concWritePartitionId + ") is empty");
          }
          final List<List<Element>> expectedResults =
              new ArrayList<>(CONC_WRITE_BLOCK_NUM * NUM_CONC_WRITE_TASKS);
          IntStream.range(0, CONC_WRITE_BLOCK_NUM * NUM_CONC_WRITE_TASKS).
              forEach(blockIdx -> expectedResults.add(concWriteBlocks));
          assertEquals(flatten(expectedResults), optionalData.get());

          final boolean exist = readerSideStore.removePartition(concWritePartitionId);
          if (!exist) {
            throw new RuntimeException("The result of removePartition(" + concWritePartitionId + ") is false");
          }

          return true;
        } catch (final Exception e) {
          e.printStackTrace();
          return false;
        }
      }
    });

    // Wait each reader to success
    try {
      assertTrue(readFuture.get());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    final long readEndNano = System.nanoTime();

    writeExecutor.shutdown();
    readExecutor.shutdown();

    System.out.println(
        "Concurrent write - write time in millis: " + (writeEndNano - startNano) / 1000000 +
            ", Read time in millis: " + (readEndNano - writeEndNano) / 1000000 + " in store " +
            writerSideStore.getClass().toString());
  }

  private List<Element> getFixedKeyRangedNumList(final int key,
                                                 final int start,
                                                 final int end) {
    final List<Element> numList = new ArrayList<>(end - start);
    IntStream.range(start, end).forEach(number -> numList.add(new BeamElement<>(KV.of(key, number))));
    return numList;
  }
}
