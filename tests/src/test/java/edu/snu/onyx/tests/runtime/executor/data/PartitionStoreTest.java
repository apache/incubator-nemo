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
package edu.snu.onyx.tests.runtime.executor.data;

import edu.snu.onyx.conf.JobConf;
import edu.snu.onyx.common.coder.BeamCoder;
import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.common.message.MessageEnvironment;
import edu.snu.onyx.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.onyx.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.onyx.runtime.common.state.PartitionState;
import edu.snu.onyx.runtime.executor.data.*;
import edu.snu.onyx.runtime.executor.data.stores.*;
import edu.snu.onyx.runtime.master.PartitionManagerMaster;
import edu.snu.onyx.runtime.master.RuntimeMaster;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static edu.snu.onyx.tests.runtime.RuntimeTestUtil.getRangedNumList;
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
  private static final PartitionManagerWorker worker = mock(PartitionManagerWorker.class);
  private PartitionManagerMaster partitionManagerMaster;
  private LocalMessageDispatcher messageDispatcher;
  // Variables for scatter and gather test
  private static final int NUM_WRITE_TASKS = 3;
  private static final int NUM_READ_TASKS = 3;
  private static final int DATA_SIZE = 1000;
  private List<String> partitionIdList;
  private List<List<NonSerializedBlock>> blocksPerPartition;
  // Variables for concurrent read test
  private static final int NUM_CONC_READ_TASKS = 10;
  private static final int CONC_READ_DATA_SIZE = 1000;
  private String concPartitionId;
  private NonSerializedBlock concPartitionBlock;
  // Variables for scatter and gather in range test
  private static final int NUM_WRITE_HASH_TASKS = 2;
  private static final int NUM_READ_HASH_TASKS = 3;
  private static final int HASH_DATA_SIZE = 1000;
  private static final int HASH_RANGE = 4;
  private List<String> hashedPartitionIdList;
  private List<List<NonSerializedBlock>> hashedPartitionBlockList;
  private List<HashRange> readHashRangeList;
  private List<List<Iterable>> expectedDataInRange;

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
    when(worker.getCoder(any())).thenReturn(CODER);

    // Following part is for for the scatter and gather test.
    final List<String> writeTaskIdList = new ArrayList<>(NUM_WRITE_TASKS);
    final List<String> readTaskIdList = new ArrayList<>(NUM_READ_TASKS);
    partitionIdList = new ArrayList<>(NUM_WRITE_TASKS);
    blocksPerPartition = new ArrayList<>(NUM_WRITE_TASKS);

    // Generates the ids of the tasks to be used.
    IntStream.range(0, NUM_WRITE_TASKS).forEach(
        number -> writeTaskIdList.add(RuntimeIdGenerator.generateTaskId()));
    IntStream.range(0, NUM_READ_TASKS).forEach(
        number -> readTaskIdList.add(RuntimeIdGenerator.generateTaskId()));

    // Generates the ids and the data of the partitions to be used.
    IntStream.range(0, NUM_WRITE_TASKS).forEach(writeTaskIdx -> {
      // Create a partition for each writer task.
      final String partitionId = RuntimeIdGenerator.generatePartitionId(
          RuntimeIdGenerator.generateRuntimeEdgeId(String.valueOf(partitionIdList.size())), writeTaskIdx);
      partitionIdList.add(partitionId);
      partitionManagerMaster.initializeState(partitionId, "Unused");
      partitionManagerMaster.onPartitionStateChanged(
          partitionId, PartitionState.State.SCHEDULED, null);

      // Create blocks for this partition.
      final List<NonSerializedBlock> blocksForPartition = new ArrayList<>(NUM_READ_TASKS);
      blocksPerPartition.add(blocksForPartition);
      IntStream.range(0, NUM_READ_TASKS).forEach(readTaskIdx -> {
        final int blocksCount = writeTaskIdx * NUM_READ_TASKS + readTaskIdx;
        blocksForPartition.add(new NonSerializedBlock(
            readTaskIdx, getRangedNumList(blocksCount * DATA_SIZE, (blocksCount + 1) * DATA_SIZE)));
      });
    });

    // Following part is for the concurrent read test.
    final String writeTaskId = RuntimeIdGenerator.generateTaskId();
    final List<String> concReadTaskIdList = new ArrayList<>(NUM_CONC_READ_TASKS);

    // Generates the ids and the data to be used.
    concPartitionId = RuntimeIdGenerator.generatePartitionId(
        RuntimeIdGenerator.generateRuntimeEdgeId("concurrent read"), NUM_WRITE_TASKS + NUM_READ_TASKS + 1);
    partitionManagerMaster.initializeState(concPartitionId, "Unused");
    partitionManagerMaster.onPartitionStateChanged(
        concPartitionId, PartitionState.State.SCHEDULED, null);
    IntStream.range(0, NUM_CONC_READ_TASKS).forEach(
        number -> concReadTaskIdList.add(RuntimeIdGenerator.generateTaskId()));
    concPartitionBlock = new NonSerializedBlock(0, getRangedNumList(0, CONC_READ_DATA_SIZE));

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
    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writeTaskIdx -> {
      final String partitionId = RuntimeIdGenerator.generatePartitionId(
          RuntimeIdGenerator.generateRuntimeEdgeId("scatter gather in range"),
          NUM_WRITE_TASKS + NUM_READ_TASKS + 1 + writeTaskIdx);
      hashedPartitionIdList.add(partitionId);
      partitionManagerMaster.initializeState(partitionId, "Unused");
      partitionManagerMaster.onPartitionStateChanged(
          partitionId, PartitionState.State.SCHEDULED, null);
      final List<NonSerializedBlock> hashedPartition = new ArrayList<>(HASH_RANGE);
      // Generates the data having each hash value.
      IntStream.range(0, HASH_RANGE).forEach(hashValue ->
          hashedPartition.add(new NonSerializedBlock(hashValue, getFixedKeyRangedNumList(
              hashValue,
              writeTaskIdx * HASH_DATA_SIZE * HASH_RANGE + hashValue * HASH_DATA_SIZE,
              writeTaskIdx * HASH_DATA_SIZE * HASH_RANGE + (hashValue + 1) * HASH_DATA_SIZE))));
      hashedPartitionBlockList.add(hashedPartition);
    });

    // Generates the range of hash value to read for each read task.
    final int smallDataRangeEnd = 1 + NUM_READ_HASH_TASKS - NUM_WRITE_HASH_TASKS;
    readHashRangeList.add(HashRange.of(0, smallDataRangeEnd));
    IntStream.range(0, NUM_READ_HASH_TASKS - 1).forEach(readTaskIdx -> {
      readHashRangeList.add(HashRange.of(smallDataRangeEnd + readTaskIdx, smallDataRangeEnd + readTaskIdx + 1));
    });

    // Generates the expected result of hash range retrieval for each read task.
    for (int readTaskIdx = 0; readTaskIdx < NUM_READ_HASH_TASKS; readTaskIdx++) {
      final HashRange hashRange = readHashRangeList.get(readTaskIdx);
      final List<Iterable> expectedRangeBlocks = new ArrayList<>(NUM_WRITE_HASH_TASKS);
      for (int writeTaskIdx = 0; writeTaskIdx < NUM_WRITE_HASH_TASKS; writeTaskIdx++) {
        final List<Iterable> appendingList = new ArrayList<>();
        for (int hashVal = hashRange.rangeStartInclusive(); hashVal < hashRange.rangeEndExclusive(); hashVal++) {
          appendingList.add(hashedPartitionBlockList.get(writeTaskIdx).get(hashVal).getData());
        }
        final List concatStreamBase = new ArrayList<>();
        Stream<Object> concatStream = concatStreamBase.stream();
        for (final Iterable data : appendingList) {
          concatStream = Stream.concat(concatStream, StreamSupport.stream(data.spliterator(), false));
        }
        expectedRangeBlocks.add(concatStream.collect(Collectors.toList()));
      }
      expectedDataInRange.add(expectedRangeBlocks);
    }
  }

  /**
   * Test {@link MemoryStore}.
   */
  @Test(timeout = 10000)
  public void testMemoryStore() throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(PartitionManagerWorker.class, worker);
    final PartitionStore memoryStore = injector.getInstance(MemoryStore.class);
    scatterGather(memoryStore, memoryStore);
    concurrentRead(memoryStore, memoryStore);
    scatterGatherInHashRange(memoryStore, memoryStore);
  }

  /**
   * Test {@link SerializedMemoryStore}.
   */
  @Test(timeout = 10000)
  public void testSerMemoryStore() throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(PartitionManagerWorker.class, worker);
    final PartitionStore serMemoryStore = injector.getInstance(SerializedMemoryStore.class);
    scatterGather(serMemoryStore, serMemoryStore);
    concurrentRead(serMemoryStore, serMemoryStore);
    scatterGatherInHashRange(serMemoryStore, serMemoryStore);
  }

  /**
   * Test {@link LocalFileStore}.
   */
  @Test(timeout = 10000)
  public void testLocalFileStore() throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.FileDirectory.class, TMP_FILE_DIRECTORY);
    injector.bindVolatileInstance(PartitionManagerWorker.class, worker);

    final PartitionStore localFileStore = injector.getInstance(LocalFileStore.class);
    scatterGather(localFileStore, localFileStore);
    concurrentRead(localFileStore, localFileStore);
    scatterGatherInHashRange(localFileStore, localFileStore);
    FileUtils.deleteDirectory(new File(TMP_FILE_DIRECTORY));
  }

  /**
   * Test {@link GlusterFileStore}.
   * Actually, we cannot create a virtual GFS volume in here.
   * Instead, this test mimics the GFS circumstances by doing the read and write on separate file stores.
   */
  @Test(timeout = 10000)
  public void testGlusterFileStore() throws Exception {
    final RemoteFileStore writerSideRemoteFileStore =
        createGlusterFileStore("writer");
    final RemoteFileStore readerSideRemoteFileStore =
        createGlusterFileStore("reader");

    scatterGather(writerSideRemoteFileStore, readerSideRemoteFileStore);
    concurrentRead(writerSideRemoteFileStore, readerSideRemoteFileStore);
    scatterGatherInHashRange(writerSideRemoteFileStore, readerSideRemoteFileStore);
    FileUtils.deleteDirectory(new File(TMP_FILE_DIRECTORY));
  }

  private GlusterFileStore createGlusterFileStore(final String executorId)
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
    IntStream.range(0, NUM_WRITE_TASKS).forEach(writeTaskIdx ->
        writeFutureList.add(writeExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              IntStream.range(writeTaskIdx, writeTaskIdx + 1).forEach(partitionIdx -> {
                final String partitionId = partitionIdList.get(partitionIdx);
                writerSideStore.createPartition(partitionId);
                writerSideStore.putBlocks(partitionId, blocksPerPartition.get(partitionIdx), false);
                writerSideStore.commitPartition(partitionId);
                partitionManagerMaster.onPartitionStateChanged(partitionId, PartitionState.State.COMMITTED,
                    "Writer side of the scatter gather edge");
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
    IntStream.range(0, NUM_READ_TASKS).forEach(readTaskIdx ->
        readFutureList.add(readExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              for (int writeTaskIdx = 0; writeTaskIdx < NUM_WRITE_TASKS; writeTaskIdx++) {
                readResultCheck(partitionIdList.get(writeTaskIdx), HashRange.of(readTaskIdx, readTaskIdx + 1),
                    readerSideStore, blocksPerPartition.get(writeTaskIdx).get(readTaskIdx).getData());
              }
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

    // Remove all partitions
    partitionIdList.forEach(partitionId -> {
      final boolean exist = readerSideStore.removePartition(partitionId);
      if (!exist) {
        throw new RuntimeException("The result of removePartition(" + partitionId + ") is false");
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
   * -> Task 2
   * Task 1 (write)-> broadcast (concurrent read)-> ...
   * -> Task 11
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
          writerSideStore.createPartition(concPartitionId);
          writerSideStore.putBlocks(concPartitionId, Collections.singleton(concPartitionBlock), false);
          writerSideStore.commitPartition(concPartitionId);
          partitionManagerMaster.onPartitionStateChanged(
              concPartitionId, PartitionState.State.COMMITTED, "Writer side of the concurrent read edge");
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
    IntStream.range(0, NUM_CONC_READ_TASKS).forEach(readTaskIdx ->
        readFutureList.add(readExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              readResultCheck(concPartitionId, HashRange.all(), readerSideStore, concPartitionBlock.getData());
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
   * (read (hash 3))-> Task 5
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
    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writeTaskIdx ->
        writeFutureList.add(writeExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              final String partitionId = hashedPartitionIdList.get(writeTaskIdx);
              writerSideStore.createPartition(partitionId);
              writerSideStore.putBlocks(partitionId,
                  hashedPartitionBlockList.get(writeTaskIdx), false);
              writerSideStore.commitPartition(partitionId);
              partitionManagerMaster.onPartitionStateChanged(partitionId, PartitionState.State.COMMITTED,
                  "Writer side of the scatter gather in hash range edge");
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
    IntStream.range(0, NUM_READ_HASH_TASKS).forEach(readTaskIdx ->
        readFutureList.add(readExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              for (int writeTaskIdx = 0; writeTaskIdx < NUM_WRITE_HASH_TASKS; writeTaskIdx++) {
                final HashRange hashRangeToRetrieve = readHashRangeList.get(readTaskIdx);
                readResultCheck(hashedPartitionIdList.get(writeTaskIdx), hashRangeToRetrieve,
                    readerSideStore, expectedDataInRange.get(readTaskIdx).get(writeTaskIdx));
              }
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

  private List getFixedKeyRangedNumList(final int key,
                                        final int start,
                                        final int end) {
    final List numList = new ArrayList<>(end - start);
    IntStream.range(start, end).forEach(number -> numList.add(KV.of(key, number)));
    return numList;
  }

  /**
   * Compares the expected iterable with the data read from a {@link PartitionStore}.
   */
  private void readResultCheck(final String partitionId,
                               final HashRange hashRange,
                               final PartitionStore partitionStore,
                               final Iterable expectedResult) throws IOException {
    final Optional<Iterable<SerializedBlock>> optionalSerResult =
        partitionStore.getSerializedBlocks(partitionId, hashRange);
    if (!optionalSerResult.isPresent()) {
      throw new IOException("The (serialized) result of get partition" + partitionId + " in range " +
          hashRange + " is empty.");
    }
    final Iterable<SerializedBlock> serializedResult = optionalSerResult.get();
    final Optional<Iterable<NonSerializedBlock>> optionalNonSerResult =
        partitionStore.getBlocks(partitionId, hashRange);
    if (!optionalSerResult.isPresent()) {
      throw new IOException("The (non-serialized) result of get partition" + partitionId + " in range " +
          hashRange + " is empty.");
    }
    final Iterable<NonSerializedBlock> nonSerializedResult = optionalNonSerResult.get();

    assertEquals(expectedResult, DataUtil.concatNonSerBlocks(nonSerializedResult));
    assertEquals(expectedResult, DataUtil.concatNonSerBlocks(DataUtil.convertToNonSerBlocks(CODER, serializedResult)));
  }
}
