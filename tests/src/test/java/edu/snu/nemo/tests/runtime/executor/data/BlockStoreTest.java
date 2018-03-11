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
package edu.snu.nemo.tests.runtime.executor.data;

import edu.snu.nemo.common.ir.edge.executionproperty.CompressionProperty;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.compiler.frontend.beam.coder.BeamCoder;
import edu.snu.nemo.common.coder.Coder;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.data.HashRange;
import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.nemo.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.nemo.runtime.common.state.BlockState;
import edu.snu.nemo.runtime.executor.data.*;
import edu.snu.nemo.runtime.executor.data.block.Block;
import edu.snu.nemo.runtime.executor.data.partition.NonSerializedPartition;
import edu.snu.nemo.runtime.executor.data.partition.SerializedPartition;
import edu.snu.nemo.runtime.executor.data.streamchainer.CompressionStreamChainer;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;
import edu.snu.nemo.runtime.executor.data.stores.*;
import edu.snu.nemo.runtime.master.BlockManagerMaster;
import edu.snu.nemo.runtime.master.RuntimeMaster;
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

import static edu.snu.nemo.tests.runtime.RuntimeTestUtil.getRangedNumList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests write and read for {@link BlockStore}s.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockManagerMaster.class, RuntimeMaster.class, SerializerManager.class})
public final class BlockStoreTest {
  private static final String TMP_FILE_DIRECTORY = "./tmpFiles";
  private static final Coder CODER = new BeamCoder(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));
  private static final Serializer SERIALIZER = new Serializer(CODER,
      Collections.singletonList(new CompressionStreamChainer(CompressionProperty.Compression.LZ4)));
  private static final SerializerManager serializerManager = mock(SerializerManager.class);
  private BlockManagerMaster blockManagerMaster;
  private LocalMessageDispatcher messageDispatcher;
  // Variables for shuffle test
  private static final int NUM_WRITE_TASKS = 3;
  private static final int NUM_READ_TASKS = 3;
  private static final int DATA_SIZE = 1000;
  private List<String> blockIdList;
  private List<List<NonSerializedPartition<Integer>>> partitionsPerBlock;
  // Variables for concurrent read test
  private static final int NUM_CONC_READ_TASKS = 10;
  private static final int CONC_READ_DATA_SIZE = 1000;
  private String concBlockId;
  private NonSerializedPartition<Integer> concBlockPartition;
  // Variables for shuffle in range test
  private static final int NUM_WRITE_HASH_TASKS = 2;
  private static final int NUM_READ_HASH_TASKS = 3;
  private static final int HASH_DATA_SIZE = 1000;
  private static final int HASH_RANGE = 4;
  private List<String> hashedBlockIdList;
  private List<List<NonSerializedPartition<Integer>>> hashedBlockPartitionList;
  private List<KeyRange> readKeyRangeList;
  private List<List<Iterable>> expectedDataInRange;

  /**
   * Generates the ids and the data which will be used for the block store tests.
   */
  @Before
  public void setUp() throws Exception {
    messageDispatcher = new LocalMessageDispatcher();
    final LocalMessageEnvironment messageEnvironment =
        new LocalMessageEnvironment(MessageEnvironment.MASTER_COMMUNICATION_ID, messageDispatcher);
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(MessageEnvironment.class, messageEnvironment);
    blockManagerMaster = injector.getInstance(BlockManagerMaster.class);
    when(serializerManager.getSerializer(any())).thenReturn(SERIALIZER);

    // Following part is for for the shuffle test.
    final List<String> writeTaskIdList = new ArrayList<>(NUM_WRITE_TASKS);
    final List<String> readTaskIdList = new ArrayList<>(NUM_READ_TASKS);
    blockIdList = new ArrayList<>(NUM_WRITE_TASKS);
    partitionsPerBlock = new ArrayList<>(NUM_WRITE_TASKS);

    // Generates the ids of the tasks to be used.
    IntStream.range(0, NUM_WRITE_TASKS).forEach(
        number -> writeTaskIdList.add(RuntimeIdGenerator.generateLogicalTaskId("Write_IR_vertex")));
    IntStream.range(0, NUM_READ_TASKS).forEach(
        number -> readTaskIdList.add(RuntimeIdGenerator.generateLogicalTaskId("Read_IR_vertex")));

    // Generates the ids and the data of the blocks to be used.
    final String shuffleEdge = RuntimeIdGenerator.generateRuntimeEdgeId("shuffle_edge");
    IntStream.range(0, NUM_WRITE_TASKS).forEach(writeTaskIdx -> {
      // Create a block for each writer task.
      final String blockId = RuntimeIdGenerator.generateBlockId(shuffleEdge, writeTaskIdx);
      blockIdList.add(blockId);
      blockManagerMaster.initializeState(blockId, "Unused");
      blockManagerMaster.onBlockStateChanged(
          blockId, BlockState.State.SCHEDULED, null);

      // Create blocks for this block.
      final List<NonSerializedPartition<Integer>> partitionsForBlock = new ArrayList<>(NUM_READ_TASKS);
      partitionsPerBlock.add(partitionsForBlock);
      IntStream.range(0, NUM_READ_TASKS).forEach(readTaskIdx -> {
        final int partitionsCount = writeTaskIdx * NUM_READ_TASKS + readTaskIdx;
        partitionsForBlock.add(new NonSerializedPartition(
            readTaskIdx, getRangedNumList(partitionsCount * DATA_SIZE, (partitionsCount + 1) * DATA_SIZE), -1, -1));
      });
    });

    // Following part is for the concurrent read test.
    final String writeTaskId = RuntimeIdGenerator.generateLogicalTaskId("conc_write_IR_vertex");
    final List<String> concReadTaskIdList = new ArrayList<>(NUM_CONC_READ_TASKS);
    final String concEdge = RuntimeIdGenerator.generateRuntimeEdgeId("conc_read_edge");

    // Generates the ids and the data to be used.
    concBlockId = RuntimeIdGenerator.generateBlockId(concEdge, NUM_WRITE_TASKS + NUM_READ_TASKS + 1);
    blockManagerMaster.initializeState(concBlockId, "unused");
    blockManagerMaster.onBlockStateChanged(
        concBlockId, BlockState.State.SCHEDULED, null);
    IntStream.range(0, NUM_CONC_READ_TASKS).forEach(
        number -> concReadTaskIdList.add(RuntimeIdGenerator.generateLogicalTaskId("conc_read_IR_vertex")));
    concBlockPartition = new NonSerializedPartition(0, getRangedNumList(0, CONC_READ_DATA_SIZE), -1, -1);

    // Following part is for the shuffle in hash range test
    final int numHashedBlocks = NUM_WRITE_HASH_TASKS;
    final List<String> writeHashTaskIdList = new ArrayList<>(NUM_WRITE_HASH_TASKS);
    final List<String> readHashTaskIdList = new ArrayList<>(NUM_READ_HASH_TASKS);
    readKeyRangeList = new ArrayList<>(NUM_READ_HASH_TASKS);
    hashedBlockIdList = new ArrayList<>(numHashedBlocks);
    hashedBlockPartitionList = new ArrayList<>(numHashedBlocks);
    expectedDataInRange = new ArrayList<>(NUM_READ_HASH_TASKS);

    // Generates the ids of the tasks to be used.
    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(
        number -> writeHashTaskIdList.add(RuntimeIdGenerator.generateLogicalTaskId("hash_write_IR_vertex")));
    IntStream.range(0, NUM_READ_HASH_TASKS).forEach(
        number -> readHashTaskIdList.add(RuntimeIdGenerator.generateLogicalTaskId("hash_read_IR_vertex")));
    final String hashEdge = RuntimeIdGenerator.generateRuntimeEdgeId("hash_edge");

    // Generates the ids and the data of the blocks to be used.
    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writeTaskIdx -> {
      final String blockId = RuntimeIdGenerator.generateBlockId(
          hashEdge, NUM_WRITE_TASKS + NUM_READ_TASKS + 1 + writeTaskIdx);
      hashedBlockIdList.add(blockId);
      blockManagerMaster.initializeState(blockId, "Unused");
      blockManagerMaster.onBlockStateChanged(
          blockId, BlockState.State.SCHEDULED, null);
      final List<NonSerializedPartition<Integer>> hashedBlock = new ArrayList<>(HASH_RANGE);
      // Generates the data having each hash value.
      IntStream.range(0, HASH_RANGE).forEach(hashValue ->
          hashedBlock.add(new NonSerializedPartition(hashValue, getFixedKeyRangedNumList(
              hashValue,
              writeTaskIdx * HASH_DATA_SIZE * HASH_RANGE + hashValue * HASH_DATA_SIZE,
              writeTaskIdx * HASH_DATA_SIZE * HASH_RANGE + (hashValue + 1) * HASH_DATA_SIZE), -1, -1)));
      hashedBlockPartitionList.add(hashedBlock);
    });

    // Generates the range of hash value to read for each read task.
    final int smallDataRangeEnd = 1 + NUM_READ_HASH_TASKS - NUM_WRITE_HASH_TASKS;
    readKeyRangeList.add(HashRange.of(0, smallDataRangeEnd));
    IntStream.range(0, NUM_READ_HASH_TASKS - 1).forEach(readTaskIdx -> {
      readKeyRangeList.add(HashRange.of(smallDataRangeEnd + readTaskIdx, smallDataRangeEnd + readTaskIdx + 1));
    });

    // Generates the expected result of hash range retrieval for each read task.
    for (int readTaskIdx = 0; readTaskIdx < NUM_READ_HASH_TASKS; readTaskIdx++) {
      final KeyRange<Integer> hashRange = readKeyRangeList.get(readTaskIdx);
      final List<Iterable> expectedRangePartitions = new ArrayList<>(NUM_WRITE_HASH_TASKS);
      for (int writeTaskIdx = 0; writeTaskIdx < NUM_WRITE_HASH_TASKS; writeTaskIdx++) {
        final List<Iterable> appendingList = new ArrayList<>();
        for (int hashVal = hashRange.rangeBeginInclusive(); hashVal < hashRange.rangeEndExclusive(); hashVal++) {
          appendingList.add(hashedBlockPartitionList.get(writeTaskIdx).get(hashVal).getData());
        }
        final List concatStreamBase = new ArrayList<>();
        Stream<Object> concatStream = concatStreamBase.stream();
        for (final Iterable data : appendingList) {
          concatStream = Stream.concat(concatStream, StreamSupport.stream(data.spliterator(), false));
        }
        expectedRangePartitions.add(concatStream.collect(Collectors.toList()));
      }
      expectedDataInRange.add(expectedRangePartitions);
    }
  }

  /**
   * Test {@link MemoryStore}.
   */
  @Test(timeout = 10000)
  public void testMemoryStore() throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(SerializerManager.class, serializerManager);
    final BlockStore memoryStore = injector.getInstance(MemoryStore.class);
    shuffle(memoryStore, memoryStore);
    concurrentRead(memoryStore, memoryStore);
    shuffleInHashRange(memoryStore, memoryStore);
  }

  /**
   * Test {@link SerializedMemoryStore}.
   */
  @Test(timeout = 10000)
  public void testSerMemoryStore() throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(SerializerManager.class, serializerManager);
    final BlockStore serMemoryStore = injector.getInstance(SerializedMemoryStore.class);
    shuffle(serMemoryStore, serMemoryStore);
    concurrentRead(serMemoryStore, serMemoryStore);
    shuffleInHashRange(serMemoryStore, serMemoryStore);
  }

  /**
   * Test {@link LocalFileStore}.
   */
  @Test(timeout = 10000)
  public void testLocalFileStore() throws Exception {
    FileUtils.deleteDirectory(new File(TMP_FILE_DIRECTORY));
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.FileDirectory.class, TMP_FILE_DIRECTORY);
    injector.bindVolatileInstance(SerializerManager.class, serializerManager);

    final BlockStore localFileStore = injector.getInstance(LocalFileStore.class);
    shuffle(localFileStore, localFileStore);
    concurrentRead(localFileStore, localFileStore);
    shuffleInHashRange(localFileStore, localFileStore);
    FileUtils.deleteDirectory(new File(TMP_FILE_DIRECTORY));
  }

  /**
   * Test {@link GlusterFileStore}.
   * Actually, we cannot create a virtual GFS volume in here.
   * Instead, this test mimics the GFS circumstances by doing the read and write on separate file stores.
   */
  @Test(timeout = 10000)
  public void testGlusterFileStore() throws Exception {
    FileUtils.deleteDirectory(new File(TMP_FILE_DIRECTORY));
    final RemoteFileStore writerSideRemoteFileStore =
        createGlusterFileStore("writer");
    final RemoteFileStore readerSideRemoteFileStore =
        createGlusterFileStore("reader");

    shuffle(writerSideRemoteFileStore, readerSideRemoteFileStore);
    concurrentRead(writerSideRemoteFileStore, readerSideRemoteFileStore);
    shuffleInHashRange(writerSideRemoteFileStore, readerSideRemoteFileStore);
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
    injector.bindVolatileInstance(SerializerManager.class, serializerManager);
    injector.bindVolatileInstance(MessageEnvironment.class, localMessageEnvironment);
    return injector.getInstance(GlusterFileStore.class);
  }

  /**
   * Tests shuffle for {@link BlockStore}s.
   * Assumes following circumstances:
   * Task 1 (write)->         (read)-> Task 4
   * Task 2 (write)-> shuffle (read)-> Task 5
   * Task 3 (write)->         (read)-> Task 6
   * It checks that each writer and reader does not throw any exception
   * and the read data is identical with written data (including the order).
   */
  private void shuffle(final BlockStore writerSideStore,
                       final BlockStore readerSideStore) {
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
              final String blockId = blockIdList.get(writeTaskIdx);
              final Block block = writerSideStore.createBlock(blockId);
              for (final NonSerializedPartition<Integer> partition : partitionsPerBlock.get(writeTaskIdx)) {
                final Iterable data = partition.getData();
                data.forEach(element -> block.write(partition.getKey(), element));
              }
              block.commit();
              writerSideStore.writeBlock(block);
              blockManagerMaster.onBlockStateChanged(blockId, BlockState.State.COMMITTED,
                  "Writer side of the shuffle edge");
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
                readResultCheck(blockIdList.get(writeTaskIdx), HashRange.of(readTaskIdx, readTaskIdx + 1),
                    readerSideStore, partitionsPerBlock.get(writeTaskIdx).get(readTaskIdx).getData());
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

    // Remove all blocks
    blockIdList.forEach(blockId -> {
      final boolean exist = readerSideStore.deleteBlock(blockId);
      if (!exist) {
        throw new RuntimeException("The result of deleteBlock(" + blockId + ") is false");
      }
    });

    final long readEndNano = System.nanoTime();

    writeExecutor.shutdown();
    readExecutor.shutdown();

    System.out.println(
        "Shuffle - write time in millis: " + (writeEndNano - startNano) / 1000000 +
            ", Read time in millis: " + (readEndNano - writeEndNano) / 1000000 + " in store " +
            writerSideStore.getClass().toString());
  }

  /**
   * Tests concurrent read for {@link BlockStore}s.
   * Assumes following circumstances:
   * -> Task 2
   * Task 1 (write)-> broadcast (concurrent read)-> ...
   * -> Task 11
   * It checks that each writer and reader does not throw any exception
   * and the read data is identical with written data (including the order).
   */
  private void concurrentRead(final BlockStore writerSideStore,
                              final BlockStore readerSideStore) {
    final ExecutorService writeExecutor = Executors.newSingleThreadExecutor();
    final ExecutorService readExecutor = Executors.newFixedThreadPool(NUM_CONC_READ_TASKS);
    final Future<Boolean> writeFuture;
    final List<Future<Boolean>> readFutureList = new ArrayList<>(NUM_CONC_READ_TASKS);
    final long startNano = System.nanoTime();

    // Write a block
    writeFuture = writeExecutor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        try {
          final Block block = writerSideStore.createBlock(concBlockId);
          final Iterable data = concBlockPartition.getData();
          data.forEach(element -> block.write(concBlockPartition.getKey(), element));
          block.commit();
          writerSideStore.writeBlock(block);
          blockManagerMaster.onBlockStateChanged(
              concBlockId, BlockState.State.COMMITTED, "Writer side of the concurrent read edge");
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

    // Read the single block concurrently and check whether the result is equal to the input
    IntStream.range(0, NUM_CONC_READ_TASKS).forEach(readTaskIdx ->
        readFutureList.add(readExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              readResultCheck(concBlockId, HashRange.all(), readerSideStore, concBlockPartition.getData());
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

    // Remove the block
    final boolean exist = writerSideStore.deleteBlock(concBlockId);
    if (!exist) {
      throw new RuntimeException("The result of deleteBlock(" + concBlockId + ") is false");
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
   * Tests shuffle in hash range for {@link BlockStore}s.
   * Assumes following circumstances:
   * Task 1 (write (hash 0~3))->         (read (hash 0~1))-> Task 3
   * Task 2 (write (hash 0~3))-> shuffle (read (hash 2))-> Task 4
   *                                     (read (hash 3))-> Task 5
   * It checks that each writer and reader does not throw any exception
   * and the read data is identical with written data (including the order).
   */
  private void shuffleInHashRange(final BlockStore writerSideStore,
                                  final BlockStore readerSideStore) {
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
              final String blockId = hashedBlockIdList.get(writeTaskIdx);
              final Block block = writerSideStore.createBlock(blockId);
              for (final NonSerializedPartition<Integer> partition : hashedBlockPartitionList.get(writeTaskIdx)) {
                final Iterable data = partition.getData();
                data.forEach(element -> block.write(partition.getKey(), element));
              }
              block.commit();
              writerSideStore.writeBlock(block);
              blockManagerMaster.onBlockStateChanged(blockId, BlockState.State.COMMITTED,
                  "Writer side of the shuffle in hash range edge");
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
                final KeyRange<Integer> hashRangeToRetrieve = readKeyRangeList.get(readTaskIdx);
                readResultCheck(hashedBlockIdList.get(writeTaskIdx), hashRangeToRetrieve,
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

    // Remove stored blocks
    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writer -> {
      final boolean exist = writerSideStore.deleteBlock(hashedBlockIdList.get(writer));
      if (!exist) {
        throw new RuntimeException("The result of deleteBlock(" +
            hashedBlockIdList.get(writer) + ") is false");
      }
    });

    writeExecutor.shutdown();
    readExecutor.shutdown();

    System.out.println(
        "Shuffle in hash range - write time in millis: " + (writeEndNano - startNano) / 1000000 +
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
   * Compares the expected iterable with the data read from a {@link BlockStore}.
   */
  private void readResultCheck(final String blockId,
                               final KeyRange hashRange,
                               final BlockStore blockStore,
                               final Iterable expectedResult) throws IOException {
    final Optional<Block> optionalBlock = blockStore.readBlock(blockId);
    if (!optionalBlock.isPresent()) {
      throw new IOException("The result block " + blockId + " is empty.");
    }
    final Iterable<NonSerializedPartition> nonSerializedResult = optionalBlock.get().readPartitions(hashRange);
    final Iterable serToNonSerialized = DataUtil.convertToNonSerPartitions(
        SERIALIZER, optionalBlock.get().readSerializedPartitions(hashRange));

    assertEquals(expectedResult, DataUtil.concatNonSerPartitions(nonSerializedResult));
    assertEquals(expectedResult, DataUtil.concatNonSerPartitions(serToNonSerialized));
  }
}
