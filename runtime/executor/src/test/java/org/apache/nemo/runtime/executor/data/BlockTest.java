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
package org.apache.nemo.runtime.executor.data;

import org.apache.commons.io.FileUtils;
import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.coder.IntDecoderFactory;
import org.apache.nemo.common.coder.IntEncoderFactory;
import org.apache.nemo.runtime.executor.data.block.Block;
import org.apache.nemo.runtime.executor.data.block.FileBlock;
import org.apache.nemo.runtime.executor.data.block.NonSerializedMemoryBlock;
import org.apache.nemo.runtime.executor.data.block.SerializedMemoryBlock;
import org.apache.nemo.runtime.executor.data.metadata.LocalFileMetadata;
import org.apache.nemo.runtime.executor.data.partition.NonSerializedPartition;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;

/**
 * Tests write and read for {@link Block}s.
 */
public final class BlockTest {
  private Serializer serializer;
  private Map<Integer, List<Integer>> testData;
  private MemoryPoolAssigner memoryPoolAssigner;

  /**
   * Generates the test data and serializer.
   *
   * @throws Exception exception on the way.
   */
  @Before
  public void setUp() throws Exception {
    serializer = new Serializer<>(IntEncoderFactory.of(), IntDecoderFactory.of(), new ArrayList<>(), new ArrayList<>());
    testData = new HashMap<>();
    memoryPoolAssigner = new MemoryPoolAssigner(1, 32);

    final List<Integer> list1 = Collections.singletonList(1);
    final List<Integer> list2 = Arrays.asList(1, 2);
    final List<Integer> list3 = Arrays.asList(1, 2, 3);

    testData.put(1, list1);
    testData.put(2, list2);
    testData.put(3, list3);
  }

  /**
   * Test {@link NonSerializedMemoryBlock}.
   *
   * @throws Exception exception on the way.
   */
  @Test(timeout = 10000)
  public void testNonSerializedMemoryBlock() throws Exception {
    final Block<Integer> block = new NonSerializedMemoryBlock<>("testBlock", serializer, memoryPoolAssigner);
    testBlock(block);
  }

  /**
   * Test {@link org.apache.nemo.runtime.executor.data.block.SerializedMemoryBlock}.
   *
   * @throws Exception exception on the way.
   */
  @Test(timeout = 10000)
  public void testSerializedMemoryBlock() throws Exception {
    final Block<Integer> block = new SerializedMemoryBlock<>("testBlock", serializer, memoryPoolAssigner);
    testBlock(block);
  }

  /**
   * Test {@link FileBlock}.
   *
   * @throws Exception exception on the way.
   */
  @Test(timeout = 10000)
  public void testFileBlock() throws Exception {
    final String tmpDir = "./tmpFiles";
    final String filePath = tmpDir + "/BlockTestFile";
    try {
      new File(tmpDir).mkdirs();
      final LocalFileMetadata<Integer> metadata = new LocalFileMetadata<>();
      final Block<Integer> block = new FileBlock<>("testBlock", serializer, filePath, metadata, memoryPoolAssigner);
      testBlock(block);
    } finally {
      FileUtils.deleteDirectory(new File(tmpDir));
    }
  }


  /**
   * Tests write to & read from a block.
   */
  private void testBlock(final Block<Integer> block) throws Exception {
    // Write elements to partitions in the block
    testData.forEach((key, partitionData) -> partitionData.forEach(element -> block.write(key, element)));

    // Commit all partitions
    block.commitPartitions();

    // Write elements again. Because all partitions are committed, new partitions for each key will be created.
    testData.forEach((key, partitionData) -> partitionData.forEach(element -> block.write(key, element)));

    // Commit the block
    block.commit();

    int count = 0;
    final Iterable<NonSerializedPartition<Integer>> partitions = block.readPartitions(HashRange.all());
    for (final NonSerializedPartition<Integer> readPartition : partitions) {
      count++;
      final List<Integer> expectedData = testData.get(readPartition.getKey());
      final Iterable<Integer> readData = readPartition.getData();
      compare(expectedData, readData);
    }
    Assert.assertEquals(count, testData.size() * 2);
  }

  /**
   * Compare the contents of a list and an iterable.
   *
   * @param list     the list to test.
   * @param iterable the iterable to test.
   * @throws RuntimeException if the contents are not matched.
   */
  private void compare(final List<Integer> list,
                       final Iterable<Integer> iterable) throws RuntimeException {
    final List<Integer> copiedList = new ArrayList<>(list);
    for (final Integer element : iterable) {
      if (!copiedList.remove(element)) {
        throw new RuntimeException("Contents mismatch! \nlist: " + list + "\niterable: " + iterable);
      }
    }
  }
}
