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
package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.executionproperty.BlockFetchFailureProperty;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.datatransfer.BlockInputReader;
import org.apache.nemo.runtime.executor.datatransfer.InputReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link ParentTaskDataFetcher}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({InputReader.class, VertexHarness.class, BlockInputReader.class})
public final class ParentTaskDataFetcherTest {

  @Test(timeout = 5000)
  public void testEmpty() throws Exception {
    final List<String> empty = new ArrayList<>(0); // empty data
    final InputReader inputReader = generateInputReader(generateCompletableFuture(empty.iterator()));

    // Fetcher
    final ParentTaskDataFetcher fetcher = createFetcher(inputReader);
    assertEquals(Finishmark.getInstance(), fetcher.fetchDataElement());
  }

  @Test(timeout = 5000)
  public void testNull() throws Exception {
    final List<String> oneNull = new ArrayList<>(1); // empty data
    oneNull.add(null);
    final InputReader inputReader = generateInputReader(generateCompletableFuture(oneNull.iterator()));

    // Fetcher
    final ParentTaskDataFetcher fetcher = createFetcher(inputReader);

    // Should return 'null'
    assertEquals(null, fetcher.fetchDataElement());
  }

  @Test(timeout = 5000)
  public void testNonEmpty() throws Exception {
    // InputReader
    final String singleData = "Single";
    final List<String> dataElements = new ArrayList<>(1);
    dataElements.add(singleData); // Single element
    final InputReader inputReader = generateInputReader(generateCompletableFuture(dataElements.iterator()));

    // Fetcher
    final ParentTaskDataFetcher fetcher = createFetcher(inputReader);

    // Should return only a single element
    assertEquals(singleData, fetcher.fetchDataElement());
  }

  @Test(timeout = 5000, expected = IOException.class)
  public void testErrorWhenFuture() throws Exception {
    // Failing future
    final CompletableFuture failingFuture = generateFailingFuture();
    final InputReader inputReader = generateInputReader(failingFuture);

    // Fetcher
    final ParentTaskDataFetcher fetcher = createFetcher(inputReader);

    // Should throw an IOException
    fetcher.fetchDataElement(); // checked by 'expected = IOException.class'
    assertTrue(failingFuture.isCompletedExceptionally());
  }

  @Test(timeout = 5000)
  public void testErrorWhenFutureWithRetry() throws Exception {
    // Failing future
    final CompletableFuture failingFuture = generateFailingFuture();
    final InputReader inputReader = generateInputReader(
      failingFuture,
      BlockFetchFailureProperty.of(BlockFetchFailureProperty.Value.RETRY_AFTER_TWO_SECONDS_FOREVER)); // retry

    final List<String> empty = new ArrayList<>(0); // empty data
    when(inputReader.retry(anyInt()))
      .thenReturn(generateCompletableFuture(
        empty.iterator())); // success upon retry

    // Fetcher should work on retry
    final ParentTaskDataFetcher fetcher = createFetcher(inputReader);
    assertEquals(Finishmark.getInstance(), fetcher.fetchDataElement());
  }

  @Test(timeout = 5000, expected = IOException.class)
  public void testErrorWhenReadingData() throws Exception {
    // Failed iterator
    final InputReader inputReader = generateInputReader(generateCompletableFuture(new FailedIterator()));

    // Fetcher
    final ParentTaskDataFetcher fetcher = createFetcher(inputReader);

    // Should throw an IOException
    fetcher.fetchDataElement(); // checked by 'expected = IOException.class'
  }

  private ParentTaskDataFetcher createFetcher(final InputReader readerForParentTask) {
    return new ParentTaskDataFetcher(
      mock(IRVertex.class),
      readerForParentTask, // This is the only argument that affects the behavior of ParentTaskDataFetcher
      mock(OutputCollector.class));
  }

  private InputReader generateInputReader(final CompletableFuture completableFuture,
                                          final EdgeExecutionProperty... properties) {
    final InputReader inputReader = mock(InputReader.class, Mockito.CALLS_REAL_METHODS);
    when(inputReader.getSrcIrVertex()).thenReturn(mock(IRVertex.class));
    when(inputReader.read()).thenReturn(Arrays.asList(completableFuture));
    final ExecutionPropertyMap<EdgeExecutionProperty> propertyMap = new ExecutionPropertyMap<>("");
    for (final EdgeExecutionProperty p : properties) {
      propertyMap.put(p);
    }
    when(inputReader.getProperties()).thenReturn(propertyMap);
    return inputReader;
  }

  private CompletableFuture generateFailingFuture() {
    return CompletableFuture.runAsync(() -> {
      throw new RuntimeException(); // Fail this future
    }, Executors.newSingleThreadExecutor());
  }

  private CompletableFuture<DataUtil.IteratorWithNumBytes> generateCompletableFuture(final Iterator iterator) {
    return CompletableFuture.completedFuture(DataUtil.IteratorWithNumBytes.of(iterator));
  }

  private class FailedIterator implements Iterator {
    @Override
    public boolean hasNext() {
      throw new RuntimeException("Fail");
    }

    @Override
    public Object next() {
      throw new RuntimeException("Fail");
    }
  }
}
