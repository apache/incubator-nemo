/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.runtime.executor.task;

import edu.snu.nemo.common.coder.DecoderFactory;
import edu.snu.nemo.common.ir.edge.executionproperty.DecoderProperty;
import edu.snu.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.executor.data.DataUtil;
import edu.snu.nemo.runtime.executor.datatransfer.InputReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

/**
 * Tests {@link ParentTaskDataFetcher}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({InputReader.class, VertexHarness.class})
public final class ParentTaskDataFetcherTest {

  @Test(timeout=5000)
  public void testVoid() throws Exception {
    // TODO #173: Properly handle zero-element. This test should be updated too.
    final List<String> dataElements = new ArrayList<>(0); // empty data
    final InputReader inputReader = generateInputReaderWithCoder(generateCompletableFuture(dataElements.iterator()),
        "VoidCoder");

    // Fetcher
    final ParentTaskDataFetcher fetcher = createFetcher(inputReader);

    // Should return Void.TYPE
    assertEquals(Void.TYPE, fetcher.fetchDataElement());
  }

  @Test(timeout=5000)
  public void testEmpty() throws Exception {
    // TODO #173: Properly handle zero-element. This test should be updated too.
    final List<String> dataElements = new ArrayList<>(0); // empty data
    final InputReader inputReader = generateInputReaderWithCoder(generateCompletableFuture(dataElements.iterator()),
        "IntCoder");

    // Fetcher
    final ParentTaskDataFetcher fetcher = createFetcher(inputReader);

    // Should return Void.TYPE
    assertEquals(null, fetcher.fetchDataElement());
  }

  @Test(timeout=5000)
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
    assertEquals(null, fetcher.fetchDataElement());
  }

  @Test(timeout=5000, expected = IOException.class)
  public void testErrorWhenRPC() throws Exception {
    // Failing future
    final CompletableFuture failingFuture = CompletableFuture.runAsync(() -> {
      try {
        Thread.sleep(2 * 1000); // Block the fetcher for 2 seconds
        throw new RuntimeException(); // Fail this future
      } catch (InterruptedException e) {
        // This shouldn't happen.
        // We don't throw anything here, so that IOException does not occur and the test fails
      }
    }, Executors.newSingleThreadExecutor());
    final InputReader inputReader = generateInputReader(failingFuture);

    // Fetcher
    final ParentTaskDataFetcher fetcher = createFetcher(inputReader);

    // Should throw an IOException
    fetcher.fetchDataElement(); // checked by 'expected = IOException.class'
    assertTrue(failingFuture.isCompletedExceptionally());
  }

  @Test(timeout=5000, expected = IOException.class)
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
        mock(VertexHarness.class),
        false);
  }


  private DecoderFactory generateCoder(final String coder) {
    final DecoderFactory decoderFactory = mock(DecoderFactory.class);
    when(decoderFactory.toString()).thenReturn(coder);
    return decoderFactory;
  }

  private RuntimeEdge generateEdge(final String coder) {
    final String runtimeIREdgeId = "Runtime edge with coder";
    final ExecutionPropertyMap edgeProperties = new ExecutionPropertyMap(runtimeIREdgeId);
    edgeProperties.put(DecoderProperty.of(generateCoder(coder)));
    return new RuntimeEdge<>(runtimeIREdgeId, edgeProperties, mock(IRVertex.class), mock(IRVertex.class), false);
  }

  private InputReader generateInputReaderWithCoder(final CompletableFuture completableFuture, final String coder) {
    final InputReader inputReader = mock(InputReader.class);
    when(inputReader.read()).thenReturn(Arrays.asList(completableFuture));
    final RuntimeEdge runtimeEdge = generateEdge(coder);
    when(inputReader.getRuntimeEdge()).thenReturn(runtimeEdge);
    return inputReader;
  }

  private InputReader generateInputReader(final CompletableFuture completableFuture) {
    final InputReader inputReader = mock(InputReader.class);
    when(inputReader.read()).thenReturn(Arrays.asList(completableFuture));
    return inputReader;
  }

  private CompletableFuture generateCompletableFuture(final Iterator iterator) {
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
