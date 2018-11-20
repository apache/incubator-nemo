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
package org.apache.nemo.compiler.frontend.beam.transform;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.executor.datatransfer.MainInputLambdaCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

/**
 * This transforms emits materialized data for each window.
 * @param <I> input type
 * @param <O> materialized output type
 */
public final class CreateViewTransform<I, O> implements Transform<WindowedValue<KV<?, I>>, WindowedValue<O>> {
  private final ViewFn<Object, O> viewFn;
  private final Map<BoundedWindow, List<I>> windowListMap;
  private static final Logger LOG = LoggerFactory.getLogger(CreateViewTransform.class.getName());


  private OutputCollector<WindowedValue<O>> outputCollector;

  private long currentOutputWatermark;

  private Context context;

  /**
   * Constructor of CreateViewTransform.
   * @param viewFn the view function.
   */
  public CreateViewTransform(final ViewFn<Object, O> viewFn)  {
    this.viewFn = viewFn;
    this.windowListMap = new HashMap<>();
    this.currentOutputWatermark = Long.MIN_VALUE;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<O>> oc) {
    this.context = context;
    this.outputCollector = oc;
  }

  @Override
  public void onData(final WindowedValue<KV<?, I>> element) {
    // The key of element is always null (beam's semantic)
    // because view is a globally materialized data regardless of key
    LOG.info("CreateViewTransform data: {}", element);
    for (final BoundedWindow window : element.getWindows()) {
      windowListMap.putIfAbsent(window, new ArrayList<>());
      final List<I> list = windowListMap.get(window);
      list.add(element.getValue().getValue());
    }
  }

  @Override
  public void onWatermark(final Watermark inputWatermark) {
    // If no data, just forwards the watermark
    if (windowListMap.size() == 0 && currentOutputWatermark < inputWatermark.getTimestamp()) {
      currentOutputWatermark = inputWatermark.getTimestamp();
      outputCollector.emitWatermark(inputWatermark);
      return;
    }

    final Iterator<Map.Entry<BoundedWindow, List<I>>> iterator = windowListMap.entrySet().iterator();
    long minOutputTimestampOfEmittedWindows = Long.MAX_VALUE;

    while (iterator.hasNext()) {
      final Map.Entry<BoundedWindow, List<I>> entry = iterator.next();
      if (entry.getKey().maxTimestamp().getMillis() < inputWatermark.getTimestamp()) {
        // emit the windowed data if the watermark timestamp > the window max boundary
        final O output = viewFn.apply(new MultiView<>(entry.getValue()));
        outputCollector.emit(WindowedValue.of(
          output, entry.getKey().maxTimestamp(), entry.getKey(), PaneInfo.ON_TIME_AND_ONLY_FIRING));
        iterator.remove();

        minOutputTimestampOfEmittedWindows =
          Math.min(minOutputTimestampOfEmittedWindows, entry.getKey().maxTimestamp().getMillis());
      }
    }

    if (minOutputTimestampOfEmittedWindows != Long.MAX_VALUE
      && currentOutputWatermark < minOutputTimestampOfEmittedWindows) {
      // update current output watermark and emit to next operators
      currentOutputWatermark = minOutputTimestampOfEmittedWindows;
      outputCollector.emitWatermark(new Watermark(currentOutputWatermark));
    }
  }

  /*
  private void writeToLambda(final WindowedValue wv) {

      final PutRecordRequest putRecordRequest = new PutRecordRequest();
      putRecordRequest.setStreamName(STREAMNAME);
      PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
      putRecordsRequest.setStreamName(STREAMNAME);
      List<PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>();
      for (int i = 0; i < 1; i++) {
        PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
        putRecordsRequestEntry.setData(ByteBuffer.wrap(String.valueOf(i).getBytes()));
        putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
        putRecordsRequestEntryList.add(putRecordsRequestEntry);
      }
      putRecordsRequest.setRecords(putRecordsRequestEntryList);
      PutRecordsResult putRecordsResult  = kinesisClient.putRecords(putRecordsRequest);
      System.out.println("Put Result" + putRecordsResult);
  }
  */

  @Override
  public void close() {
    onWatermark(new Watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("CreateViewTransform:" + viewFn);
    return sb.toString();
  }

  /**
   * Represents {@code PrimitiveViewT} supplied to the {@link ViewFn}.
   * @param <T> primitive view type
   */
  public static final class MultiView<T> implements Materializations.MultimapView<Void, T>, Serializable {
    private final Iterable<T> iterable;

    /**
     * Constructor.
     */
    public MultiView(final Iterable<T> iterable) {
      // Create a placeholder for side input data. CreateViewTransform#onData stores data to this list.
      this.iterable = iterable;
    }

    @Override
    public Iterable<T> get(@Nullable final Void aVoid) {
      return iterable;
    }
  }
}
