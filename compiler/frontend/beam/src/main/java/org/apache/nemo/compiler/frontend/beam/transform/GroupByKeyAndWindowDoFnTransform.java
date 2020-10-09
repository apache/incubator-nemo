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

import org.apache.beam.runners.core.*;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

import static java.util.Collections.emptyMap;

/**
 * Groups elements according to key and window.
 * @param <K> key type.
 * @param <InputT> input type.
 */
public final class GroupByKeyAndWindowDoFnTransform<K, InputT>
  extends AbstractDoFnTransform<KV<K, InputT>, KeyedWorkItem<K, InputT>, KV<K, Iterable<InputT>>> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKeyAndWindowDoFnTransform.class.getName());

  private final SystemReduceFn reduceFn;
  private transient InMemoryTimerInternalsFactory inMemoryTimerInternalsFactory;
  private transient InMemoryStateInternalsFactory inMemoryStateInternalsFactory;
  private Watermark prevOutputWatermark;
  private final Map<K, Watermark> keyAndWatermarkHoldMap;
  private final WindowingStrategy windowingStrategy;
  private Watermark inputWatermark;

  int numProcessedData = 0;

  private boolean dataReceived = false;

  /**
   * GroupByKey constructor.
   */
  public GroupByKeyAndWindowDoFnTransform(final Map<TupleTag<?>, Coder<?>> outputCoders,
                                          final TupleTag<KV<K, Iterable<InputT>>> mainOutputTag,
                                          final WindowingStrategy<?, ?> windowingStrategy,
                                          final PipelineOptions options,
                                          final SystemReduceFn reduceFn,
                                          final DisplayData displayData) {
    super(null, /* GBK doesn't have doFn */
      null, /* inputCoder */
      outputCoders,
      mainOutputTag,
      Collections.emptyList(),  /*  GBK does not have additional outputs */
      windowingStrategy,
      Collections.emptyMap(), /*  GBK does not have additional side inputs */
      options,
      displayData,
      DoFnSchemaInformation.create(),
      Collections.<String, PCollectionView<?>>emptyMap());
    this.reduceFn = reduceFn;
    this.prevOutputWatermark = new Watermark(Long.MIN_VALUE);
    this.inputWatermark = new Watermark(Long.MIN_VALUE);
    this.keyAndWatermarkHoldMap = new HashMap<>();
    this.windowingStrategy = windowingStrategy;
  }

  /**
   * This creates a new DoFn that groups elements by key and window.
   * @param doFn original doFn.
   * @return GroupAlsoByWindowViaWindowSetNewDoFn
   */
  @Override
  protected DoFn wrapDoFn(final DoFn doFn) {
    this.inMemoryStateInternalsFactory = new InMemoryStateInternalsFactory();
    this.inMemoryTimerInternalsFactory = new InMemoryTimerInternalsFactory();

    // This function performs group by key and window operation
    return
      GroupAlsoByWindowViaWindowSetNewDoFn.create(
        getWindowingStrategy(),
        inMemoryStateInternalsFactory,
        inMemoryTimerInternalsFactory,
        null, // GBK has no side input
        reduceFn,
        getOutputManager(),
        getMainOutputTag());
  }

  /**
   * Wrapper function of output collector
   * @param oc the original outputCollector.
   * @return GBKWOutputCollector
   */
  @Override
  OutputCollector wrapOutputCollector(final OutputCollector oc) {
    return new GBKWOutputCollector(oc);
  }

  /**
   * It collects data for each key.
   * The collected data are emitted at {@link GroupByKeyAndWindowDoFnTransform#onWatermark(Watermark)}
   * @param element data element
   */
  @Override
  public void onData(final WindowedValue<KV<K, InputT>> element) {
    dataReceived = true;
    final KV<K, InputT> kv = element.getValue();
    checkAndInvokeBundle();
    final KeyedWorkItem<K, InputT> keyedWorkItem =
      KeyedWorkItems.elementsWorkItem(kv.getKey(),
        Collections.singletonList(element.withValue(kv.getValue())));
    numProcessedData += 1;
    // The DoFnRunner interface requires WindowedValue,
    // but this windowed value is actually not used in the ReduceFnRunner internal.
    getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(keyedWorkItem));
    checkAndFinishBundle();
  }

  /**
   * Process the collected data and trigger timers.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   */
  private void processElementsAndTriggerTimers(final Instant processingTime,
                                               final Instant synchronizedTime) {
    triggerTimers(processingTime, synchronizedTime);
  }

  /**
   * Output watermark
   * = max(prev output watermark,
   *          min(input watermark, watermark holds)).
   */
  private void emitOutputWatermark() {
    // Find min watermark hold
    final Watermark minWatermarkHold = keyAndWatermarkHoldMap.isEmpty()
      ? new Watermark(dataReceived ? Long.MIN_VALUE : Long.MAX_VALUE)
      : Collections.min(keyAndWatermarkHoldMap.values());
    final Watermark outputWatermarkCandidate = new Watermark(
      Math.max(prevOutputWatermark.getTimestamp(),
        Math.min(minWatermarkHold.getTimestamp(), inputWatermark.getTimestamp())));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Watermark hold: {}, "
        + "inputWatermark: {}, outputWatermark: {}", minWatermarkHold, inputWatermark, prevOutputWatermark);
    }

    if (outputWatermarkCandidate.getTimestamp() > prevOutputWatermark.getTimestamp()) {
      // progress!
      prevOutputWatermark = outputWatermarkCandidate;
      // emit watermark.
      getOutputCollector().emitWatermark(outputWatermarkCandidate);
      // Remove minimum watermark holds
      if (minWatermarkHold.getTimestamp() == outputWatermarkCandidate.getTimestamp()) {
        keyAndWatermarkHoldMap.entrySet()
          .removeIf(entry -> entry.getValue().getTimestamp() == minWatermarkHold.getTimestamp());
      }
    }
  }

  /**
   * Receives watermark and process timers
   * @param watermark watermark
   */
  @Override
  public void onWatermark(final Watermark watermark) {
    checkAndInvokeBundle();
    inputWatermark = watermark;
    processElementsAndTriggerTimers(Instant.now(), Instant.now());
    emitOutputWatermark();
    checkAndFinishBundle();
  }

  /**
   * This advances the input watermark and processing time to the timestamp max value
   * in order to emit all data.
   */
  @Override
  protected void beforeClose() {
    // Finish any pending windows by advancing the input watermark to infinity.
    inputWatermark = new Watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
    processElementsAndTriggerTimers(BoundedWindow.TIMESTAMP_MAX_VALUE, BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  /**
   * Trigger timers for current key.
   * When triggering, it emits the windowed data to downstream operators.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   */
  private void triggerTimers(final Instant processingTime,
                            final Instant synchronizedTime) {

    inMemoryTimerInternalsFactory.inputWatermarkTime = new Instant(inputWatermark.getTimestamp());
    inMemoryTimerInternalsFactory.processingTime = processingTime;
    inMemoryTimerInternalsFactory.synchronizedProcessingTime = synchronizedTime;

    Pair<K, TimerInternals.TimerData> timer = inMemoryTimerInternalsFactory.getNextTimer();
    while (timer != null) {
      final NemoTimerInternals timerInternals = (NemoTimerInternals)
        inMemoryTimerInternalsFactory.timerInternalsMap.get(timer.left());
      timerInternals.setCurrentInputWatermarkTime(new Instant(inputWatermark.getTimestamp()));
      timerInternals.setCurrentProcessingTime(processingTime);
      timerInternals.setCurrentSynchronizedProcessingTime(synchronizedTime);
      // Trigger timers and emit windowed data
      final KeyedWorkItem<K, InputT> timerWorkItem =
        KeyedWorkItems.timersWorkItem(timer.left(), Collections.singletonList(timer.right()));
      // The DoFnRunner interface requires WindowedValue,
      // but this windowed value is actually not used in the ReduceFnRunner internal.
      getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(timerWorkItem));
      timer = inMemoryTimerInternalsFactory.getNextTimer();
    }
  }

  public class GBKWOutputCollector implements OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> {
    OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> oc;

    public GBKWOutputCollector(OutputCollector oc) {
      this.oc = oc;
    }

    @Override
    public void emit(final WindowedValue<KV<K,Iterable<InputT>>> output) {
      // The watermark advances only in ON_TIME
      if (output.getPane().getTiming().equals(PaneInfo.Timing.ON_TIME)) {
        KV<K, Iterable<InputT>> value = output.getValue();
        final K key = value.getKey();
        final NemoTimerInternals timerInternals = (NemoTimerInternals)
          inMemoryTimerInternalsFactory.timerInternalsForKey(key);
        keyAndWatermarkHoldMap.put(key,
          // adds the output timestamp to the watermark hold of each key
          // +1 to the output timestamp because if the window is [0-5000), the timestamp is 4999
          new Watermark(output.getTimestamp().getMillis() + 1));
        timerInternals.setCurrentOutputWatermarkTime(new Instant(output.getTimestamp().getMillis() + 1));
      }
      oc.emit(output);
    }

    @Override
    public void emitWatermark(final Watermark watermark) {
      oc.emitWatermark(watermark);
    }

    @Override
    public <T> void emit(final String dstVertexId, final T output) {
      oc.emit(dstVertexId, output);
    }
  }
}
