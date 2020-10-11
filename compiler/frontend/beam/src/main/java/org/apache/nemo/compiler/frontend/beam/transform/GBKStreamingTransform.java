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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.transform.coders.GBKStateCoder;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import java.util.*;

/**
 * This transform performs GroupByKey or CombinePerKey operation for streaming data.
 * @param <K> key type.
 * @param <InputT> input type.
 * @param <OutputT> output type.
 */
public final class GBKStreamingTransform<K, InputT, OutputT>
  extends AbstractDoFnTransform<KV<K, InputT>, KeyedWorkItem<K, InputT>, KV<K, OutputT>>
  implements StatefulTransform<GBKState<K>> {
  private static final Logger LOG = LoggerFactory.getLogger(GBKStreamingTransform.class.getName());
  private final SystemReduceFn reduceFn;
  private transient InMemoryTimerInternalsFactory<K> inMemoryTimerInternalsFactory;
  private transient InMemoryStateInternalsFactory<K> inMemoryStateInternalsFactory;
  private volatile Watermark prevOutputWatermark;
  private volatile Map<K, Watermark> keyAndWatermarkHoldMap;
  private volatile Watermark inputWatermark;
  private transient OutputCollector originOc;
  private final Coder<K> keyCoder;
  private final Coder windowCoder;
  private volatile boolean dataReceived = false;

  public GBKStreamingTransform(final Coder<K> keyCoder,
                           final Map<TupleTag<?>, Coder<?>> outputCoders,
                           final TupleTag<KV<K, OutputT>> mainOutputTag,
                           final WindowingStrategy<?, ?> windowingStrategy,
                           final PipelineOptions options,
                           final SystemReduceFn reduceFn,
                           final DoFnSchemaInformation doFnSchemaInformation,
                           final DisplayData displayData) {
    super(null, /* doFn */
      null, /* inputCoder */
      outputCoders,
      mainOutputTag,
      Collections.emptyList(),  /* does not have additional outputs */
      windowingStrategy,
      Collections.emptyMap(), /* does not have additional side inputs */
      options,
      displayData,
      doFnSchemaInformation,
      Collections.<String, PCollectionView<?>>emptyMap()); /* does not have side inputs */
    this.windowCoder = windowingStrategy.getWindowFn().windowCoder();
    this.keyCoder = keyCoder;
    this.reduceFn = reduceFn;
    this.prevOutputWatermark = new Watermark(Long.MIN_VALUE);
    this.inputWatermark = new Watermark(Long.MIN_VALUE);
    this.keyAndWatermarkHoldMap = new HashMap<>();
  }

  /**
   * This creates a new DoFn that groups elements by key and window.
   * @param doFn original doFn.
   * @return GroupAlsoByWindowViaWindowSetNewDoFn
   */
  @Override
  protected DoFn wrapDoFn(final DoFn doFn) {
    if (inMemoryStateInternalsFactory == null) {
      this.inMemoryStateInternalsFactory = new InMemoryStateInternalsFactory<>();
    } else {
      LOG.info("InMemoryStateInternalFactroy is already set");
    }

    if (inMemoryTimerInternalsFactory == null) {
      this.inMemoryTimerInternalsFactory = new InMemoryTimerInternalsFactory<>();
    } else {
      LOG.info("InMemoryTimerInternalsFactory is already set");
    }

    // This function performs group by key and window operation.
    return
      GroupAlsoByWindowViaWindowSetNewDoFn.create(
        getWindowingStrategy(),
        inMemoryStateInternalsFactory,
        inMemoryTimerInternalsFactory,
        null, // does not have side input.
        reduceFn,
        getOutputManager(),
        getMainOutputTag());
  }

  /** Wrapper function of output collector. */
  @Override
  OutputCollector wrapOutputCollector(final OutputCollector oc) {
    originOc = oc;
    return new GBKOutputCollector(oc);
  }

  /**
   * Invoke runner to process a single element.
   * The collected data are emitted at {@link GBKStreamingTransform#onWatermark(Watermark)}
   * @param element input data element.
   */
  @Override
  public void onData(final WindowedValue<KV<K, InputT>> element) {
      dataReceived = true;
      try {
        checkAndInvokeBundle();
        final KV<K, InputT> kv = element.getValue();
        final KeyedWorkItem<K, InputT> keyedWorkItem =
          KeyedWorkItems.elementsWorkItem(kv.getKey(),
            Collections.singletonList(element.withValue(kv.getValue())));
        getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(keyedWorkItem));
        checkAndFinishBundle();
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException("exception trigggered element " + element.toString());
      }
  }

  /**
   * Process the collected data, trigger timers, and emit outputwatermark.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   * @param triggerWatermark watermark
   */
  private void processElementsAndTriggerTimers(final Instant processingTime,
                                               final Instant synchronizedTime,
                                               final Watermark triggerWatermark) {
    triggerTimers(processingTime, synchronizedTime, triggerWatermark);
    emitOutputWatermark();
  }

  /**
   * Output watermark
   * = max(prev output watermark,
   *          min(input watermark, watermark holds)).
   */
  private void emitOutputWatermark() {
    // Find min watermark hold
    Watermark minWatermarkHold = keyAndWatermarkHoldMap.isEmpty()
      ? new Watermark(dataReceived ? Long.MIN_VALUE : Long.MAX_VALUE)
      : Collections.min(keyAndWatermarkHoldMap.values());

    Watermark outputWatermarkCandidate = new Watermark(
      Math.max(prevOutputWatermark.getTimestamp(),
        Math.min(minWatermarkHold.getTimestamp(), inputWatermark.getTimestamp())));

    while (outputWatermarkCandidate.getTimestamp() > prevOutputWatermark.getTimestamp()) {
      // progress!
      prevOutputWatermark = outputWatermarkCandidate;
      // emit watermark
      getOutputCollector().emitWatermark(outputWatermarkCandidate);
      // Remove minimum watermark holds
      if (minWatermarkHold.getTimestamp() == outputWatermarkCandidate.getTimestamp()) {
        final long minWatermarkTimestamp = minWatermarkHold.getTimestamp();
        keyAndWatermarkHoldMap.entrySet()
          .removeIf(entry -> entry.getValue().getTimestamp() == minWatermarkTimestamp);
      }

      minWatermarkHold = keyAndWatermarkHoldMap.isEmpty()
        ? new Watermark(Long.MAX_VALUE) : Collections.min(keyAndWatermarkHoldMap.values());

      outputWatermarkCandidate = new Watermark(
        Math.max(prevOutputWatermark.getTimestamp(),
          Math.min(minWatermarkHold.getTimestamp(), inputWatermark.getTimestamp())));
    }
  }

  /**
   * Trigger timers that need to be fired.
   * @param watermark watermark
   */
  @Override
  public void onWatermark(final Watermark watermark) {
    if (watermark.getTimestamp() <= inputWatermark.getTimestamp()) {
      return;
    }
    checkAndInvokeBundle();
    inputWatermark = watermark;
    // Triggering timers
    try {
      processElementsAndTriggerTimers(Instant.now(), Instant.now(), inputWatermark);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // Emit watermark to downstream operators
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
    processElementsAndTriggerTimers(
      BoundedWindow.TIMESTAMP_MAX_VALUE, BoundedWindow.TIMESTAMP_MAX_VALUE, inputWatermark);
  }

  /**
   * Trigger timers for current key.
   * When triggering, it emits the windowed data to downstream operators.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   * @param triggerWatermark watermark
   */
  private void triggerTimers(final Instant processingTime,
                            final Instant synchronizedTime,
                            final Watermark triggerWatermark) {

    inMemoryTimerInternalsFactory.setInputWatermarkTime(new Instant(triggerWatermark.getTimestamp()));
    inMemoryTimerInternalsFactory.setProcessingTime(processingTime);
    inMemoryTimerInternalsFactory.setSynchronizedProcessingTime(synchronizedTime);

    // Next timer that needs to be processed
    Iterable<Pair<K, TimerInternals.TimerData>> timers = getEligibleTimers();

    for (Pair<K, TimerInternals.TimerData> curr : timers) {
      final NemoTimerInternals timerInternals = (NemoTimerInternals)
        inMemoryTimerInternalsFactory.getTimerInternalsMap().get(curr.left());
      timerInternals.setCurrentInputWatermarkTime(new Instant(inputWatermark.getTimestamp()));
      timerInternals.setCurrentProcessingTime(processingTime);
      timerInternals.setCurrentSynchronizedProcessingTime(synchronizedTime);
      // Trigger timers and emit windowed data
      final KeyedWorkItem<K, InputT> timerWorkItem =
        KeyedWorkItems.timersWorkItem(curr.left(), Collections.singletonList(curr.right()));

      // The DoFnRunner interface requires WindowedValue,
      // but this windowed value is actually not used in the ReduceFnRunner internal.
      getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(timerWorkItem));

      inMemoryTimerInternalsFactory.removeTimer(curr);
      inMemoryTimerInternalsFactory.removeTimerForKeyIfEmpty(curr.left());
      inMemoryStateInternalsFactory.removeNamespaceForKey(
        curr.left(), curr.right().getNamespace(), curr.right().getTimestamp());
    }
    return;
  }

  /**
   * Get timers that need to be processed.
   */
  private List<Pair<K, TimerInternals.TimerData>> getEligibleTimers() {
    final List<Pair<K, TimerInternals.TimerData>> timerData = new LinkedList<>();

    while (true) {
      Pair<K, TimerInternals.TimerData> timer;
      boolean hasFired = false;

      while ((timer = inMemoryTimerInternalsFactory.getNextEventTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }

      while ((timer = inMemoryTimerInternalsFactory.getNextProcessingTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }
      while ((timer = inMemoryTimerInternalsFactory.getNextSynchronizedProcessingTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }
      if (!hasFired) {
        break;
      }
    }

    return timerData;
  }

  @Override
  public Coder<GBKState<K>> getStateCoder() {
    return new GBKStateCoder<>(keyCoder, windowCoder);
  }

  @Override
  public GBKState<K> getState() {
    return new GBKState<>(inMemoryTimerInternalsFactory,
      inMemoryStateInternalsFactory,
      prevOutputWatermark,
      keyAndWatermarkHoldMap,
      inputWatermark);
  }

  @Override
  public void setState(final GBKState<K> state) {

    if (inMemoryStateInternalsFactory == null) {
      inMemoryStateInternalsFactory = state.getStateInternalsFactory();
      inMemoryTimerInternalsFactory = state.getTimerInternalsFactory();
    } else {
      inMemoryStateInternalsFactory.setState(state.getStateInternalsFactory());
      inMemoryTimerInternalsFactory.setState(state.getTimerInternalsFactory());
    }

    inputWatermark = state.getInputWatermark();
    prevOutputWatermark = state.getPrevOutputWatermark();

    keyAndWatermarkHoldMap.clear();
    keyAndWatermarkHoldMap.putAll(state.getKeyAndWatermarkHoldMap());
  }

  /** Wrapper class for {@link OutputCollector}. */
  public class GBKOutputCollector implements OutputCollector<WindowedValue<KV<K, OutputT>>> {
    private final OutputCollector<WindowedValue<KV<K, OutputT>>> oc;

    public GBKOutputCollector(final OutputCollector oc) {
      this.oc = oc;
    }

    /** Emit output value. If value is emitted on-time, add output timestamp to watermark hold map. */
    @Override
    public void emit(final WindowedValue<KV<K, OutputT>> output) {
      // The watermark advances only in ON_TIME
      if (output.getPane().getTiming().equals(PaneInfo.Timing.ON_TIME)) {
        KV<K, OutputT> value = output.getValue();
        final K key = value.getKey();
        final NemoTimerInternals timerInternals = (NemoTimerInternals)
          inMemoryTimerInternalsFactory.timerInternalsForKey(key);
        // Add the output timestamp to the watermark hold of each key.
        // +1 to the output timestamp because if the window is [0-5000), the timestamp is 4999.
          keyAndWatermarkHoldMap.put(key,
            new Watermark(output.getTimestamp().getMillis() + 1));
          timerInternals.setCurrentOutputWatermarkTime(new Instant(output.getTimestamp().getMillis() + 1));
      }
      oc.emit(output);
    }

    /** Emit watermark. */
    @Override
    public void emitWatermark(final Watermark watermark) {
      oc.emitWatermark(watermark);
    }

    /** Emit output value to {@param dstVertexId}. */
    @Override
    public <T> void emit(final String dstVertexId, final T output) {
      oc.emit(dstVertexId, output);
    }
  }
}
