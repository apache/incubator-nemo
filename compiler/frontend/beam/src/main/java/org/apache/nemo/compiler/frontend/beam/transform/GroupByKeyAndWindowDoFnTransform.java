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
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.common.GBKLambdaEvent;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Groups elements according to key and window.
 * @param <K> key type.
 * @param <InputT> input type.
 */
public final class GroupByKeyAndWindowDoFnTransform<K, InputT>
  extends AbstractDoFnTransform<KV<K, InputT>, KeyedWorkItem<K, InputT>, KV<K, Iterable<InputT>>> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKeyAndWindowDoFnTransform.class.getName());

  private final SystemReduceFn reduceFn;
  //private final Map<K, List<WindowedValue<InputT>>> keyToValues;
  private transient InMemoryTimerInternalsFactory inMemoryTimerInternalsFactory;
  private transient InMemoryStateInternalsFactory inMemoryStateInternalsFactory;
  private Watermark prevOutputWatermark;
  private final Map<K, Watermark> keyAndWatermarkHoldMap;
  private final WindowingStrategy windowingStrategy;
  private Watermark inputWatermark;

  int numProcessedData = 0;

  /**
   * GroupByKey constructor.
   */
  public GroupByKeyAndWindowDoFnTransform(final Map<TupleTag<?>, Coder<?>> outputCoders,
                                          final TupleTag<KV<K, Iterable<InputT>>> mainOutputTag,
                                          final WindowingStrategy<?, ?> windowingStrategy,
                                          final PipelineOptions options,
                                          final SystemReduceFn reduceFn,
                                          final DisplayData displayData) {
    super(null, /* doFn */
      null, /* inputCoder */
      outputCoders,
      mainOutputTag,
      Collections.emptyList(),  /*  GBK does not have additional outputs */
      windowingStrategy,
      Collections.emptyMap(), /*  GBK does not have additional side inputs */
      options,
      displayData);
    //this.keyToValues = new HashMap<>();
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
    final Map<K, StateAndTimerForKey> map = new HashMap<>();
    this.inMemoryStateInternalsFactory = new InMemoryStateInternalsFactory(map);
    this.inMemoryTimerInternalsFactory = new InMemoryTimerInternalsFactory();


    // This function performs group by key and window operation
    return
      GroupAlsoByWindowViaWindowSetNewDoFn.create(
        getWindowingStrategy(),
        inMemoryStateInternalsFactory,
        inMemoryTimerInternalsFactory,
        null, // GBK has no sideinput.
        reduceFn,
        getOutputManager(),
        getMainOutputTag());
  }

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
    // drop late data
    if (element.getTimestamp().isAfter(inputWatermark.getTimestamp())) {
      checkAndInvokeBundle();
      // We can call Beam's DoFnRunner#processElement here,
      // but it may generate some overheads if we call the method for each data.
      // The `processElement` requires a `Iterator` of data, so we emit the buffered data every watermark.
      // TODO #250: But, this approach can delay the event processing in streaming,
      // TODO #250: if the watermark is not triggered for a long time.

      final KV<K, InputT> kv = element.getValue();
      //keyToValues.putIfAbsent(kv.getKey(), new ArrayList<>());
      //keyToValues.get(kv.getKey()).add(element.withValue(kv.getValue()));
      final KeyedWorkItem<K, InputT> keyedWorkItem =
        KeyedWorkItems.elementsWorkItem(kv.getKey(),
          Collections.singletonList(element.withValue(kv.getValue())));
      numProcessedData += 1;
      // The DoFnRunner interface requires WindowedValue,
      // but this windowed value is actually not used in the ReduceFnRunner internal.
      getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(keyedWorkItem));
      checkAndFinishBundle();
    }
  }

  /**
   * Process the collected data and trigger timers.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   */
  private void processElementsAndTriggerTimers(final Instant processingTime,
                                               final Instant synchronizedTime) {
    final long st = System.currentTimeMillis();
    int numOfProcessedKeys = 0;

    /*
    final Iterator<Map.Entry<K, List<WindowedValue<InputT>>>> iterator = keyToValues.entrySet().iterator();
    while (iterator.hasNext()) {
      final Map.Entry<K, List<WindowedValue<InputT>>> entry = iterator.next();
      final K key = entry.getKey();
      final List<WindowedValue<InputT>> values = entry.getValue();

      // for each key
      // Process elements
      if (!values.isEmpty()) {
        final KeyedWorkItem<K, InputT> keyedWorkItem =
          KeyedWorkItems.elementsWorkItem(key, values);
        // The DoFnRunner interface requires WindowedValue,
        // but this windowed value is actually not used in the ReduceFnRunner internal.
        getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(keyedWorkItem));
        // Remove values
        numOfProcessedKeys += 1;
      }

      iterator.remove();
    }
    */

    final long e = System.currentTimeMillis();

    // Trigger timers

    final int triggeredKeys = triggerTimers(processingTime, synchronizedTime);
    final long triggerTime = System.currentTimeMillis();

//    LOG.info("{} time to elem: {} trigger: {} triggered: {} triggeredKey: {}", getContext().getIRVertex().getId(),
//      (e-st), (triggerTime - st), triggeredKeys > 0, triggeredKeys);
  }

  /**
   * Output watermark
   * = max(prev output watermark,
   *          min(input watermark, watermark holds)).
   */
  private void emitOutputWatermark() {
    // Find min watermark hold
    final Watermark minWatermarkHold = keyAndWatermarkHoldMap.isEmpty()
      ? new Watermark(Long.MAX_VALUE) // set this to MAX, in order to just use the input watermark.
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
      // emit watermark

      getOutputCollector().emitWatermark(outputWatermarkCandidate);
      // Remove minimum watermark holds
      if (minWatermarkHold.getTimestamp() == outputWatermarkCandidate.getTimestamp()) {
        keyAndWatermarkHoldMap.entrySet()
          .removeIf(entry -> entry.getValue().getTimestamp() == minWatermarkHold.getTimestamp());
      }
    }
  }

  @Override
  public void onWatermark(final Watermark watermark) {
    checkAndInvokeBundle();
    inputWatermark = watermark;

    final long st = System.currentTimeMillis();
    processElementsAndTriggerTimers(Instant.now(), Instant.now());
    // Emit watermark to downstream operators

    emitOutputWatermark();
    final long et1 = System.currentTimeMillis();
    checkAndFinishBundle();

    final long et = System.currentTimeMillis();
//    LOG.info("{}/{} latency {}, watermark: {}, emitOutputWatermarkTime: {}",
//      getContext().getIRVertex().getId(), Thread.currentThread().getId(), (et-st),
//      new Instant(watermark.getTimestamp()), (et - et1));
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
   * Trigger times for current key.
   * When triggering, it emits the windowed data to downstream operators.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   */
  private int triggerTimers(final Instant processingTime,
                            final Instant synchronizedTime) {

    inMemoryTimerInternalsFactory.inputWatermarkTime = new Instant(inputWatermark.getTimestamp());
    inMemoryTimerInternalsFactory.processingTime = processingTime;
    inMemoryTimerInternalsFactory.synchronizedProcessingTime = synchronizedTime;

    final long st = System.currentTimeMillis();
    final List<Pair<K, TimerInternals.TimerData>> timers = getEligibleTimers();

//    LOG.info("{}/{} GetEligibleTimer time: {}", getContext().getIRVertex().getId(),
//      Thread.currentThread().getId(), (System.currentTimeMillis() - st));

    // TODO: send start event
    if (!timers.isEmpty()) {
      final WindowedValue<KV<K, Iterable<InputT>>> startEvent =
        WindowedValue.valueInGlobalWindow(
          KV.of((K) new GBKLambdaEvent(GBKLambdaEvent.Type.START, new Integer(timers.size())),
            Collections.emptyList()));
      getOutputCollector().emit(startEvent);
    }
    // TODO: end

    for (final Pair<K, TimerInternals.TimerData> timer : timers) {
      final NemoTimerInternals timerInternals =
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
    }

    // TODO: send end event
    if (!timers.isEmpty()) {
      final WindowedValue<KV<K, Iterable<InputT>>> endEvent =
        WindowedValue.valueInGlobalWindow(
          KV.of((K) new GBKLambdaEvent(GBKLambdaEvent.Type.END, new Integer(timers.size())),
            Collections.emptyList()));
      getOutputCollector().emit(endEvent);
    }

    return timers.size();
  }

  /**
   * Get timer data.
   */
  private List<Pair<K, TimerInternals.TimerData>> getEligibleTimers() {
    final List<Pair<K, TimerInternals.TimerData>> timerData = new LinkedList<>();

    while (true) {
      Pair<K, TimerInternals.TimerData> timer;
      boolean hasFired = false;

      while ((timer = inMemoryTimerInternalsFactory.removeNextEventTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }

      while ((timer = inMemoryTimerInternalsFactory.removeNextProcessingTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }
      while ((timer = inMemoryTimerInternalsFactory.removeNextSynchronizedProcessingTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }
      if (!hasFired) {
        break;
      }
    }

    return timerData;
  }

  /**
   * State and timer internal.
   */
  final class StateAndTimerForKey {
    private StateInternals stateInternals;
    private TimerInternals timerInternals;

    StateAndTimerForKey(final StateInternals stateInternals,
                        final TimerInternals timerInternals) {
      this.stateInternals = stateInternals;
      this.timerInternals = timerInternals;
    }
  }

  /**
   * InMemoryStateInternalsFactory.
   */
  final class InMemoryStateInternalsFactory implements StateInternalsFactory<K> {
    private final Map<K, StateAndTimerForKey> map;

    InMemoryStateInternalsFactory(final Map<K, StateAndTimerForKey> map) {
      this.map = map;
    }

    @Override
    public StateInternals stateInternalsForKey(final K key) {
      map.putIfAbsent(key, new StateAndTimerForKey(InMemoryStateInternals.forKey(key), null));
      final StateAndTimerForKey stateAndTimerForKey = map.get(key);
      if (stateAndTimerForKey.stateInternals == null) {
        stateAndTimerForKey.stateInternals = InMemoryStateInternals.forKey(key);
      }
      return stateAndTimerForKey.stateInternals;
    }
  }

  /**
   * InMemoryTimerInternalsFactory.
   */
  final class InMemoryTimerInternalsFactory implements TimerInternalsFactory<K> {

    /** Pending input watermark timers, in timestamp order. */
    private final NavigableSet<Pair<K, TimerInternals.TimerData>> watermarkTimers;
    /** Pending processing time timers, in timestamp order. */
    private final NavigableSet<Pair<K, TimerInternals.TimerData>> processingTimers;
    /** Pending synchronized processing time timers, in timestamp order. */
    private final NavigableSet<Pair<K, TimerInternals.TimerData>> synchronizedProcessingTimers;

    /** Current input watermark. */
    private Instant inputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;


    /** Current processing time. */
    private Instant processingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

    /** Current synchronized processing time. */
    private Instant synchronizedProcessingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

    private final Map<K, NemoTimerInternals> timerInternalsMap = new HashMap<>();

    private final Comparator<Pair<K, TimerInternals.TimerData>> comparator = (o1, o2) -> {
      final int comp = o1.right().compareTo(o2.right());
      if (comp == 0) {
        if (o1.left() == null) {
          return 0;
        } else {
          return o1.left().toString().compareTo(o2.left().toString());
        }
      } else {
        return comp;
      }
    };

    InMemoryTimerInternalsFactory() {
      this.watermarkTimers = new TreeSet<>(comparator);
      this.processingTimers = new TreeSet<>(comparator);
      this.synchronizedProcessingTimers = new TreeSet<>(comparator);
    }

    @Override
    public TimerInternals timerInternalsForKey(final K key) {
      if (timerInternalsMap.get(key) != null) {
        return timerInternalsMap.get(key);
      } else {
        final NemoTimerInternals internal =  new NemoTimerInternals<>(key,
          watermarkTimers,
          processingTimers,
          synchronizedProcessingTimers);
        timerInternalsMap.put(key, internal);
        return internal;
      }
    }


    /** Returns the next eligible event time timer, if none returns null. */
    @Nullable
    public Pair<K, TimerInternals.TimerData> removeNextEventTimer() {
      Pair<K, TimerInternals.TimerData> timer = removeNextTimer(inputWatermarkTime, TimeDomain.EVENT_TIME);
      if (timer != null) {
        WindowTracing.trace(
          "{}.removeNextEventTimer: firing {} at {}",
          getClass().getSimpleName(),
          timer,
          inputWatermarkTime);
      }
      return timer;
    }

    /** Returns the next eligible processing time timer, if none returns null. */
    @Nullable
    public Pair<K, TimerInternals.TimerData> removeNextProcessingTimer() {
      Pair<K, TimerInternals.TimerData> timer = removeNextTimer(processingTime, TimeDomain.PROCESSING_TIME);
      if (timer != null) {
        WindowTracing.trace(
          "{}.removeNextProcessingTimer: firing {} at {}",
          getClass().getSimpleName(),
          timer,
          processingTime);
      }
      return timer;
    }

    /** Returns the next eligible synchronized processing time timer, if none returns null. */
    @Nullable
    public Pair<K, TimerInternals.TimerData> removeNextSynchronizedProcessingTimer() {
      Pair<K, TimerInternals.TimerData> timer =
        removeNextTimer(synchronizedProcessingTime, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
      if (timer != null) {
        WindowTracing.trace(
          "{}.removeNextSynchronizedProcessingTimer: firing {} at {}",
          getClass().getSimpleName(),
          timer,
          synchronizedProcessingTime);
      }
      return timer;
    }


    @Nullable
    private Pair<K, TimerInternals.TimerData> removeNextTimer(Instant currentTime, TimeDomain domain) {
      NavigableSet<Pair<K, TimerInternals.TimerData>> timers = timersForDomain(domain);

      if (!timers.isEmpty() && currentTime.isAfter(timers.first().right().getTimestamp())) {
        Pair<K, TimerInternals.TimerData> timer = timers.pollFirst();
        timerInternalsMap.get(timer.left()).deleteTimer(timer.right());
        return timer;
      } else {
        return null;
      }
    }

    private NavigableSet<Pair<K, TimerInternals.TimerData>> timersForDomain(TimeDomain domain) {
      switch (domain) {
        case EVENT_TIME:
          return watermarkTimers;
        case PROCESSING_TIME:
          return processingTimers;
        case SYNCHRONIZED_PROCESSING_TIME:
          return synchronizedProcessingTimers;
        default:
          throw new IllegalArgumentException("Unexpected time domain: " + domain);
      }
    }

  }

  /**
   * This class wraps the output collector to track the watermark hold of each key.
   */
  final class GBKWOutputCollector implements OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> {
    private final OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> outputCollector;
    GBKWOutputCollector(final OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> outputCollector) {
      this.outputCollector = outputCollector;
    }

    @Override
    public void emit(final WindowedValue<KV<K, Iterable<InputT>>> output) {

      // TODO: remove
      if (output.getValue().getKey() instanceof  GBKLambdaEvent) {
        //outputCollector.emit(output); // this commnet should be remove for query 8
        return;
      }
      // TODO: remove


      // The watermark advances only in ON_TIME
      if (output.getPane().getTiming().equals(PaneInfo.Timing.ON_TIME)) {
        final K key = output.getValue().getKey();
        final NemoTimerInternals timerInternals = (NemoTimerInternals)
          inMemoryTimerInternalsFactory.timerInternalsForKey(key);
        keyAndWatermarkHoldMap.put(key,
          // adds the output timestamp to the watermark hold of each key
          // +1 to the output timestamp because if the window is [0-5000), the timestamp is 4999
          new Watermark(output.getTimestamp().getMillis() + 1));
        timerInternals.setCurrentOutputWatermarkTime(new Instant(output.getTimestamp().getMillis() + 1));
      }
      outputCollector.emit(output);
    }

    @Override
    public void emitWatermark(final Watermark watermark) {

      outputCollector.emitWatermark(watermark);
    }
    @Override
    public <T> void emit(final String dstVertexId, final T output) {
      outputCollector.emit(dstVertexId, output);
    }
  }
}
