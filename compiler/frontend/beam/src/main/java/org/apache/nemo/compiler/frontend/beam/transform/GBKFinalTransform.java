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

import com.google.common.io.CountingInputStream;
import com.google.common.io.CountingOutputStream;
import org.apache.beam.runners.core.*;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.Pair;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.common.ir.AbstractOutputCollector;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.transform.coders.GBKFinalStateCoder;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.nemo.common.Util.prettyPrintMap;

/**
 * Groups elements according to key and window.
 * @param <K> key type.
 * @param <InputT> input type.
 */
public final class GBKFinalTransform<K, InputT>
  extends AbstractDoFnTransform<KV<K, InputT>, KeyedWorkItem<K, InputT>, KV<K, Iterable<InputT>>> implements StatefulTransform<GBKFinalState<K>> {
  private static final Logger LOG = LoggerFactory.getLogger(GBKFinalTransform.class.getName());

  public final SystemReduceFn reduceFn; //private final Map<K, List<WindowedValue<InputT>>> keyToValues;
  private transient InMemoryTimerInternalsFactory<K> inMemoryTimerInternalsFactory;
  private transient InMemoryStateInternalsFactory<K> inMemoryStateInternalsFactory;
  private Watermark prevOutputWatermark;
  private final Map<K, Watermark> keyAndWatermarkHoldMap;
  private Watermark inputWatermark;

  int numProcessedData = 0;

  private transient OutputCollector originOc;

  public final Coder<K> keyCoder;
  public final Coder windowCoder;

  private transient Map<K, Integer> keyCountMap;
  private final boolean partial;

  /**
   * GroupByKey constructor.
   */
  public GBKFinalTransform(final Coder inputcoder,
                           final Coder<K> keyCoder,
                           final Map<TupleTag<?>, Coder<?>> outputCoders,
                           final TupleTag<KV<K, Iterable<InputT>>> mainOutputTag,
                           final WindowingStrategy<?, ?> windowingStrategy,
                           final PipelineOptions options,
                           final SystemReduceFn reduceFn,
                           final DisplayData displayData,
                           final boolean partial) {
    super(null, /* doFn */
      inputcoder, /* inputCoder */
      outputCoders,
      mainOutputTag,
      Collections.emptyList(),  /*  GBK does not have additional outputs */
      windowingStrategy,
      Collections.emptyMap(), /*  GBK does not have additional side inputs */
      options,
      displayData);
    this.windowCoder = windowingStrategy.getWindowFn().windowCoder();
    //this.keyToValues = new HashMap<>();
    this.keyCoder = keyCoder;
    this.reduceFn = reduceFn;
    LOG.info("Reduce function {}", reduceFn.getClass());
    this.prevOutputWatermark = new Watermark(Long.MIN_VALUE);
    this.inputWatermark = new Watermark(Long.MIN_VALUE);
    this.keyAndWatermarkHoldMap = new HashMap<>();
    this.partial = partial;
  }

  @Override
  public int getNumKeys() {
    //LOG.info("TimerInteranslKey: {} StateInternalsKey: {}", inMemoryTimerInternalsFactory.getNumKey(),
    //  inMemoryStateInternalsFactory.getNumKeys());
    return inMemoryTimerInternalsFactory.getNumKey();
  }

  @Override
  public boolean isGBKPartialTransform() {
    return partial;
  }

  @Override
  public void checkpoint() {
    final long st = System.currentTimeMillis();
    final StateStore stateStore = getContext().getStateStore();
    final OutputStream os = stateStore.getOutputStream(getContext().getTaskId() + "-" + partial);
    final CountingOutputStream cos = new CountingOutputStream(os);
    final GBKFinalStateCoder<K> coder = new GBKFinalStateCoder<>(keyCoder, windowCoder);

    try {
      coder.encode(new GBKFinalState<>(inMemoryTimerInternalsFactory,
        inMemoryStateInternalsFactory,
        prevOutputWatermark,
        keyAndWatermarkHoldMap,
        inputWatermark), cos);

      cos.close();

      final long et = System.currentTimeMillis();
      LOG.info("State encoding byte {}, time {} Checkpoint timer state size {}, {} for {} in {}",
        cos.getCount(),
        et - st,
        inMemoryTimerInternalsFactory.getNumKey(),
        inMemoryStateInternalsFactory.stateInternalMap.size(),
        getContext().getTaskId(),
        getContext().getExecutorId());

    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private StateStore stateStore;
  private String taskId;

  @Override
  public void restore() {
    if (stateStore.containsState(getContext().getTaskId() + "-" + partial)) {
      final long st = System.currentTimeMillis();
      final InputStream is = stateStore.getStateStream(getContext().getTaskId() + "-" + partial);
      final CountingInputStream countingInputStream = new CountingInputStream(is);
      final GBKFinalStateCoder<K> coder = new GBKFinalStateCoder<>(keyCoder, windowCoder);
      final GBKFinalState<K> state;
      try {
        state = coder.decode(countingInputStream);
        countingInputStream.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      // TODO set ...
      if (inMemoryStateInternalsFactory == null) {
        inMemoryStateInternalsFactory = state.stateInternalsFactory;
        inMemoryTimerInternalsFactory = state.timerInternalsFactory;
      } else {
        inMemoryStateInternalsFactory.setState(state.stateInternalsFactory);
        inMemoryTimerInternalsFactory.setState(state.timerInternalsFactory);
      }

      final long et = System.currentTimeMillis();
      LOG.info("State decoding byte {}, time {}, numkey {}, Restored size {} for {} in {}",
        countingInputStream.getCount(),
        et - st,
        inMemoryTimerInternalsFactory.getNumKey(),
        inMemoryStateInternalsFactory.stateInternalMap.size(),
        getContext().getTaskId(),
        getContext().getExecutorId());

      inputWatermark = state.inputWatermark;
      prevOutputWatermark = state.prevOutputWatermark;

      keyAndWatermarkHoldMap.clear();
      keyAndWatermarkHoldMap.putAll(state.keyAndWatermarkHoldMap);

    } else {

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
    }
  }

  /**
   * This creates a new DoFn that groups elements by key and window.
   * @param doFn original doFn.
   * @return GroupAlsoByWindowViaWindowSetNewDoFn
   */
  @Override
  protected DoFn wrapDoFn(final DoFn doFn) {
    final Map<StateTag, Pair<State, Coder>> map = new ConcurrentHashMap<>();
    keyCountMap = new HashMap<>();

    taskId = getContext().getTaskId();
    stateStore = getContext().getStateStore();

    if (stateStore.containsState(getContext().getTaskId() + "-" + partial)) {
      final long st = System.currentTimeMillis();
      final InputStream is = stateStore.getStateStream(getContext().getTaskId() + "-" + partial);
      final CountingInputStream countingInputStream = new CountingInputStream(is);
      final GBKFinalStateCoder<K> coder = new GBKFinalStateCoder<>(keyCoder, windowCoder);
      final GBKFinalState<K> state;
      try {
        state = coder.decode(countingInputStream);
        countingInputStream.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      // TODO set ...
      if (inMemoryStateInternalsFactory == null) {
        inMemoryStateInternalsFactory = state.stateInternalsFactory;
        inMemoryTimerInternalsFactory = state.timerInternalsFactory;
      } else {
        inMemoryStateInternalsFactory.setState(state.stateInternalsFactory);
        inMemoryTimerInternalsFactory.setState(state.timerInternalsFactory);
      }

      final long et = System.currentTimeMillis();
      LOG.info("State decoding byte {}, time {}, numKey {}, Restored size {} for {} in {}",
        countingInputStream.getCount(),
        et - st,
        inMemoryTimerInternalsFactory.getNumKey(),
        inMemoryStateInternalsFactory.stateInternalMap.size(),
        getContext().getTaskId(),
        getContext().getExecutorId());


      inputWatermark = state.inputWatermark;
      prevOutputWatermark = state.prevOutputWatermark;

      keyAndWatermarkHoldMap.clear();
      keyAndWatermarkHoldMap.putAll(state.keyAndWatermarkHoldMap);

    } else {

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
    }

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
    originOc = oc;
    return new GBKWOutputCollector(oc);
  }


  /**
   * It collects data for each key.
   * The collected data are emitted at {@link GBKFinalTransform#onWatermark(Watermark)}
   * @param element data element
   */
  @Override
  public void onData(final WindowedValue<KV<K, InputT>> element) {
    // LOG.info("Final input receive at {}, timestamp: {}, inputWatermark: {}",
    //  getContext().getTaskId(),
    //  element.getTimestamp(), new Instant(inputWatermark.getTimestamp()));

    if (keyCountMap.containsKey(element.getValue().getKey())) {
      keyCountMap.put(element.getValue().getKey(), keyCountMap.get(element.getValue().getKey()) + 1);
    } else {
      keyCountMap.put(element.getValue().getKey(), 1);
    }

    // drop late data
    try {
      if (element.getTimestamp().isAfter(inputWatermark.getTimestamp())) {

        //LOG.info("Final input!!: {}", element);
        // We can call Beam's DoFnRunner#processElement here,
        // but it may generate some overheads if we call the method for each data.
        // The `processElement` requires a `Iterator` of data, so we emit the buffered data every watermark.
        // TODO #250: But, this approach can delay the event processing in streaming,
        // TODO #250: if the watermark is not triggered for a long time.
        final KV<K, InputT> kv = element.getValue();
        checkAndInvokeBundle();
        final KeyedWorkItem<K, InputT> keyedWorkItem =
          KeyedWorkItems.elementsWorkItem(kv.getKey(),
            Collections.singletonList(element.withValue(kv.getValue())));
        numProcessedData += 1;

        // LOG.info("Final input process: {} key {}, time {}", getContext().getTaskId(),
        //  kv.getKey(), new Instant(element.getTimestamp()));

        // The DoFnRunner interface requires WindowedValue,
        // but this windowed value is actually not used in the ReduceFnRunner internal.
        getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(keyedWorkItem));
        checkAndFinishBundle();
      } else {
        LOG.info("Late input at {}, time {}", getContext().getTaskId(), new Instant(element.getTimestamp()));
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException("exception trigggered element " + element.toString());
    }
  }

  /**
   * Process the collected data and trigger timers.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   */
  private void processElementsAndTriggerTimers(final Instant processingTime,
                                               final Instant synchronizedTime,
                                               final Watermark triggerWatermark) {

    // Trigger timers
    final int triggeredKeys = triggerTimers(processingTime, synchronizedTime, triggerWatermark);
    final long triggerTime = System.currentTimeMillis();

    if (triggeredKeys > 0) {
      // LOG.info("{} time to elem: triggeredKey: {}, numKeys: {}", getContext().getTaskId(),
      //  triggeredKeys, inMemoryTimerInternalsFactory.watermarkTimers.size());
    }
  }

  /**
   * Output watermark
   * = max(prev output watermark,
   *          min(input watermark, watermark holds)).
   */
  private void emitOutputWatermark() {
    // Find min watermark hold
 Watermark minWatermarkHold = keyAndWatermarkHoldMap.isEmpty()
      ? new Watermark(Long.MAX_VALUE) : Collections.min(keyAndWatermarkHoldMap.values());

    Watermark outputWatermarkCandidate = new Watermark(
      Math.max(prevOutputWatermark.getTimestamp(),
        Math.min(minWatermarkHold.getTimestamp(), inputWatermark.getTimestamp())));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Watermark hold: {}, "
        + "inputWatermark: {}, outputWatermark: {}", minWatermarkHold, inputWatermark, prevOutputWatermark);
    }

    /*
    LOG.info("MinWatermarkHold: {}, OutputWatermarkCandidate: {}, PrevOutputWatermark: {}, inputWatermark: {}, " +
        "keyAndWatermarkHoldMap: {}, at {}",
      new Instant(minWatermarkHold.getTimestamp()), new Instant(outputWatermarkCandidate.getTimestamp()),
      new Instant(prevOutputWatermark.getTimestamp()),
      new Instant(inputWatermark.getTimestamp()),
      keyAndWatermarkHoldMap,
      getContext().getTaskId());
      */

    // keep going if the watermark increases
    while (outputWatermarkCandidate.getTimestamp() > prevOutputWatermark.getTimestamp()) {
      // progress!
      prevOutputWatermark = outputWatermarkCandidate;
      // emit watermark

      //LOG.info("Emit watermark at GBKW: {}", outputWatermarkCandidate);
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

      /*
      LOG.info("MinWatermarkHold: {}, OutputWatermarkCandidate: {}, PrevOutputWatermark: {}, inputWatermark: {}, " +
          "keyAndWatermarkHoldMap: {}, at {}",
        new Instant(minWatermarkHold.getTimestamp()), new Instant(outputWatermarkCandidate.getTimestamp()),
        new Instant(prevOutputWatermark.getTimestamp()),
        new Instant(inputWatermark.getTimestamp()),
        keyAndWatermarkHoldMap,
        getContext().getTaskId());
        */
    }
  }

  @Override
  public void onWatermark(final Watermark watermark) {

    if (watermark.getTimestamp() <= inputWatermark.getTimestamp()) {
      LOG.info("Task {} Input watermark {} is before the prev watermark: {}",
        getContext().getTaskId(),
        new Instant(watermark.getTimestamp()),
        new Instant(inputWatermark.getTimestamp()));
      return;
    }

    LOG.info("Final watermark receive at {}:  {}", getContext().getTaskId(), new Instant(watermark.getTimestamp()));

    //LOG.info("Before bundle {} at {}", new Instant(watermark.getTimestamp()), getContext().getTaskId());
    checkAndInvokeBundle();
    //LOG.info("After bundle {} at {}", new Instant(watermark.getTimestamp()), getContext().getTaskId());
    inputWatermark = watermark;

    final long st = System.currentTimeMillis();
    try {
      processElementsAndTriggerTimers(Instant.now(), Instant.now(), inputWatermark);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // Emit watermark to downstream operators
   // LOG.info("After trigger at {} / {}", new Instant(watermark.getTimestamp()), getContext().getTaskId());

    emitOutputWatermark();
    //LOG.info("After emitwatermark at {} / {}", new Instant(watermark.getTimestamp()), getContext().getTaskId());

    final long et1 = System.currentTimeMillis();
    checkAndFinishBundle();

    final long et = System.currentTimeMillis();
//    LOG.info("{}/{} latency {}, watermark: {}, emitOutputWatermarkTime: {}",
//      getContext().getIRVertex().getId(), Thread.currentThread().getId(), (et-st),
//      new Instant(watermark.getInputTimestamp()), (et - et1));
  }

  /**
   * This advances the input watermark and processing time to the timestamp max value
   * in order to emit all data.
   */
  @Override
  protected void beforeClose() {
    // Finish any pending windows by advancing the input watermark to infinity.
    inputWatermark = new Watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
    processElementsAndTriggerTimers(BoundedWindow.TIMESTAMP_MAX_VALUE, BoundedWindow.TIMESTAMP_MAX_VALUE, inputWatermark);
  }


  private long prevLoggingTime = System.currentTimeMillis();
  /**
   * Trigger times for current key.
   * When triggering, it emits the windowed data to downstream operators.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   */
  private int triggerTimers(final Instant processingTime,
                            final Instant synchronizedTime,
                            final Watermark triggerWatermark) {

    inMemoryTimerInternalsFactory.inputWatermarkTime = new Instant(triggerWatermark.getTimestamp());
    inMemoryTimerInternalsFactory.processingTime = processingTime;
    inMemoryTimerInternalsFactory.synchronizedProcessingTime = synchronizedTime;

    //LOG.info("Triggering timers... {}/{}", inMemoryTimerInternalsFactory.hashCode(),
    //  inMemoryTimerInternalsFactory);

    final long st = System.currentTimeMillis();
    final List<Pair<K, TimerInternals.TimerData>> timers = getEligibleTimers();


//    LOG.info("{}/{} GetEligibleTimer time: {}", getContext().getIRVertex().getId(),
//      Thread.currentThread().getId(), (System.currentTimeMillis() - st));

    // TODO: send start event
    /*
    if (!timers.isEmpty()) {
      final WindowedValue<KV<K, Iterable<InputT>>> startEvent =
        WindowedValue.valueInGlobalWindow(
          KV.of((K) new GBKLambdaEvent(GBKLambdaEvent.Type.START, new Integer(timers.size())),
            Collections.emptyList()));
      getOutputCollector().emit(startEvent);
    }
    */
    // TODO: end


//    if (inMemoryTimerInternalsFactory.watermarkTimers.isEmpty()) {
//      LOG.info("{} time to elem: triggeredKey: {}, numKeys: {}, first: {}", getContext().getTaskId(),
//        timers.size(), inMemoryTimerInternalsFactory.watermarkTimers.size(), "empty");
//    } else {
//      LOG.info("{} time to elem: triggeredKey: {}, numKeys: {}, first: {}", getContext().getTaskId(),
//        timers.size(), inMemoryTimerInternalsFactory.watermarkTimers.size(),  inMemoryTimerInternalsFactory.watermarkTimers.first());
//    }


    for (final Pair<K, TimerInternals.TimerData> timer : timers) {
      final NemoTimerInternals timerInternals =
        (NemoTimerInternals) inMemoryTimerInternalsFactory.timerInternalsForKey(timer.left());
      timerInternals.setCurrentInputWatermarkTime(new Instant(triggerWatermark.getTimestamp()));
      timerInternals.setCurrentProcessingTime(processingTime);
      timerInternals.setCurrentSynchronizedProcessingTime(synchronizedTime);

      // Trigger timers and emit windowed data
      final KeyedWorkItem<K, InputT> timerWorkItem =
        KeyedWorkItems.timersWorkItem(timer.left(), Collections.singletonList(timer.right()));

      // The DoFnRunner interface requires WindowedValue,
      // but this windowed value is actually not used in the ReduceFnRunner internal.
      getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(timerWorkItem));


      // Remove states
      inMemoryTimerInternalsFactory.removeTimerForKeyIfEmpty(timer.left());
      inMemoryStateInternalsFactory.removeNamespaceForKey(timer.left(), timer.right().getNamespace(), timer.right().getTimestamp());

      /*
      timerInternals.decrementRegisteredTimer();
      if (!timerInternals.hasTimer()) {
        LOG.info("Remove key: {}", timer.left());
        inMemoryTimerInternalsFactory.timerInternalsMap.remove(timer.left());
      }
      */
    }

    /*
    if (System.currentTimeMillis() - prevLoggingTime >= 1000) {
      prevLoggingTime = System.currentTimeMillis();
      LOG.info("Timers {}, {}", new Instant(triggerWatermark.getTimestamp()), inMemoryTimerInternalsFactory.watermarkTimers);
    }
    */



    // TODO: send end event
    /*
    if (!timers.isEmpty()) {
      final WindowedValue<KV<K, Iterable<InputT>>> endEvent =
        WindowedValue.valueInGlobalWindow(
          KV.of((K) new GBKLambdaEvent(GBKLambdaEvent.Type.END, new Integer(timers.size())),
            Collections.emptyList()));
      getOutputCollector().emit(endEvent);
    }
    */

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

  @Override
  public Coder<GBKFinalState<K>> getStateCoder() {
    return new GBKFinalStateCoder<>(keyCoder, windowCoder);
  }

  @Override
  public GBKFinalState<K> getState() {
    return new GBKFinalState<>(inMemoryTimerInternalsFactory,
      inMemoryStateInternalsFactory,
      prevOutputWatermark,
      keyAndWatermarkHoldMap,
      inputWatermark);
  }

  @Override
  public void setState(GBKFinalState<K> state) {
    //LOG.info("Set state {} at {}", state, this);

    if (inMemoryStateInternalsFactory == null) {
      inMemoryStateInternalsFactory = state.stateInternalsFactory;
      inMemoryTimerInternalsFactory = state.timerInternalsFactory;
    } else {
      inMemoryStateInternalsFactory.setState(state.stateInternalsFactory);
      inMemoryTimerInternalsFactory.setState(state.timerInternalsFactory);
    }

    inputWatermark = state.inputWatermark;
    prevOutputWatermark = state.prevOutputWatermark;

    keyAndWatermarkHoldMap.clear();
    keyAndWatermarkHoldMap.putAll(state.keyAndWatermarkHoldMap);
  }


  /**
   * This class wraps the output collector to track the watermark hold of each key.
   */
  final class GBKWOutputCollector extends AbstractOutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> {
    private final OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> outputCollector;
    GBKWOutputCollector(final OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> outputCollector) {
      this.outputCollector = outputCollector;
    }

    @Override
    public void emit(final WindowedValue<KV<K, Iterable<InputT>>> output) {

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

      // LOG.info("Emitting output at {}: key {}", getContext().getTaskId(),  output.getValue().getKey());

      originOc.setInputTimestamp(output.getTimestamp().getMillis());
      outputCollector.emit(output);
    }

    @Override
    public void emitWatermark(final Watermark watermark) {

      //if (RuntimeIdManager.getStageIdFromTaskId(getContext().getTaskId()).equals("Stage2")) {
       // LOG.info("Emit watermark in final: {}, {} / {}", new Instant(watermark.getTimestamp()),
       //   watermark.getTimestamp(), getContext().getTaskId());
      //}

      outputCollector.emitWatermark(watermark);
    }
    @Override
    public <T> void emit(final String dstVertexId, final T output) {
      outputCollector.emit(dstVertexId, output);
    }
  }
}
