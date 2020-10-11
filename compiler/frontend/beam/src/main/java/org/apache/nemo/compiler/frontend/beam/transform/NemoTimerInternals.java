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


import com.google.common.base.MoreObjects;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.nemo.common.Pair;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Keep track of timer for a specific key.
 * @param <K> key type
 */
public class NemoTimerInternals<K> implements TimerInternals {
  private static final Logger LOG = LoggerFactory.getLogger(NemoTimerInternals.class.getName());
  /** The current set timers by namespace and ID. */
  private Table<StateNamespace, String, TimerData> existingTimers;

  /** Pending input watermark timers, in timestamp order. */
  private final NavigableSet<Pair<K, TimerData>> watermarkTimers;

  /** Pending processing time timers, in timestamp order. */
  private final NavigableSet<Pair<K, TimerData>> processingTimers;

  /** Pending synchronized processing time timers, in timestamp order. */
  private final NavigableSet<Pair<K, TimerData>> synchronizedProcessingTimers;

  /** Current input watermark. */
  private Instant inputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  /** Current output watermark. */
  @Nullable
  private Instant outputWatermarkTime = null;

  /** Current processing time. */
  private Instant processingTime;

  /** Current synchronized processing time. */
  private Instant synchronizedProcessingTime;

  private final K key;

  public NemoTimerInternals(final K key,
                            final NavigableSet<Pair<K, TimerData>> watermarkTimers,
                            final NavigableSet<Pair<K, TimerData>> processingTimers,
                            final NavigableSet<Pair<K, TimerData>> synchronizedProcessingTimers) {
    this.key = key;
    this.watermarkTimers = watermarkTimers;
    this.processingTimers = processingTimers;
    this.synchronizedProcessingTimers = synchronizedProcessingTimers;
    this.existingTimers = HashBasedTable.create();
    this.processingTime = Instant.now();
    this.synchronizedProcessingTime = Instant.now();
  }

  public NemoTimerInternals(final K key,
                            final NavigableSet<Pair<K, TimerData>> watermarkTimers,
                            final NavigableSet<Pair<K, TimerData>> processingTimers,
                            final NavigableSet<Pair<K, TimerData>> synchronizedProcessingTimers,
                            final Table<StateNamespace, String, TimerData> existingTimers,
                            final Instant inputWatermarkTime,
                            final Instant processingTime,
                            final Instant synchronizedProcessingTime,
                            final Instant outputWatermarkTime) {
    this.key = key;
    this.watermarkTimers = watermarkTimers;
    this.processingTimers = processingTimers;
    this.synchronizedProcessingTimers = synchronizedProcessingTimers;
    this.existingTimers = existingTimers;
    this.inputWatermarkTime = inputWatermarkTime;
    this.processingTime = processingTime;
    this.synchronizedProcessingTime = synchronizedProcessingTime;
    this.outputWatermarkTime = outputWatermarkTime;
  }

  /** Check if any timer exists. */
  public boolean isEmpty() {
    return existingTimers.isEmpty();
  }

  /** returns current output watermark. */
  @Override
  @Nullable
  public Instant currentOutputWatermarkTime() {
    return outputWatermarkTime;
  }

  /*
  public boolean hasTimer() {
    return existingTimers.isEmpty() && registeredTimers == 0;
  }

  public void decrementRegisteredTimer() {
    registeredTimers -= 1;
  }
  */

  /* Set current output watermark. */
  public void setCurrentOutputWatermarkTime(final Instant time) {
    outputWatermarkTime = time;
  }

  /** Returns a list of timers based on time domain. */
  public NavigableSet<Pair<K, TimerInternals.TimerData>> timersForDomain(final TimeDomain domain) {
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

  /* Set timer. */
  @Override
  public void setTimer(
    final StateNamespace namespace, final String timerId, final Instant target, final TimeDomain timeDomain) {
    //LOG.info("Setting timer {}/{}, {}/{}", namespace, timerId, target, timeDomain);
    setTimer(TimerInternals.TimerData.of(timerId, namespace, target, timeDomain));
  }

  /**
   * @deprecated use {@link #setTimer(StateNamespace, String, Instant, TimeDomain)}.
   * @param timerData timer
   */
  @Deprecated
  @Override
  public void setTimer(final TimerInternals.TimerData timerData) {
    //LOG.info("Setting timer {}", timerData);
    WindowTracing.trace("{}.setTimer: {}", getClass().getSimpleName(), timerData);

    @Nullable
    TimerInternals.TimerData existing = existingTimers.get(timerData.getNamespace(), timerData.getTimerId());
    if (existing == null) {
      existingTimers.put(timerData.getNamespace(), timerData.getTimerId(), timerData);
      timersForDomain(timerData.getDomain()).add(Pair.of(key, timerData));
    } else {

      if (!timerData.getTimestamp().equals(existing.getTimestamp())) {
        NavigableSet<Pair<K, TimerInternals.TimerData>> timers = timersForDomain(timerData.getDomain());
        timers.remove(Pair.of(key, existing));
        timers.add(Pair.of(key, timerData));
        existingTimers.put(timerData.getNamespace(), timerData.getTimerId(), timerData);
      }
    }
  }

  /** Delete timer. */
  @Override
  public void deleteTimer(final StateNamespace namespace, final String timerId, final TimeDomain timeDomain) {
    throw new UnsupportedOperationException("Canceling a timer by ID is not yet supported.");
  }

  /**
   * @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}.
   * @param namespace state namespace
   * @param timerId timer ID
   */
  @Deprecated
  @Override
  public void deleteTimer(final StateNamespace namespace, final String timerId) {
    TimerInternals.TimerData existing = existingTimers.get(namespace, timerId);
    if (existing != null) {
      deleteTimer(existing);
    }
  }

  /**
   * @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}.
   * @param timer timer
   */
  @Deprecated
  @Override
  public void deleteTimer(final TimerInternals.TimerData timer) {
    WindowTracing.trace("{}.deleteTimer: {}", getClass().getSimpleName(), timer);
    existingTimers.remove(timer.getNamespace(), timer.getTimerId());
    timersForDomain(timer.getDomain()).remove(Pair.of(key, timer));
  }

  /** Set current processing time. */
  public void setCurrentProcessingTime(final Instant time) {
    processingTime = time;
  }

  /** Set current synchronized processing time. */
  public void setCurrentSynchronizedProcessingTime(final Instant time) {
    synchronizedProcessingTime = time;
  }

  /** Set current input watermark. */
  public void setCurrentInputWatermarkTime(final Instant time) {
    inputWatermarkTime = time;
  }

  /** Returns current processing time. */
  @Override
  public Instant currentProcessingTime() {
    processingTime = Instant.now();
    return processingTime;
  }

  /* Returns current synchronized processing time. */
  @Override
  @Nullable
  public Instant currentSynchronizedProcessingTime() {
    synchronizedProcessingTime = Instant.now();
    return synchronizedProcessingTime;
  }

  /* Returns currnet input watermark. */
  @Override
  public Instant currentInputWatermarkTime() {
    return inputWatermarkTime;
  }

  /* Stringify {@link NemoTimerInternals}. */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
      .add("watermarkTimers", watermarkTimers)
      .add("watermarkTimersHashCode", watermarkTimers.hashCode())
      .add("processingTimers", processingTimers)
      .add("synchronizedProcessingTimers", synchronizedProcessingTimers)
      .add("inputWatermarkTime", inputWatermarkTime)
      .add("outputWatermarkTime", outputWatermarkTime)
      .add("processingTime", processingTime)
      .toString();
  }

  /** Accessor for existing timers. */
  public Table<StateNamespace, String, TimerData> getExistingTimers() {
    return existingTimers;
  }

  /** Accessor for watermark timers. */
  public NavigableSet<Pair<K, TimerData>> getWatermarkTimers() {
    return watermarkTimers;
  }

  /** Accessor for processing timers. */
  public NavigableSet<Pair<K, TimerData>> getProcessingTimers() {
    return processingTimers;
  }

  /** Accessor for synchronized processing timers. */
  public NavigableSet<Pair<K, TimerData>> getSynchronizedProcessingTimers() {
    return synchronizedProcessingTimers;
  }

  /** Returns input watermark. */
  public Instant getInputWatermarkTime() {
    return inputWatermarkTime;
  }

  /** Returns output watermark. */
  public Instant getOutputWatermarkTime() {
    return outputWatermarkTime;
  }

  /** Returns current processing time. */
  public Instant getProcessingTime() {
    return processingTime;
  }

  /** Returns current processing time. */
  public Instant getSynchronizedProcessingTime() {
    return synchronizedProcessingTime;
  }

  /** Accessor for key. */
  public K getKey() {
    return key;
  }
}
