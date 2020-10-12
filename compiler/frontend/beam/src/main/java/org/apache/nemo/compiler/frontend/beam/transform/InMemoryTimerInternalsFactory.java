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

import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.nemo.common.Pair;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import java.util.*;

/**
 * InMemoryTimerInternalsFactory.
 * @param <K> key type
 */
public final class InMemoryTimerInternalsFactory<K> implements TimerInternalsFactory<K> {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTimerInternalsFactory.class.getName());

  /**
   * Current input watermark.
   */
  private Instant inputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  /**
   * Current processing time.
   */
  private Instant processingTime;

  /**
   * Current synchronized processing time.
   */
  private Instant synchronizedProcessingTime;

  private Map<K, NemoTimerInternals> timerInternalsMap;

  @Override
  public String toString() {
    return "TimerInternalsMap: " + timerInternalsMap + "\n"
      + "InputWatermarkTime: " + inputWatermarkTime + "\n"
      + "ProcessingTime: " + processingTime + "\n"
      + "SyncProcessingTime: " + synchronizedProcessingTime;
  }

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
    this.timerInternalsMap = new HashMap<>();
    this.processingTime = Instant.now();
    this.synchronizedProcessingTime = Instant.now();
  }

  public InMemoryTimerInternalsFactory(
    final NavigableSet<Pair<K, TimerInternals.TimerData>> watermarkTimers,
    final NavigableSet<Pair<K, TimerInternals.TimerData>> processingTimers,
    final NavigableSet<Pair<K, TimerInternals.TimerData>> synchronizedProcessingTimers,
    final Instant inputWatermarkTime,
    final Instant processingTime,
    final Instant synchronizedProcessingTime,
    final Map<K, NemoTimerInternals> timerInternalsMap) {
    this.inputWatermarkTime = inputWatermarkTime;
    this.processingTime = processingTime;
    this.synchronizedProcessingTime = synchronizedProcessingTime;
    this.timerInternalsMap = timerInternalsMap;
  }

  public void setState(final InMemoryTimerInternalsFactory<K> timerInternalsFactory) {
    this.inputWatermarkTime = timerInternalsFactory.inputWatermarkTime;
    this.processingTime = timerInternalsFactory.processingTime;
    this.synchronizedProcessingTime = timerInternalsFactory.synchronizedProcessingTime;
    this.timerInternalsMap = timerInternalsFactory.timerInternalsMap;
  }

  @Override
  public TimerInternals timerInternalsForKey(final K key) {
    if (timerInternalsMap.get(key) != null) {
      return timerInternalsMap.get(key);
    } else {
      final NemoTimerInternals internal = new NemoTimerInternals<>(key,
        new TreeSet<>(comparator),
        new TreeSet<>(comparator),
        new TreeSet<>(comparator));
      timerInternalsMap.put(key, internal);
      return internal;
    }
  }

  public void removeTimerForKeyIfEmpty(final K key) {
    final NemoTimerInternals<K> timerInternals = timerInternalsMap.get(key);
    if (timerInternals.isEmpty()) {
      // remove from timerInternalsMap
      timerInternalsMap.remove(key);
    }
  }

  /**
   * Remove timer.
   */
  public void removeTimer(final Pair<K, TimerInternals.TimerData> timer) {
    timerInternalsMap.get(timer.left()).deleteTimer(timer.right());
    return;
  }

  /**
   * Returns the next eligible event time timer, if none returns null.
   */
  @Nullable
  public Pair<K, TimerInternals.TimerData> getNextEventTimer() {
    Pair<K, TimerInternals.TimerData> timer = getNextTimer(inputWatermarkTime, TimeDomain.EVENT_TIME);
    if (timer != null) {
      WindowTracing.trace(
        "{}.removeNextEventTimer: firing {} at {}",
        getClass().getSimpleName(),
        timer,
        inputWatermarkTime);
    }
    return timer;
  }

  /**
   * Returns the next eligible processing time timer, if none returns null.
   */
  @Nullable
  public Pair<K, TimerInternals.TimerData> getNextProcessingTimer() {
    Pair<K, TimerInternals.TimerData> timer = getNextTimer(processingTime, TimeDomain.PROCESSING_TIME);
    if (timer != null) {
      WindowTracing.trace(
        "{}.removeNextProcessingTimer: firing {} at {}",
        getClass().getSimpleName(),
        timer,
        processingTime);
    }
    return timer;
  }

  /**
   * Returns the next eligible synchronized processing time timer, if none returns null.
   */
  @Nullable
  public Pair<K, TimerInternals.TimerData> getNextSynchronizedProcessingTimer() {
    Pair<K, TimerInternals.TimerData> timer =
      getNextTimer(synchronizedProcessingTime, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
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
  private Pair<K, TimerInternals.TimerData> getNextTimer(final Instant currentTime, final TimeDomain domain) {
    for (Iterator<NemoTimerInternals> iter = timerInternalsMap.values().iterator(); iter.hasNext();) {
      NavigableSet<Pair<K, TimerInternals.TimerData>> timers = iter.next().timersForDomain(domain);
      if (!timers.isEmpty() && !currentTime.isBefore(timers.first().right().getTimestamp())) {
        Pair<K, TimerInternals.TimerData> timer = timers.pollFirst();
        return timer;
      }
    }
    return null;
  }

  public Instant getInputWatermarkTime() {
    return inputWatermarkTime;
  }

  public Instant getProcessingTime() {
    return processingTime;
  }

  public Instant getSynchronizedProcessingTime() {
    return synchronizedProcessingTime;
  }

  public Map<K, NemoTimerInternals> getTimerInternalsMap() {
    return timerInternalsMap;
  }

  public void setSynchronizedProcessingTime(final Instant time) {
    synchronizedProcessingTime = time;
  }

  public void setProcessingTime(final Instant time) {
    processingTime = time;
  }

  public void setInputWatermarkTime(final Instant time) {
    inputWatermarkTime = time;
  }
}
