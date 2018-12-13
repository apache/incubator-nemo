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
import com.google.common.collect.*;
import org.apache.beam.runners.core.*;
import org.apache.beam.sdk.state.*;
import org.joda.time.Instant;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Nemo timer internals that add/remove timer data to timer context.
 * @param <K> key type
 */
public final class NemoTimerInternals<K> implements TimerInternals {

  /** The current set timers by namespace and ID. */
  private final Table<StateNamespace, String, TimerInternals.TimerData> existingTimers = HashBasedTable.create();

  /** Current output watermark. */
  @Nullable private Instant outputWatermarkTime = null;

  private final K key;
  private final ContextForTimer<K> context;

  public NemoTimerInternals(final K key,
                            final ContextForTimer<K> context) {
    this.key = key;
    this.context = context;
  }

  @Override
  @Nullable
  public Instant currentOutputWatermarkTime() {
    return outputWatermarkTime;
  }

  public void setCurrentOutputWatermarkTime(final Instant time) {
    outputWatermarkTime = time;
  }

  @Override
  public void setTimer(
    final StateNamespace namespace, final String timerId, final Instant target, final TimeDomain timeDomain) {
    setTimer(TimerInternals.TimerData.of(timerId, namespace, target, timeDomain));
  }

  /** @deprecated use {@link #setTimer(StateNamespace, String, Instant, TimeDomain)}. */
  @Deprecated
  @Override
  public void setTimer(final TimerInternals.TimerData timerData) {
    @Nullable
    final TimerInternals.TimerData existing = existingTimers.get(timerData.getNamespace(), timerData.getTimerId());
    if (existing == null) {
      existingTimers.put(timerData.getNamespace(), timerData.getTimerId(), timerData);
      context.addTimer(key, timerData);
    } else {
      checkArgument(
        timerData.getDomain().equals(existing.getDomain()),
        "Attempt to set %s for time domain %s, but it is already set for time domain %s",
        timerData.getTimerId(),
        timerData.getDomain(),
        existing.getDomain());

      if (!timerData.getTimestamp().equals(existing.getTimestamp())) {
        context.removeTimer(key, existing);
        context.addTimer(key, timerData);
        existingTimers.put(timerData.getNamespace(), timerData.getTimerId(), timerData);
      }
    }
  }

  @Override
  public void deleteTimer(final StateNamespace namespace, final String timerId, final TimeDomain timeDomain) {
    throw new UnsupportedOperationException("Canceling a timer by ID is not yet supported.");
  }

  /** @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}. */
  @Deprecated
  @Override
  public void deleteTimer(final StateNamespace namespace, final String timerId) {
    final TimerInternals.TimerData existing = existingTimers.get(namespace, timerId);
    if (existing != null) {
      deleteTimer(existing);
    }
  }

  /** @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}. */
  @Deprecated
  @Override
  public void deleteTimer(final TimerInternals.TimerData timer) {
    existingTimers.remove(timer.getNamespace(), timer.getTimerId());
    context.removeTimer(key, timer);
  }

  @Override
  public Instant currentProcessingTime() {
    return context.currentProcessingTime();
  }

  @Override
  @Nullable
  public Instant currentSynchronizedProcessingTime() {
    return context.currentSynchronizedProcessingTime();
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return context.currentInputWatermarkTime();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
      .add("key", key)
      .add("outputWatermarkTime", outputWatermarkTime)
      .toString();
  }
}
