package org.apache.nemo.compiler.frontend.beam.transform;

import com.google.common.base.MoreObjects;
import com.google.common.collect.*;
import org.apache.beam.runners.core.*;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.nemo.common.Pair;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NemoTimerInternals<K> implements TimerInternals {

  /** The current set timers by namespace and ID. */
  Table<StateNamespace, String, TimerInternals.TimerData> existingTimers = HashBasedTable.create();

  /** Pending input watermark timers, in timestamp order. */
  private final NavigableSet<Pair<K, TimerData>> watermarkTimers;

  /** Pending processing time timers, in timestamp order. */
  private final NavigableSet<Pair<K, TimerData>> processingTimers;

  /** Pending synchronized processing time timers, in timestamp order. */
  private final NavigableSet<Pair<K, TimerData>> synchronizedProcessingTimers;

  /** Current input watermark. */
  private Instant inputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  /** Current output watermark. */
  @Nullable private Instant outputWatermarkTime = null;

  /** Current processing time. */
  private Instant processingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  /** Current synchronized processing time. */
  private Instant synchronizedProcessingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  private final K key;

  public NemoTimerInternals(final K key,
                            final NavigableSet<Pair<K, TimerData>> watermarkTimers,
                            final NavigableSet<Pair<K, TimerData>> processingTimers,
                            final NavigableSet<Pair<K, TimerData>> synchronizedProcessingTimers) {
    this.key = key;
    this.watermarkTimers = watermarkTimers;
    this.processingTimers = processingTimers;
    this.synchronizedProcessingTimers = synchronizedProcessingTimers;
  }

  @Override
  @Nullable
  public Instant currentOutputWatermarkTime() {
    return outputWatermarkTime;
  }

  public void setCurrentOutputWatermarkTime(final Instant time) {
    outputWatermarkTime = time;
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

  @Override
  public void setTimer(
    StateNamespace namespace, String timerId, Instant target, TimeDomain timeDomain) {
    setTimer(TimerInternals.TimerData.of(timerId, namespace, target, timeDomain));
  }

  /** @deprecated use {@link #setTimer(StateNamespace, String, Instant, TimeDomain)}. */
  @Deprecated
  @Override
  public void setTimer(TimerInternals.TimerData timerData) {
    WindowTracing.trace("{}.setTimer: {}", getClass().getSimpleName(), timerData);

    @Nullable
    TimerInternals.TimerData existing = existingTimers.get(timerData.getNamespace(), timerData.getTimerId());
    if (existing == null) {
      existingTimers.put(timerData.getNamespace(), timerData.getTimerId(), timerData);
      timersForDomain(timerData.getDomain()).add(Pair.of(key, timerData));
    } else {
      checkArgument(
        timerData.getDomain().equals(existing.getDomain()),
        "Attempt to set %s for time domain %s, but it is already set for time domain %s",
        timerData.getTimerId(),
        timerData.getDomain(),
        existing.getDomain());

      if (!timerData.getTimestamp().equals(existing.getTimestamp())) {
        NavigableSet<Pair<K, TimerInternals.TimerData>> timers = timersForDomain(timerData.getDomain());
        timers.remove(Pair.of(key, existing));
        timers.add(Pair.of(key, timerData));
        existingTimers.put(timerData.getNamespace(), timerData.getTimerId(), timerData);
      }
    }
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
    throw new UnsupportedOperationException("Canceling a timer by ID is not yet supported.");
  }

  /** @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}. */
  @Deprecated
  @Override
  public void deleteTimer(StateNamespace namespace, String timerId) {
    TimerInternals.TimerData existing = existingTimers.get(namespace, timerId);
    if (existing != null) {
      deleteTimer(existing);
    }
  }

  /** @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}. */
  @Deprecated
  @Override
  public void deleteTimer(TimerInternals.TimerData timer) {
    WindowTracing.trace("{}.deleteTimer: {}", getClass().getSimpleName(), timer);
    existingTimers.remove(timer.getNamespace(), timer.getTimerId());
    timersForDomain(timer.getDomain()).remove(Pair.of(key, timer));
  }

  public void setCurrentProcessingTime(final Instant time) {
    processingTime = time;
  }


  public void setCurrentSynchronizedProcessingTime(final Instant time) {
    synchronizedProcessingTime = time;
  }

  public void setCurrentInputWatermarkTime(final Instant time) {
    inputWatermarkTime = time;
  }

  @Override
  public Instant currentProcessingTime() {
    return processingTime;
  }

  @Override
  @Nullable
  public Instant currentSynchronizedProcessingTime() {
    return synchronizedProcessingTime;
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return inputWatermarkTime;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
      .add("watermarkTimers", watermarkTimers)
      .add("processingTimers", processingTimers)
      .add("synchronizedProcessingTimers", synchronizedProcessingTimers)
      .add("inputWatermarkTime", inputWatermarkTime)
      .add("outputWatermarkTime", outputWatermarkTime)
      .add("processingTime", processingTime)
      .toString();
  }
}
