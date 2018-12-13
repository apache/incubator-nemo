package org.apache.nemo.compiler.frontend.beam.transform;

import com.google.common.base.MoreObjects;
import com.google.common.collect.*;
import org.apache.beam.runners.core.*;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.util.WindowTracing;
import org.joda.time.Instant;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Nemo timer internals that add/remove timer data to timer context.
 * @param <K> key type
 */
public final class NemoTimerInternals<K> implements TimerInternals {

  /** The current set timers by namespace and ID. */
  Table<StateNamespace, String, TimerInternals.TimerData> existingTimers = HashBasedTable.create();

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
