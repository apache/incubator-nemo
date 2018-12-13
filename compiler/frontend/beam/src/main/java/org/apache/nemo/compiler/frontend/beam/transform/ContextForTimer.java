package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.nemo.common.Pair;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

final class ContextForTimer<K> {
  // Pending input watermark timers of all keys, in timestamp order.
  private final NavigableSet<Pair<K, TimerInternals.TimerData>> watermarkTimers;

  // Pending processing time timers of all keys, in timestamp order.
  private final NavigableSet<Pair<K, TimerInternals.TimerData>> processingTimers;

  // Pending synchronized processing time timers of all keys, in timestamp order.
  private final NavigableSet<Pair<K, TimerInternals.TimerData>> synchronizedProcessingTimers;

  // Current input watermark.
  private Instant inputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  // Current processing time.
  private Instant processingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  // Current synchronized processing time.
  private Instant synchronizedProcessingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  // map that holds timer internals of each key
  private final Map<K, NemoTimerInternals> timerInternalsMap;

  /**
   * This comparator first compares the timer data, in timestamp order.
   * If two timer data have the same timestamp, we order them by key.
   * As the key is not comparable, we convert it to string and compare the string value.
   * In fact, the key ordering is not important, because the first ordering is determined by the timestamp.
   * We only have to check whether the two keys are the same or not.
   */
  private final Comparator<Pair<K, TimerInternals.TimerData>> comparator = (o1, o2) -> {
    final int comp = o1.right().compareTo(o2.right());
    if (comp == 0) {
      // if two timer are the same, compare key
      if (o1.left() == null && o2.left() == null) {
        return 0;
      } else if (o1.left() == null || o2.left() == null) {
        return -1;
      } else if (o1.left().equals(o2.left())) {
        return 0;
      } else {
        return o1.left().toString().compareTo(o2.left().toString());
      }
    } else {
      return comp;
    }
  };

  ContextForTimer(final Map<K, NemoTimerInternals> timerInternalsMap) {
    this.watermarkTimers = new TreeSet<>(comparator);
    this.processingTimers = new TreeSet<>(comparator);
    this.synchronizedProcessingTimers = new TreeSet<>(comparator);
    this.timerInternalsMap = timerInternalsMap;
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

  public Instant currentProcessingTime() {
    return processingTime;
  }

  @Nullable
  public Instant currentSynchronizedProcessingTime() {
    return synchronizedProcessingTime;
  }

  public Instant currentInputWatermarkTime() {
    return inputWatermarkTime;
  }

  public void addTimer(final K key, final TimerInternals.TimerData timer) {
    timersForDomain(timer.getDomain()).add(Pair.of(key, timer));
  }

  public void removeTimer(final K key, final TimerInternals.TimerData timer) {
    NavigableSet<Pair<K, TimerInternals.TimerData>> timers = timersForDomain(timer.getDomain());
    timers.remove(Pair.of(key, timer));
  }

  /** Returns the next eligible event time timer, if none returns null. */
  @Nullable
  public Pair<K, TimerInternals.TimerData> removeNextEventTimer() {
    final Pair<K, TimerInternals.TimerData> timer = removeNextTimer(inputWatermarkTime, TimeDomain.EVENT_TIME);
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
    final Pair<K, TimerInternals.TimerData> timer = removeNextTimer(processingTime, TimeDomain.PROCESSING_TIME);
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
    final Pair<K, TimerInternals.TimerData> timer =
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
  private Pair<K, TimerInternals.TimerData> removeNextTimer(
    final Instant currentTime, final TimeDomain domain) {
    final NavigableSet<Pair<K, TimerInternals.TimerData>> timers = timersForDomain(domain);

    if (!timers.isEmpty() && currentTime.isAfter(timers.first().right().getTimestamp())) {
      Pair<K, TimerInternals.TimerData> timer = timers.pollFirst();
      timerInternalsMap.get(timer.left()).deleteTimer(timer.right());
      return timer;
    } else {
      return null;
    }
  }

  private NavigableSet<Pair<K, TimerInternals.TimerData>> timersForDomain(final TimeDomain domain) {
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
