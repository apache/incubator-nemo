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
   */
public final class InMemoryTimerInternalsFactory<K> implements TimerInternalsFactory<K> {

    /** Pending input watermark timers, in timestamp order. */
    public NavigableSet<Pair<K, TimerInternals.TimerData>> watermarkTimers;
    /** Pending processing time timers, in timestamp order. */
    public NavigableSet<Pair<K, TimerInternals.TimerData>> processingTimers;
    /** Pending synchronized processing time timers, in timestamp order. */
    public NavigableSet<Pair<K, TimerInternals.TimerData>> synchronizedProcessingTimers;

    /** Current input watermark. */
    public Instant inputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;


    /** Current processing time. */
    public Instant processingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

    /** Current synchronized processing time. */
    public Instant synchronizedProcessingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

    public Map<K, NemoTimerInternals> timerInternalsMap;

    @Override
    public String toString() {
      return "TimerInternalsMap: " + timerInternalsMap + "\n"
      + "WatermarkTimers: " + watermarkTimers + "(" + watermarkTimers.hashCode() + ")\n"
      + "ProcessingTimers: " + processingTimers + "\n"
      + "SyncTimers: " + synchronizedProcessingTimers + "\n"
      + "InputWatermarkTime: " + inputWatermarkTime + "\n"
        + "ProcessingTime: " + processingTime +"\n"
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
      this.watermarkTimers = new TreeSet<>(comparator);
      this.processingTimers = new TreeSet<>(comparator);
      this.synchronizedProcessingTimers = new TreeSet<>(comparator);
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
      this.watermarkTimers = watermarkTimers;
      this.processingTimers = processingTimers;
      this.synchronizedProcessingTimers = synchronizedProcessingTimers;
      this.inputWatermarkTime = inputWatermarkTime;
      this.processingTime = processingTime;
      this.synchronizedProcessingTime = synchronizedProcessingTime;
      this.timerInternalsMap = timerInternalsMap;
    }

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTimerInternalsFactory.class.getName());

  public void setState(final InMemoryTimerInternalsFactory<K> timerInternalsFactory) {
    this.watermarkTimers = timerInternalsFactory.watermarkTimers;
    this.processingTimers = timerInternalsFactory.processingTimers;
    this.synchronizedProcessingTimers = timerInternalsFactory.synchronizedProcessingTimers;

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
      final NemoTimerInternals internal =  new NemoTimerInternals<>(key,
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


  public Pair<K, TimerInternals.TimerData> getNextTimer() {
    Pair<K, TimerInternals.TimerData> timer;
    if ((timer = removeNextEventTimer()) != null) {
      return timer;
    }
    else if ((timer = removeNextProcessingTimer()) != null) {
      return timer;
    }
    else if ((timer = removeNextSynchronizedProcessingTimer()) != null) {
      return timer;
    }
    else {
      return null;
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
    for (Iterator<NemoTimerInternals> iter = timerInternalsMap.values().iterator(); iter.hasNext();) {
      NavigableSet<Pair<K, TimerInternals.TimerData>> timers = iter.next().timersForDomain(domain);
      if (!timers.isEmpty() && !currentTime.isBefore(timers.first().right().getTimestamp())) {
        Pair<K, TimerInternals.TimerData> timer = timers.pollFirst();
        final NemoTimerInternals<K> timerInternals = timerInternalsMap.get(timer.left());
        timerInternals.deleteTimer(timer.right());
        return timer;
      }
    }
    return null;
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
