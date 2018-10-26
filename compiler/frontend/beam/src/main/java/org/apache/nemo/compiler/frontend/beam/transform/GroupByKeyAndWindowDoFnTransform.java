package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.*;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final Map<K, List<WindowedValue<InputT>>> keyToValues;
  private transient InMemoryTimerInternalsFactory timerInternalsFactory;

  /**
   * GroupByKey constructor.
   */
  public GroupByKeyAndWindowDoFnTransform(final Map<TupleTag<?>, Coder<?>> outputCoders,
                                          final TupleTag<KV<K, Iterable<InputT>>> mainOutputTag,
                                          final List<TupleTag<?>> additionalOutputTags,
                                          final WindowingStrategy<?, ?> windowingStrategy,
                                          final Collection<PCollectionView<?>> sideInputs,
                                          final PipelineOptions options,
                                          final SystemReduceFn reduceFn) {
    super(null, /* doFn */
      null, /* inputCoder */
      outputCoders,
      mainOutputTag,
      additionalOutputTags,
      windowingStrategy,
      sideInputs,
      options);
    this.keyToValues = new HashMap<>();
    this.reduceFn = reduceFn;
  }

  /**
   * This creates a new DoFn that groups elements by key and window.
   * @param doFn original doFn.
   * @return GroupAlsoByWindowViaWindowSetNewDoFn
   */
  @Override
  protected DoFn wrapDoFn(final DoFn doFn) {
    timerInternalsFactory = new InMemoryTimerInternalsFactory();
    // This function performs group by key and window operation
    return
      GroupAlsoByWindowViaWindowSetNewDoFn.create(
        getWindowingStrategy(),
        new InMemoryStateInternalsFactory(),
        timerInternalsFactory,
        getSideInputReader(),
        reduceFn,
        getOutputManager(),
        getMainOutputTag());
  }

  @Override
  public void onData(final WindowedValue<KV<K, InputT>> element) {
    final KV<K, InputT> kv = element.getValue();
    keyToValues.putIfAbsent(kv.getKey(), new ArrayList());
    keyToValues.get(kv.getKey()).add(element.withValue(kv.getValue()));
  }

  /**
   * This advances the input watermark and processing time to the timestamp max value
   * in order to emit all data.
   */
  @Override
  protected void beforeClose() {
    final InMemoryTimerInternals timerInternals = timerInternalsFactory.timerInternals;
    try {
      // Finish any pending windows by advancing the input watermark to infinity.
      timerInternals.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);

      // Finally, advance the processing time to infinity to fire any timers.
      timerInternals.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
      timerInternals.advanceSynchronizedProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    if (keyToValues.isEmpty()) {
      LOG.warn("Beam GroupByKeyAndWindowDoFnTransform received no data!");
    } else {
      // timer
      final Iterable<TimerInternals.TimerData> timerData = getTimers(timerInternals);

      keyToValues.entrySet().stream().forEach(entry -> {
        // The GroupAlsoByWindowViaWindowSetNewDoFn requires KeyedWorkItem,
        // so we convert the KV to KeyedWorkItem
        final KeyedWorkItem<K, InputT> keyedWorkItem =
          KeyedWorkItems.workItem(entry.getKey(), timerData, entry.getValue());
        getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(keyedWorkItem));
      });
      keyToValues.clear();
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("GroupByKeyAndWindowDoFnTransform:");
    return sb.toString();
  }

  private Iterable<TimerInternals.TimerData> getTimers(final InMemoryTimerInternals timerInternals) {
    final List<TimerInternals.TimerData> timerData = new LinkedList<>();

    while (true) {
      TimerInternals.TimerData timer;
      boolean hasFired = false;

      while ((timer = timerInternals.removeNextEventTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }
      while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }
      while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
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
   * InMemoryStateInternalsFactory.
   */
  final class InMemoryStateInternalsFactory implements StateInternalsFactory<K> {
    private final InMemoryStateInternals inMemoryStateInternals;

    InMemoryStateInternalsFactory() {
      this.inMemoryStateInternals = InMemoryStateInternals.forKey(null);
    }

    @Override
    public StateInternals stateInternalsForKey(final K key) {
      return inMemoryStateInternals;
    }
  }

  /**
   * InMemoryTimerInternalsFactory.
   */
  final class InMemoryTimerInternalsFactory implements TimerInternalsFactory<K> {
    private final InMemoryTimerInternals timerInternals;

    InMemoryTimerInternalsFactory() {
      this.timerInternals = new InMemoryTimerInternals();
    }

    @Override
    public TimerInternals timerInternalsForKey(final K key) {
      return timerInternals;
    }
  }
}

