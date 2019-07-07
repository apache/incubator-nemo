package org.apache.nemo.compiler.frontend.beam.transform;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTable;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.nemo.common.Pair;
import org.apache.nemo.compiler.frontend.beam.transform.coders.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStateInternals<K> implements StateInternals {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryStateInternals.class.getName());
  public static <K> InMemoryStateInternals<K> forKey(@Nullable K key,
                                                     final NemoStateBackend nemoStateBackend) {

    //LOG.info("State internals for key {}: {}", key, nemoStateBackend);
    return new InMemoryStateInternals<>(key, nemoStateBackend);
  }

  private final @Nullable K key;

  private NemoStateBackend nemoStateBackend;

  //private final InMemoryStateInternalsFactory stateInternalsFactory;

  protected InMemoryStateInternals(@Nullable K key,
                                   final NemoStateBackend nemoStateBackend) {
    this.key = key;
    this.nemoStateBackend = nemoStateBackend;
    //this.stateInternalsFactory = stateInternalsFactory;
  }

  @Override
  public @Nullable K getKey() {
    return key;
  }

  /**
   * Interface common to all in-memory state cells. Includes ability to see whether a cell has been
   * cleared and the ability to create a clone of the contents.
   */
  public interface InMemoryState<T extends InMemoryState<T>> {
    boolean isCleared();

    T copy();
  }

  protected final StateTable inMemoryState =
      new StateTable() {
        @Override
        protected StateTag.StateBinder binderForNamespace(StateNamespace namespace, StateContext<?> c) {
          final Map<StateTag, Pair<State, Coder>> map = nemoStateBackend.map.getOrDefault(namespace, new ConcurrentHashMap<>());
          nemoStateBackend.map.putIfAbsent(namespace, map);

          final Map<StateTag, Pair<State, Coder>> m = nemoStateBackend.map.get(namespace);

          return new InMemoryStateBinder(namespace, m, c);
        }
      };

  public void clear() {
    inMemoryState.clear();
    nemoStateBackend.clear();
  }

  /**
   * Return true if the given state is empty. This is used by the test framework to make sure that
   * the state has been properly cleaned up.
   */
  protected boolean isEmptyForTesting(State state) {
    return ((InMemoryState<?>) state).isCleared();
  }

  @Override
  public <T extends State> T state(
    StateNamespace namespace, StateTag<T> address, final StateContext<?> c) {
    return inMemoryState.get(namespace, address, c);
  }

  /** A {@link StateBinder} that returns In Memory {@link State} objects. */
  public class InMemoryStateBinder implements StateTag.StateBinder {
    private final StateContext<?> c;
    private final Map<StateTag, Pair<State, Coder>> stateCoderMap;
    private final StateNamespace namespace;


    public InMemoryStateBinder(final StateNamespace namespace,
                               final Map<StateTag, Pair<State, Coder>> stateCoderMap,
                               StateContext<?> c) {
      this.namespace = namespace;
      this.stateCoderMap = stateCoderMap;
      this.c = c;
    }

    @Override
    public <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder) {
      //LOG.info("Bind value for key {}, namespace: {}, address: {}",
      //  key, namespace, address);
      if (stateCoderMap.containsKey(address)) {
        return (ValueState<T>) stateCoderMap.get(address).left();
      } else {
        final ValueState<T> state = new InMemoryValue<>(coder);
        stateCoderMap.put(address, Pair.of(state, new ValueStateCoder<>(coder)));
        return state;
      }
    }

    @Override
    public <T> BagState<T> bindBag(final StateTag<BagState<T>> address, Coder<T> elemCoder) {
      //LOG.info("Bind value for key {}, namespace: {}, address: {}",
      //  key, namespace, address);
      if (stateCoderMap.containsKey(address)) {
        return (BagState<T>) stateCoderMap.get(address).left();
      } else {
        final BagState<T> state = new InMemoryBag<>(elemCoder);
        stateCoderMap.put(address, Pair.of(state, new BagStateCoder<>(elemCoder)));
        return state;
      }
    }

    @Override
    public <T> SetState<T> bindSet(StateTag<SetState<T>> spec, Coder<T> elemCoder) {
      //LOG.info("Bind value for key {}, namespace: {}, address: {}",
      //  key, namespace, spec);
      if (stateCoderMap.containsKey(spec)) {
        return (SetState<T>) stateCoderMap.get(spec).left();
      } else {
        final SetState<T> state = new InMemorySet<>(elemCoder);
        stateCoderMap.put(spec, Pair.of(state, new SetStateCoder<>(elemCoder)));
        return state;
      }
    }

    @Override
    public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
        StateTag<MapState<KeyT, ValueT>> spec,
        Coder<KeyT> mapKeyCoder,
        Coder<ValueT> mapValueCoder) {
      //LOG.info("Bind value for key {}, namespace: {}, address: {}",
      //  key, namespace, spec);
      if (stateCoderMap.containsKey(spec)) {
        return (MapState<KeyT, ValueT>) stateCoderMap.get(spec).left();
      } else {
        final MapState<KeyT, ValueT> state = new InMemoryMap<>(mapKeyCoder, mapValueCoder);
        stateCoderMap.put(spec, Pair.of(state, new MapStateCoder<>(mapKeyCoder, mapValueCoder)));
        return state;
      }
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
        StateTag<CombiningState<InputT, AccumT, OutputT>> address,
        Coder<AccumT> accumCoder,
        final Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
      //LOG.info("Bind value for key {}, namespace: {}, address: {}",
      //  key, namespace, address);
      if (stateCoderMap.containsKey(address)) {
        //LOG.info("Bind combining value multiple: {}/{}", namespace, address);
        return (CombiningState<InputT, AccumT, OutputT>) stateCoderMap.get(address).left();
      } else {
        //LOG.info("Bind combining value first: {}/{}", namespace, address);
        final CombiningState<InputT, AccumT, OutputT> state = new InMemoryCombiningState<>(combineFn, accumCoder);
        stateCoderMap.put(address, Pair.of(state, new CombiningStateCoder<>(accumCoder, combineFn)));
        return state;
      }
    }

    @Override
    public WatermarkHoldState bindWatermark(
        StateTag<WatermarkHoldState> address, TimestampCombiner timestampCombiner) {
      //LOG.info("Bind value for key {}, namespace: {}, address: {}",
      //  key, namespace, address);
      if (stateCoderMap.containsKey(address)) {
        return (WatermarkHoldState) stateCoderMap.get(address).left();
      } else {
        final WatermarkHoldState state = new InMemoryWatermarkHold(timestampCombiner);
        stateCoderMap.put(address, Pair.of(state, WatermarkHoldStateCoder.getInstance()));
        return state;
      }
    }

    @Override
    public <InputT, AccumT, OutputT>
        CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
            StateTag<CombiningState<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder,
            CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
      //LOG.info("Bind value for key {}, namespace: {}, address: {}",
      //  key, namespace, address);
      return bindCombiningValue(address, accumCoder, CombineFnUtil.bindContext(combineFn, c));
    }
  }

  /** An {@link InMemoryState} implementation of {@link ValueState}. */
  public static final class InMemoryValue<T>
      implements ValueState<T>, InMemoryState<InMemoryValue<T>> {
    private final Coder<T> coder;

    private boolean isCleared = true;
    private @Nullable T value = null;

    public InMemoryValue(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this Value.
      //LOG.info("Clear value: {}", value);
      value = null;
      isCleared = true;
    }

    @Override
    public InMemoryValue<T> readLater() {
      return this;
    }

    @Override
    public T read() {
      return value;
    }

    @Override
    public void write(T input) {
      isCleared = false;
      this.value = input;
    }

    @Override
    public InMemoryValue<T> copy() {
      InMemoryValue<T> that = new InMemoryValue<>(coder);
      if (!this.isCleared) {
        that.isCleared = this.isCleared;
        that.value = uncheckedClone(coder, this.value);
      }
      return that;
    }

    @Override
    public boolean isCleared() {
      return isCleared;
    }
  }

  /** An {@link InMemoryState} implementation of {@link WatermarkHoldState}. */
  public static final class InMemoryWatermarkHold<W extends BoundedWindow>
      implements WatermarkHoldState, InMemoryState<InMemoryWatermarkHold<W>> {

    private final TimestampCombiner timestampCombiner;

    @Nullable private Instant combinedHold = null;

    public InMemoryWatermarkHold(TimestampCombiner timestampCombiner) {
      this.timestampCombiner = timestampCombiner;
    }

    @Override
    public InMemoryWatermarkHold readLater() {
      return this;
    }

    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this WatermarkBagInternal.
      //LOG.info("Clear value: {}", combinedHold);
      combinedHold = null;
    }

    @Override
    public Instant read() {
      return combinedHold;
    }

    @Override
    public void add(Instant outputTime) {
      combinedHold =
          combinedHold == null ? outputTime : timestampCombiner.combine(combinedHold, outputTime);
    }

    @Override
    public boolean isCleared() {
      return combinedHold == null;
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }

        @Override
        public Boolean read() {
          return combinedHold == null;
        }
      };
    }

    @Override
    public TimestampCombiner getTimestampCombiner() {
      return timestampCombiner;
    }

    @Override
    public String toString() {
      return Objects.toString(combinedHold);
    }

    @Override
    public InMemoryWatermarkHold<W> copy() {
      InMemoryWatermarkHold<W> that = new InMemoryWatermarkHold<>(timestampCombiner);
      that.combinedHold = this.combinedHold;
      return that;
    }
  }

  /** An {@link InMemoryState} implementation of {@link CombiningState}. */
  public static final class InMemoryCombiningState<InputT, AccumT, OutputT>
      implements CombiningState<InputT, AccumT, OutputT>,
          InMemoryState<InMemoryCombiningState<InputT, AccumT, OutputT>> {
    private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;
    private final Coder<AccumT> accumCoder;
    private boolean isCleared = true;
    private AccumT accum;

    public InMemoryCombiningState(
      Combine.CombineFn<InputT, AccumT, OutputT> combineFn, Coder<AccumT> accumCoder) {
      this.combineFn = combineFn;
      accum = combineFn.createAccumulator();
      this.accumCoder = accumCoder;
    }

    @Override
    public InMemoryCombiningState<InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this CombiningValue.
      //LOG.info("Clear value: {}", accum);
      accum = combineFn.createAccumulator();
      isCleared = true;
    }

    @Override
    public OutputT read() {
      return combineFn.extractOutput(
          combineFn.mergeAccumulators(Arrays.asList(combineFn.createAccumulator(), accum)));
    }

    @Override
    public void add(InputT input) {
      isCleared = false;
      accum = combineFn.addInput(accum, input);
    }

    @Override
    public AccumT getAccum() {
      return accum;
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }

        @Override
        public Boolean read() {
          return isCleared;
        }
      };
    }

    @Override
    public void addAccum(AccumT accum) {
      isCleared = false;
      this.accum = combineFn.mergeAccumulators(Arrays.asList(this.accum, accum));
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(accumulators);
    }

    @Override
    public boolean isCleared() {
      return isCleared;
    }

    @Override
    public InMemoryCombiningState<InputT, AccumT, OutputT> copy() {
      InMemoryCombiningState<InputT, AccumT, OutputT> that =
          new InMemoryCombiningState<>(combineFn, accumCoder);
      if (!this.isCleared) {
        that.isCleared = this.isCleared;
        that.addAccum(uncheckedClone(accumCoder, accum));
      }
      return that;
    }
  }

  /** An {@link InMemoryState} implementation of {@link BagState}. */
  public static final class InMemoryBag<T> implements BagState<T>, InMemoryState<InMemoryBag<T>> {
    private final Coder<T> elemCoder;
    private List<T> contents = new ArrayList<>();

    public InMemoryBag(Coder<T> elemCoder) {
      this.elemCoder = elemCoder;
    }

    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this Bag.
      // The result of get/read below must be stable for the lifetime of the bundle within which it
      // was generated. In batch and direct runners the bundle lifetime can be
      // greater than the window lifetime, in which case this method can be called while
      // the result is still in use. We protect against this by hot-swapping instead of
      // clearing the contents.
      //LOG.info("Clear value: {}", contents);
      contents = new ArrayList<>();
    }

    @Override
    public InMemoryBag<T> readLater() {
      return this;
    }

    @Override
    public Iterable<T> read() {
      return contents;
    }

    @Override
    public void add(T input) {
      contents.add(input);
    }

    @Override
    public boolean isCleared() {
      return contents.isEmpty();
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }

        @Override
        public Boolean read() {
          return contents.isEmpty();
        }
      };
    }

    @Override
    public InMemoryBag<T> copy() {
      InMemoryBag<T> that = new InMemoryBag<>(elemCoder);
      for (T elem : this.contents) {
        that.contents.add(uncheckedClone(elemCoder, elem));
      }
      return that;
    }
  }

  /** An {@link InMemoryState} implementation of {@link SetState}. */
  public static final class InMemorySet<T> implements SetState<T>, InMemoryState<InMemorySet<T>> {
    private final Coder<T> elemCoder;
    private Set<T> contents = new HashSet<>();

    public InMemorySet(Coder<T> elemCoder) {
      this.elemCoder = elemCoder;
    }

    @Override
    public void clear() {
      //LOG.info("Clear value: {}", contents);
      contents = new HashSet<>();
    }

    @Override
    public ReadableState<Boolean> contains(T t) {
      return ReadableStates.immediate(contents.contains(t));
    }

    @Override
    public ReadableState<Boolean> addIfAbsent(T t) {
      boolean alreadyContained = contents.contains(t);
      contents.add(t);
      return ReadableStates.immediate(!alreadyContained);
    }

    @Override
    public void remove(T t) {
      contents.remove(t);
    }

    @Override
    public InMemorySet<T> readLater() {
      return this;
    }

    @Override
    public Iterable<T> read() {
      return ImmutableSet.copyOf(contents);
    }

    @Override
    public void add(T input) {
      contents.add(input);
    }

    @Override
    public boolean isCleared() {
      return contents.isEmpty();
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }

        @Override
        public Boolean read() {
          return contents.isEmpty();
        }
      };
    }

    @Override
    public InMemorySet<T> copy() {
      InMemorySet<T> that = new InMemorySet<>(elemCoder);
      for (T elem : this.contents) {
        that.contents.add(uncheckedClone(elemCoder, elem));
      }
      return that;
    }
  }

  /** An {@link InMemoryState} implementation of {@link MapState}. */
  public static final class InMemoryMap<K, V>
      implements MapState<K, V>, InMemoryState<InMemoryMap<K, V>> {
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;

    private Map<K, V> contents = new HashMap<>();

    public InMemoryMap(Coder<K> keyCoder, Coder<V> valueCoder) {
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    @Override
    public void clear() {
      contents = new HashMap<>();
    }

    @Override
    public ReadableState<V> get(K key) {
      return ReadableStates.immediate(contents.get(key));
    }

    @Override
    public void put(K key, V value) {
      contents.put(key, value);
    }

    @Override
    public ReadableState<V> putIfAbsent(K key, V value) {
      V v = contents.get(key);
      if (v == null) {
        v = contents.put(key, value);
      }

      return ReadableStates.immediate(v);
    }

    @Override
    public void remove(K key) {
      contents.remove(key);
    }

    private static class CollectionViewState<T> implements ReadableState<Iterable<T>> {
      private final Collection<T> collection;

      private CollectionViewState(Collection<T> collection) {
        this.collection = collection;
      }

      public static <T> CollectionViewState<T> of(Collection<T> collection) {
        return new CollectionViewState<>(collection);
      }

      @Override
      public Iterable<T> read() {
        return ImmutableList.copyOf(collection);
      }

      @Override
      public ReadableState<Iterable<T>> readLater() {
        return this;
      }
    }

    @Override
    public ReadableState<Iterable<K>> keys() {
      return CollectionViewState.of(contents.keySet());
    }

    @Override
    public ReadableState<Iterable<V>> values() {
      return CollectionViewState.of(contents.values());
    }

    @Override
    public ReadableState<Iterable<Map.Entry<K, V>>> entries() {
      return CollectionViewState.of(contents.entrySet());
    }

    @Override
    public boolean isCleared() {
      return contents.isEmpty();
    }

    @Override
    public InMemoryMap<K, V> copy() {
      InMemoryMap<K, V> that = new InMemoryMap<>(keyCoder, valueCoder);
      for (Map.Entry<K, V> entry : this.contents.entrySet()) {
        that.contents.put(
            uncheckedClone(keyCoder, entry.getKey()), uncheckedClone(valueCoder, entry.getValue()));
      }
      that.contents.putAll(this.contents);
      return that;
    }
  }

  /** Like {@link CoderUtils#clone} but without a checked exception. */
  private static <T> T uncheckedClone(Coder<T> coder, T value) {
    try {
      return CoderUtils.clone(coder, value);
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
  }
}
