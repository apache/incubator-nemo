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

/**
 * This class contains states of a specific key in stateful transformation.
 * @param <K> key type.
 */
public class InMemoryStateInternals<K> implements StateInternals {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryStateInternals.class.getName());
  public static <K> InMemoryStateInternals<K> forKey(final @Nullable K key,
                                                     final NemoStateBackend nemoStateBackend) {
    return new InMemoryStateInternals<>(key, nemoStateBackend);
  }

  private final @Nullable K key;

  private final NemoStateBackend nemoStateBackend;

  protected InMemoryStateInternals(final @Nullable K key,
                                   final NemoStateBackend nemoStateBackend) {
    this.key = key;
    this.nemoStateBackend = nemoStateBackend;
  }

  /**
   * Accessor for key.
   * @return key
   */
  @Override
  public @Nullable K getKey() {
    return key;
  }

  /**
   * Interface common to all in-memory state cells. Includes ability to see whether a cell has been
   * cleared and the ability to create a clone of the contents.
   * @param <T> state type
   */
  public interface InMemoryState<T extends InMemoryState<T>> {
    boolean isCleared();
    T copy();
  }

  private final StateTable inMemoryState =
      new StateTable() {
        @Override
        protected StateTag.StateBinder binderForNamespace(final StateNamespace namespace, final StateContext<?> c) {
          final Map<StateTag, Pair<State, Coder>> map =
            nemoStateBackend.map.getOrDefault(namespace, new ConcurrentHashMap<>());
          nemoStateBackend.map.putIfAbsent(namespace, map);
          final Map<StateTag, Pair<State, Coder>> m = nemoStateBackend.map.get(namespace);
          return new InMemoryStateBinder(namespace, m, c);
        }
      };

  /**
   * Accessor for inMemoryState.
   * @return inMemoryState
   */
  public StateTable getInMemoryState() {
    return inMemoryState;
  }

  /**
   * Clear inMemoryState and NemoStateBackend.
   */
  public void clear() {
    inMemoryState.clear();
    nemoStateBackend.clear();
  }

  /**
   * Return true if the given state is empty. This is used by the test framework to make sure that
   * the state has been properly cleaned up.
   */
  protected boolean isEmptyForTesting(final State state) {
    return ((InMemoryState<?>) state).isCleared();
  }

  /**
   * Return the corresponding state in state table.
   * @param namespace stateNamespace
   * @param address state Tag
   * @param c state Context
   * @param <T> state type
   * @return state in state table
   */
  @Override
  public <T extends State> T state(
    final StateNamespace namespace, final StateTag<T> address, final StateContext<?> c) {
    return inMemoryState.get(namespace, address, c);
  }

  /** A {@link StateBinder} that returns In Memory {@link State} objects. */
  public class InMemoryStateBinder implements StateTag.StateBinder {
    private final StateContext<?> c;
    private final Map<StateTag, Pair<State, Coder>> stateCoderMap;
    private final StateNamespace namespace;

    public InMemoryStateBinder(final StateNamespace namespace,
                               final Map<StateTag, Pair<State, Coder>> stateCoderMap,
                               final StateContext<?> c) {
      this.namespace = namespace;
      this.stateCoderMap = stateCoderMap;
      this.c = c;
    }

    /**
     * Return ValueState with {@link StateTag} in state table.
     * @param address state tag
     * @param coder coder for element
     * @param <T> element type
     * @return ValueState
     */
    @Override
    public <T> ValueState<T> bindValue(final StateTag<ValueState<T>> address, final Coder<T> coder) {
      if (stateCoderMap.containsKey(address)) {
        return (ValueState<T>) stateCoderMap.get(address).left();
      } else {
        final ValueState<T> state = new InMemoryValue<>(coder);
        stateCoderMap.put(address, Pair.of(state, new ValueStateCoder<>(coder)));
        return state;
      }
    }

    /**
     * Return BagState with {@link StateTag} in state table.
     * @param address state tag
     * @param elemCoder coder for element
     * @param <T> element type
     * @return BagState
     */
    @Override
    public <T> BagState<T> bindBag(final StateTag<BagState<T>> address, final Coder<T> elemCoder) {
      if (stateCoderMap.containsKey(address)) {
        return (BagState<T>) stateCoderMap.get(address).left();
      } else {
        final BagState<T> state = new InMemoryBag<>(elemCoder);
        stateCoderMap.put(address, Pair.of(state, new BagStateCoder<>(elemCoder)));
        return state;
      }
    }

    /**
     * Return SetState with {@link StateTag} in state table.
     * @param address state tag
     * @param elemCoder coder for element
     * @param <T> element type
     * @return SetState
     */
    @Override
    public <T> SetState<T> bindSet(final StateTag<SetState<T>> address, final Coder<T> elemCoder) {
      if (stateCoderMap.containsKey(address)) {
        return (SetState<T>) stateCoderMap.get(address).left();
      } else {
        final SetState<T> state = new InMemorySet<>(elemCoder);
        stateCoderMap.put(address, Pair.of(state, new SetStateCoder<>(elemCoder)));
        return state;
      }
    }

    /**
     * Return MapState with {@link StateTag} in state table.
     * @param address state tag
     * @param mapKeyCoder coder for key
     * @param mapValueCoder coder for value
     * @param <KeyT> key type
     * @param <ValueT> value type
     * @return MapState
     */
    @Override
    public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
        final StateTag<MapState<KeyT, ValueT>> address,
        final Coder<KeyT> mapKeyCoder,
        final Coder<ValueT> mapValueCoder) {
      if (stateCoderMap.containsKey(address)) {
        return (MapState<KeyT, ValueT>) stateCoderMap.get(address).left();
      } else {
        final MapState<KeyT, ValueT> state = new InMemoryMap<>(mapKeyCoder, mapValueCoder);
        stateCoderMap.put(address, Pair.of(state, new MapStateCoder<>(mapKeyCoder, mapValueCoder)));
        return state;
      }
    }

    /**
     * Return {@link CombiningState} with {@link StateTag} in state table.
     * If it doesn't exist in state table, create one and return it.
     * @param address state tag
     * @param accumCoder coder for accumulator
     * @param combineFn combine function
     * @param <InputT> input type
     * @param <AccumT> accumulator type
     * @param <OutputT> output type
     * @return CombiningState
     */
    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
        final StateTag<CombiningState<InputT, AccumT, OutputT>> address,
        final Coder<AccumT> accumCoder,
        final Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
      if (stateCoderMap.containsKey(address)) {
        return (CombiningState<InputT, AccumT, OutputT>) stateCoderMap.get(address).left();
      } else {
        final CombiningState<InputT, AccumT, OutputT> state = new InMemoryCombiningState<>(combineFn, accumCoder);
        stateCoderMap.put(address, Pair.of(state, new CombiningStateCoder<>(accumCoder, combineFn)));
        return state;
      }
    }

    /**
     * Return {@link WatermarkHoldState} with {@link StateTag} in state table.
     * If it doesn't exist in state table, create one and return it.
     * @param address state tag
     * @param timestampCombiner timestamp combiner
     * @return WatermarkHoldState
     */
    @Override
    public WatermarkHoldState bindWatermark(
        final StateTag<WatermarkHoldState> address, final TimestampCombiner timestampCombiner) {
      if (stateCoderMap.containsKey(address)) {
        return (WatermarkHoldState) stateCoderMap.get(address).left();
      } else {
        final WatermarkHoldState state = new InMemoryWatermarkHold(timestampCombiner);
        stateCoderMap.put(address, Pair.of(state, WatermarkHoldStateCoder.getInstance()));
        return state;
      }
    }

    /**
     * Return {@link CombiningState} with {@link StateTag} in state table.
     * If it doesn't exist in state table, create one and return it.
     * @param address state tag
     * @param accumCoder coder for accumulator
     * @param combineFn combine function
     * @param <InputT> input type
     * @param <AccumT> accumulator type
     * @param <OutputT> output type
     * @return CombiningState
     */
    @Override
    public <InputT, AccumT, OutputT>
        CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
            final StateTag<CombiningState<InputT, AccumT, OutputT>> address,
            final Coder<AccumT> accumCoder,
            final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
      return bindCombiningValue(address, accumCoder, CombineFnUtil.bindContext(combineFn, c));
    }
  }

  /**
   * An {@link InMemoryState} implementation of {@link ValueState}.
   * @param <T> element type stored in {@link ValueState}
   */
  public static final class InMemoryValue<T>
      implements ValueState<T>, InMemoryState<InMemoryValue<T>> {
    private final Coder<T> coder;

    private boolean isCleared = true;
    private @Nullable T value = null;

    public InMemoryValue(final Coder<T> coder) {
      this.coder = coder;
    }

    /** Clear the {@link InMemoryValue}. */
    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this Value.
      value = null;
      isCleared = true;
    }

    /** Fetch the {@link InMemoryValue} before reading it. */
    @Override
    public InMemoryValue<T> readLater() {
      return this;
    }

    /** Read the {@link InMemoryValue}. */
    @Override
    public T read() {
      return value;
    }

    /** Write to the {@link InMemoryValue}. */
    @Override
    public void write(final T input) {
      isCleared = false;
      this.value = input;
    }

    /** copy the {@link InMemoryValue}. */
    @Override
    public InMemoryValue<T> copy() {
      InMemoryValue<T> that = new InMemoryValue<>(coder);
      if (!this.isCleared) {
        that.isCleared = this.isCleared;
        that.value = uncheckedClone(coder, this.value);
      }
      return that;
    }

    /** Return true if the {@link InMemoryValue} is cleared. */
    @Override
    public boolean isCleared() {
      return isCleared;
    }
  }

  /**
   * An {@link InMemoryState} implementation of {@link WatermarkHoldState}.
   * @param <W> window type
   */
  public static final class InMemoryWatermarkHold<W extends BoundedWindow>
      implements WatermarkHoldState, InMemoryState<InMemoryWatermarkHold<W>> {

    private final TimestampCombiner timestampCombiner;

    @Nullable private Instant combinedHold = null;

    public InMemoryWatermarkHold(final TimestampCombiner timestampCombiner) {
      this.timestampCombiner = timestampCombiner;
    }

    /** Fetch the {@link InMemoryWatermarkHold} before reading it. */
    @Override
    public InMemoryWatermarkHold readLater() {
      return this;
    }

    /** Clear the {@link InMemoryWatermarkHold}. */
    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this WatermarkBagInternal.
      combinedHold = null;
    }

    /** Read the {@link InMemoryWatermarkHold}. */
    @Override
    public Instant read() {
      return combinedHold;
    }

    /**
     * Add outputTime to {@link InMemoryWatermarkHold}.
     * @param outputTime Instant to be combined
     */
    @Override
    public void add(final Instant outputTime) {
      combinedHold =
          combinedHold == null ? outputTime : timestampCombiner.combine(combinedHold, outputTime);
    }

    /** Return true if the {@link InMemoryWatermarkHold} is cleared. */
    @Override
    public boolean isCleared() {
      return combinedHold == null;
    }

    /** Returns {@link ReadableState}. */
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

    /** Accessor for timestampCombiner. */
    @Override
    public TimestampCombiner getTimestampCombiner() {
      return timestampCombiner;
    }

    /** Stringify the {@link InMemoryWatermarkHold}. */
    @Override
    public String toString() {
      return Objects.toString(combinedHold);
    }

    /** Copy the {@link InMemoryWatermarkHold}. */
    @Override
    public InMemoryWatermarkHold<W> copy() {
      InMemoryWatermarkHold<W> that = new InMemoryWatermarkHold<>(timestampCombiner);
      that.combinedHold = this.combinedHold;
      return that;
    }
  }

  /**
   * An {@link InMemoryState} implementation of {@link CombiningState}.
   * @param <InputT> input type
   * @param <AccumT> accumulator type
   * @param <OutputT> output type
   */
  public static final class InMemoryCombiningState<InputT, AccumT, OutputT>
      implements CombiningState<InputT, AccumT, OutputT>,
          InMemoryState<InMemoryCombiningState<InputT, AccumT, OutputT>> {
    private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;
    private final Coder<AccumT> accumCoder;
    private boolean isCleared = true;
    private AccumT accum;

    public InMemoryCombiningState(
      final Combine.CombineFn<InputT, AccumT, OutputT> combineFn, final Coder<AccumT> accumCoder) {
      this.combineFn = combineFn;
      accum = combineFn.createAccumulator();
      this.accumCoder = accumCoder;
    }

    /** Fetch the {@link InMemoryCombiningState} before reading it. */
    @Override
    public InMemoryCombiningState<InputT, AccumT, OutputT> readLater() {
      return this;
    }

    /** Clear the {@link InMemoryCombiningState}. */
    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this CombiningValue.
      accum = combineFn.createAccumulator();
      isCleared = true;
    }

    /** Read the {@link InMemoryCombiningState}. */
    @Override
    public OutputT read() {
      return combineFn.extractOutput(
          combineFn.mergeAccumulators(Arrays.asList(combineFn.createAccumulator(), accum)));
    }

    /** Add input to the {@link InMemoryCombiningState}. */
    @Override
    public void add(final InputT input) {
      isCleared = false;
      accum = combineFn.addInput(accum, input);
    }

    /** Accessor for accumulator. */
    @Override
    public AccumT getAccum() {
      return accum;
    }

    /** Returns {@link ReadableState}. */
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

    /** Add accumulator to the {@link InMemoryCombiningState}. */
    @Override
    public void addAccum(final AccumT accumulator) {
      isCleared = false;
      this.accum = combineFn.mergeAccumulators(Arrays.asList(this.accum, accumulator));
    }

    /**
     * Merge accumulators, including the one in the {@link InMemoryCombiningState},
     * into a single accumulator and return it.
     */
    @Override
    public AccumT mergeAccumulators(final Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(accumulators);
    }

    /** Return true if the {@link InMemoryCombiningState} is already cleared. */
    @Override
    public boolean isCleared() {
      return isCleared;
    }

    /** Copy the {@link InMemoryCombiningState}. */
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

  /**
   * An {@link InMemoryState} implementation of {@link BagState}.
   * @param <T> element type
   */
  public static final class InMemoryBag<T> implements BagState<T>, InMemoryState<InMemoryBag<T>> {
    private final Coder<T> elemCoder;
    private List<T> contents = new ArrayList<>();

    public InMemoryBag(final Coder<T> elemCoder) {
      this.elemCoder = elemCoder;
    }

    /** Clear the {@link InMemoryBag}. */
    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this Bag.
      // The result of get/read below must be stable for the lifetime of the bundle within which it
      // was generated. In batch and direct runners the bundle lifetime can be
      // greater than the window lifetime, in which case this method can be called while
      // the result is still in use. We protect against this by hot-swapping instead of
      // clearing the contents.
      contents = new ArrayList<>();
    }

    /** Fetch the {@link InMemoryBag} before reading it. */
    @Override
    public InMemoryBag<T> readLater() {
      return this;
    }

    /** Read the {@link InMemoryBag}. */
    @Override
    public Iterable<T> read() {
      return contents;
    }

    /** Add input to the {@link InMemoryBag}. */
    @Override
    public void add(final T input) {
      contents.add(input);
    }

    /** Check if the {@link InMemoryBag} is cleared. */
    @Override
    public boolean isCleared() {
      return contents.isEmpty();
    }

    /** Returns {@link ReadableState}. */
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

    /** Copy the {@link InMemoryBag}. */
    @Override
    public InMemoryBag<T> copy() {
      InMemoryBag<T> that = new InMemoryBag<>(elemCoder);
      for (T elem : this.contents) {
        that.contents.add(uncheckedClone(elemCoder, elem));
      }
      return that;
    }
  }

  /**
   * An {@link InMemoryState} implementation of {@link SetState}.
   * @param <T> element type
   */
  public static final class InMemorySet<T> implements SetState<T>, InMemoryState<InMemorySet<T>> {
    private final Coder<T> elemCoder;
    private Set<T> contents = new HashSet<>();

    public InMemorySet(final Coder<T> elemCoder) {
      this.elemCoder = elemCoder;
    }

    /** Clear the {@link InMemorySet}. */
    @Override
    public void clear() {
      contents = new HashSet<>();
    }

    /** Check if the {@link InMemorySet} contains {@param t}. */
    @Override
    public ReadableState<Boolean> contains(final T t) {
      return ReadableStates.immediate(contents.contains(t));
    }

    /** Add {@param t} into the {@link InMemorySet} if {@param t} is absent. */
    @Override
    public ReadableState<Boolean> addIfAbsent(final T t) {
      boolean alreadyContained = contents.contains(t);
      contents.add(t);
      return ReadableStates.immediate(!alreadyContained);
    }

    /** Remove {@param t} in the {@link InMemorySet}. */
    @Override
    public void remove(final T t) {
      contents.remove(t);
    }

    /** Fetch the {@link InMemorySet} before reading it. */
    @Override
    public InMemorySet<T> readLater() {
      return this;
    }

    /** Read the {@link InMemorySet}. */
    @Override
    public Iterable<T> read() {
      return ImmutableSet.copyOf(contents);
    }

    /** Add {@param input} to the {@link InMemorySet}. */
    @Override
    public void add(final T input) {
      contents.add(input);
    }

    /** Check if the {@link InMemorySet} is cleared. */
    @Override
    public boolean isCleared() {
      return contents.isEmpty();
    }

    /** Returns {@link ReadableState}. */
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

    /** Copy the {@link InMemorySet}. */
    @Override
    public InMemorySet<T> copy() {
      InMemorySet<T> that = new InMemorySet<>(elemCoder);
      for (T elem : this.contents) {
        that.contents.add(uncheckedClone(elemCoder, elem));
      }
      return that;
    }
  }

  /**
   * An {@link InMemoryState} implementation of {@link MapState}.
   * @param <K> key type
   * @param <V> value type
   */
  public static final class InMemoryMap<K, V>
      implements MapState<K, V>, InMemoryState<InMemoryMap<K, V>> {
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;

    private Map<K, V> contents = new HashMap<>();

    public InMemoryMap(final Coder<K> keyCoder, final Coder<V> valueCoder) {
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    /** Check if the {@link InMemoryMap} is cleared. */
    @Override
    public void clear() {
      contents = new HashMap<>();
    }

    /** Return {@link ReadableState}. */
    @Override
    public ReadableState<V> get(final K key) {
      return ReadableStates.immediate(contents.get(key));
    }

    /** Add key-value pair into the {@link InMemoryMap}. */
    @Override
    public void put(final K key, final V value) {
      contents.put(key, value);
    }

    /** Add key-value pair into the {@link InMemoryMap} if it is absent. */
    @Override
    public ReadableState<V> putIfAbsent(final K key, final V value) {
      V v = contents.get(key);
      if (v == null) {
        v = contents.put(key, value);
      }

      return ReadableStates.immediate(v);
    }

    /** Remove key and its corresponding value from the {@link InMemoryMap}. */
    @Override
    public void remove(final K key) {
      contents.remove(key);
    }

    /**
     * An implementation of {@link ReadableState}.
     * @param <T> element type
     */
    private static final class CollectionViewState<T> implements ReadableState<Iterable<T>> {
      private final Collection<T> collection;

      private CollectionViewState(final Collection<T> collection) {
        this.collection = collection;
      }

      public static <T> CollectionViewState<T> of(final Collection<T> collection) {
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

    /** Accessor for keys stored in {@link InMemoryMap}. */
    @Override
    public ReadableState<Iterable<K>> keys() {
      return CollectionViewState.of(contents.keySet());
    }

    /** Accessor for values stored in {@link InMemoryMap}. */
    @Override
    public ReadableState<Iterable<V>> values() {
      return CollectionViewState.of(contents.values());
    }

    /** Returns entrySet of key-value pairs stored in{@link InMemoryMap}. */
    @Override
    public ReadableState<Iterable<Map.Entry<K, V>>> entries() {
      return CollectionViewState.of(contents.entrySet());
    }

    /** Check if the {@link InMemoryMap} is cleared. */
    @Override
    public boolean isCleared() {
      return contents.isEmpty();
    }

    /** Copy the {@link InMemoryMap}. */
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
  private static <T> T uncheckedClone(final Coder<T> coder, final T value) {
    try {
      return CoderUtils.clone(coder, value);
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
  }
}
