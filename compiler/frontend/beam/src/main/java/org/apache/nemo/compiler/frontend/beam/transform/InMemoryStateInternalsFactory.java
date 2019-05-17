package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;

import java.util.HashMap;
import java.util.Map;

/**
   * InMemoryStateInternalsFactory.
 */
public final class InMemoryStateInternalsFactory<K> implements StateInternalsFactory<K> {

  public final Map<K, StateInternals> stateInternalMap;
  public final Map<K, NemoStateBackend> stateBackendMap;

  public InMemoryStateInternalsFactory() {
    this.stateInternalMap = new HashMap<>();
    this.stateBackendMap = new HashMap<>();
  }

  public InMemoryStateInternalsFactory(
    final Map<K, StateInternals> stateInternalMap,
    final Map<K, NemoStateBackend> stateBackendMap) {
    this.stateInternalMap = stateInternalMap;
    this.stateBackendMap = stateBackendMap;
  }

  @Override
  public String toString() {
    return "StateBackend: " + stateBackendMap;
  }

  @Override
  public StateInternals stateInternalsForKey(final K key) {
    stateBackendMap.putIfAbsent(key, new NemoStateBackend());

    final NemoStateBackend stateBackend = stateBackendMap.get(key);

    stateInternalMap.putIfAbsent(key,
      InMemoryStateInternals.forKey(key, stateBackend));

    return stateInternalMap.get(key);
  }
}
