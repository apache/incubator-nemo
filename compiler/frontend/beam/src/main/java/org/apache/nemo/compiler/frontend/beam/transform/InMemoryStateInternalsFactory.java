package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.*;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.State;
import org.apache.nemo.common.Pair;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
   * InMemoryStateInternalsFactory.
 */
public final class InMemoryStateInternalsFactory<K> implements StateInternalsFactory<K> {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryStateInternalsFactory.class.getName());

  public Map<K, StateInternals> stateInternalMap;
  public Map<K, NemoStateBackend> stateBackendMap;

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

  public void setState(final InMemoryStateInternalsFactory<K> stateFactorty) {

    /*
    this.stateInternalMap.clear();
    this.stateInternalMap.putAll(stateFactorty.stateInternalMap);

    this.stateBackendMap.clear();
    this.stateBackendMap.putAll(stateFactorty.stateBackendMap);
    */

    this.stateInternalMap = stateFactorty.stateInternalMap;
    this.stateBackendMap = stateFactorty.stateBackendMap;
  }

  public void removeNamespaceForKey(final K key, StateNamespace namespace, final Instant timestamp) {

    //LOG.info("Remove namespace for key {}/{}", key, namespace);

    stateBackendMap.get(key).map.remove(namespace);
    stateBackendMap.get(key).map.remove(StateNamespaces.global());

    final Iterator<Map.Entry<StateNamespace, Map<StateTag, Pair<State, Coder>>>> iterator = stateBackendMap.get(key).map.entrySet().iterator();

    while (iterator.hasNext()) {
      final Map.Entry<StateNamespace, Map<StateTag, Pair<State, Coder>>> elem = iterator.next();
      final StateNamespace stateNamespace = elem.getKey();

      if (stateNamespace instanceof StateNamespaces.WindowNamespace) {
        final StateNamespaces.WindowNamespace windowNamespace = (StateNamespaces.WindowNamespace) stateNamespace;
        if (windowNamespace.getWindow().maxTimestamp().isBefore(timestamp)
          || windowNamespace.getWindow().maxTimestamp().isEqual(timestamp)) {

          iterator.remove();
        }
      }
    }

    // LOG.info("Remaining key {}, timestamp {} / {}", key, timestamp, stateBackendMap.get(key).map.keySet());

    if (stateBackendMap.get(key).map.isEmpty()) {
      //LOG.info("Remove key: {}", key);
      // remove key
      stateBackendMap.remove(key);
      stateInternalMap.remove(key);
    }
  }

  public int getNumKeys() {
    return stateInternalMap.size();
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
