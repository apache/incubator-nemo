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
 * @param <K> key type
 */
public final class InMemoryStateInternalsFactory<K> implements StateInternalsFactory<K> {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryStateInternalsFactory.class.getName());

  private Map<K, StateInternals> stateInternalMap;
  private Map<K, NemoStateBackend> stateBackendMap;

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

  public Map<K, StateInternals> getStateInternalMap() {
    return stateInternalMap;
  }

  public Map<K, NemoStateBackend> getStateBackendMap() {
    return stateBackendMap;
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

  // removing states. Consider accumulating mode. (Only remove states if it is the last pane in corresponding window)
  public void removeNamespaceForKey(final K key,
                                    final StateNamespace namespace,
                                    final Instant timestamp) {
    stateBackendMap.get(key).getMap().remove(namespace);
    stateBackendMap.get(key).getMap().remove(StateNamespaces.global());

    final Iterator<Map.Entry<StateNamespace, Map<StateTag, Pair<State, Coder>>>> iterator =
      stateBackendMap.get(key).getMap().entrySet().iterator();

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
    if (stateBackendMap.get(key).getMap().isEmpty()) {
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
