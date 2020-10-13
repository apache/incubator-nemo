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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * InMemoryStateInternalsFactory.
 * @param <K> key type
 */
public final class InMemoryStateInternalsFactory<K> implements StateInternalsFactory<K> {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryStateInternalsFactory.class.getName());
  private final Map<K, StateInternals> stateInternalMap = new HashMap<>();

  @Override
  public String toString() {
    return "StateInternalMap" + stateInternalMap;
  }

  @Override
  public StateInternals stateInternalsForKey(final K key) {
    stateInternalMap.putIfAbsent(key,
      InMemoryStateInternals.forKey(key));
    return stateInternalMap.get(key);
  }

  public Map<K, StateInternals> getStateInternalMap() {
    return stateInternalMap;
  }
}
