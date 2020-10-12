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

import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.state.State;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Keep track of states in {@link InMemoryStateInternals}. */
public final class NemoStateBackend {
  private final Map<StateNamespace, Map<StateTag, State>> map;

  public NemoStateBackend() {
    this.map = new ConcurrentHashMap<>();
  }

  public NemoStateBackend(final Map<StateNamespace, Map<StateTag, State>> map) {
    this.map = map;
  }

  public void clear() {
    map.clear();
  }

  public Map<StateNamespace, Map<StateTag, State>> getMap() {
    return map;
  }

  @Override
  public String toString() {
    return map.toString();
  }
}
