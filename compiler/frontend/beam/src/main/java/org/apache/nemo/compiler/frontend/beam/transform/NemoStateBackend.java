package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.State;
import org.apache.nemo.common.Pair;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// Backend of State

public final class NemoStateBackend {
  public final Map<StateNamespace, Map<StateTag, Pair<State, Coder>>> map;

  public NemoStateBackend() {
    this.map = new ConcurrentHashMap<>();
  }

  public NemoStateBackend(final Map<StateNamespace, Map<StateTag, Pair<State, Coder>>> map) {
    this.map = map;
  }

  public void clear() {
    map.clear();
  }

  @Override
  public String toString() {
    return map.toString();
  }
}
