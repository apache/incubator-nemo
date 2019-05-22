package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.NemoTriple;
import org.apache.nemo.common.Pair;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TaskLocationMap implements Serializable {

  public enum LOC {
    VM,
    SF
  }

  // edgeId, taskIndex, src
  public final Map<NemoTriple<String, Integer, Boolean>, LOC> locationMap;

  @Inject
  private TaskLocationMap() {
    this.locationMap = new ConcurrentHashMap<>();
  }
}
