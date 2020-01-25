package org.apache.nemo.common;

import org.apache.nemo.common.TaskLoc;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TaskLocationMap implements Serializable {

  // edgeId, taskIndex, src
  public final Map<String, TaskLoc> locationMap;

  @Inject
  public TaskLocationMap() {
    this.locationMap = new ConcurrentHashMap<>();
  }
}
