package org.apache.nemo.runtime.master;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class PrevLambdaScheduleMap {

  public final Map<String, ExecutorRepresenter> map = new ConcurrentHashMap<>();

  @Inject
  private PrevLambdaScheduleMap() {

  }
}
