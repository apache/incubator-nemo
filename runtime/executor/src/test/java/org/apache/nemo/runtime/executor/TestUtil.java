package org.apache.nemo.runtime.executor;

import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.Stage;

import java.util.ArrayList;
import java.util.List;

public final class TestUtil {
  public static List<String> generateTaskIds(final Stage stage) {
    final List<String> result = new ArrayList<>(stage.getParallelism());
    final int first_attempt = 0;
    for (int taskIndex = 0; taskIndex < stage.getParallelism(); taskIndex++) {
      result.add(RuntimeIdManager.generateTaskId(stage.getId(), taskIndex, first_attempt));
    }
    return result;
  }
}
