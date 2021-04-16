package org.apache.nemo.runtime.executor.common.tasks;

import org.apache.commons.lang3.tuple.Triple;

public interface  CRTaskExecutor extends TaskExecutor {

  // src -> edge -> dst
  void stopInputPipeIndex(Triple<String, String, String> triple);
  void startInputPipeIndex(Triple<String, String, String> triple);

  void setRerouting(final String originTask,
                    final String pairTaskId,
                    final String pairEdgeId,
                    final ReroutingState rstate);
}
