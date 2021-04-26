package org.apache.nemo.runtime.executor.common.tasks;

import org.apache.commons.lang3.tuple.Triple;

public interface MergerTaskExecutor extends CRTaskExecutor {

  void receivePartialFinal(final boolean finalResult);
}
