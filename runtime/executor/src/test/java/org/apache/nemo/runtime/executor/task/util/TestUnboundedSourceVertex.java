package org.apache.nemo.runtime.executor.task.util;

import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;

import java.util.List;

/**
 * Source vertex for unbounded source test.
 */
public final class TestUnboundedSourceVertex extends SourceVertex {

  @Override
  public boolean isBounded() {
    return false;
  }

  @Override
  public List<Readable> getReadables(int desiredNumOfSplits) throws Exception {
    return null;
  }

  @Override
  public void clearInternalStates() {

  }

  @Override
  public IRVertex getClone() {
    return null;
  }
}
