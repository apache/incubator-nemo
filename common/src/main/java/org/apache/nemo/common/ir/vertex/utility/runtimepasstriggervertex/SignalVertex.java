package org.apache.nemo.common.ir.vertex.utility.runtimepasstriggervertex;

import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.MessageIdVertexProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.SignalTransform;

import java.util.concurrent.atomic.AtomicInteger;

public final class SignalVertex extends OperatorVertex {
  private static final AtomicInteger MESSAGE_ID_GENERATOR = new AtomicInteger(0);

  public SignalVertex() {
    super(new SignalTransform());
    this.setPropertyPermanently(MessageIdVertexProperty.of(MESSAGE_ID_GENERATOR.incrementAndGet()));
    this.setPropertyPermanently(ParallelismProperty.of(1));
  }
}
