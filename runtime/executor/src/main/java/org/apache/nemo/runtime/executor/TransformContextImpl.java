package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;

import java.io.Serializable;
import java.util.Optional;

/**
 * Transform Context Implementation.
 */
public final class TransformContextImpl implements Transform.Context {
  private final BroadcastManagerWorker broadcastManagerWorker;
  private String data;

  /**
   * Constructor of Context Implementation.
   * @param broadcastManagerWorker for broadcast variables.
   */
  public TransformContextImpl(final BroadcastManagerWorker broadcastManagerWorker) {
    this.broadcastManagerWorker = broadcastManagerWorker;
    this.data = null;
  }

  @Override
  public Object getBroadcastVariable(final Serializable tag) {
    return broadcastManagerWorker.get(tag);
  }

  @Override
  public void setSerializedData(final String serializedData) {
    this.data = serializedData;
  }

  @Override
  public Optional<String> getSerializedData() {
    return Optional.ofNullable(this.data);
  }
}
