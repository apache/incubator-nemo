package org.apache.nemo.compiler.frontend.spark.core;

import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import scala.reflect.ClassTag$;

/**
 * @param <T> type of the broadcast data.
 */
public final class SparkBroadcast<T> extends org.apache.spark.broadcast.Broadcast<T> {
  private final long tag;

  SparkBroadcast(final long tag, final Class<T> classType) {
    super(tag, ClassTag$.MODULE$.apply(classType));
    this.tag = tag;
  }

  @Override
  public T getValue() {
    return (T) BroadcastManagerWorker.getStaticReference().get(tag);
  }

  @Override
  public void doUnpersist(final boolean blocking) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doDestroy(final boolean blocking) {
    throw new UnsupportedOperationException();
  }
}
