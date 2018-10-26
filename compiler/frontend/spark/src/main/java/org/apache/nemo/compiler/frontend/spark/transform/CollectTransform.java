package org.apache.nemo.compiler.frontend.spark.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.commons.lang3.SerializationUtils;

import java.util.ArrayList;
import java.util.Base64;

/**
 * Collect transform.
 * @param <T> type of data to collect.
 */
public final class CollectTransform<T> implements Transform<T, T> {
  private final ArrayList<T> list;
  private Context ctxt;

  /**
   * Constructor.
   */
  public CollectTransform() {
    this.list = new ArrayList<>();
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> oc) {
    this.ctxt = context;
  }

  @Override
  public void onData(final T element) {
    list.add(element);
  }

  @Override
  public void close() {
    ctxt.setSerializedData(Base64.getEncoder().encodeToString(SerializationUtils.serialize(list)));
  }
}
