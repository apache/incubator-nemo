package edu.snu.onyx.compiler.frontend.spark.transform;

import edu.snu.onyx.common.ir.OutputCollector;
import edu.snu.onyx.common.ir.vertex.transform.Transform;
import edu.snu.onyx.compiler.frontend.spark.core.java.JavaRDD;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Collect transform.
 * @param <T> type of data to collect.
 */
public final class CollectTransform<T> implements Transform<T, T> {
  private String filename;

  /**
   * Constructor.
   * @param filename file to keep the result in.
   */
  public CollectTransform(final String filename) {
    this.filename = filename;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> outputCollector) {
    this.filename = filename + JavaRDD.getResultId();
  }

  @Override
  public void onData(final Iterator<T> elements, final String srcVertexId) {
    // Write result to a temporary file.
    // TODO #740: remove this part, and make it properly transfer with executor.
    try {
      final FileOutputStream fos = new FileOutputStream(filename);
      final ObjectOutputStream oos = new ObjectOutputStream(fos);
      final List<T> list = new ArrayList<>();
      elements.forEachRemaining(list::add);
      oos.writeObject(list);
      oos.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
  }
}
