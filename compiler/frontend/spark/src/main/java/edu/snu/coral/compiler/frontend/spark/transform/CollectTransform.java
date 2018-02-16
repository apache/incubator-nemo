package edu.snu.coral.compiler.frontend.spark.transform;

import edu.snu.coral.common.ir.vertex.transform.Transform;
import edu.snu.coral.compiler.frontend.spark.core.java.JavaRDD;
import edu.snu.coral.common.ir.Pipe;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
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
  public void prepare(final Context context, final Pipe<T> p) {
    this.filename = filename + JavaRDD.getResultId();
  }

  @Override
  public void onData(final Object element) {
    // Write result to a temporary file.
    // TODO #740: remove this part, and make it properly transfer with executor.
    try {
      final FileOutputStream fos = new FileOutputStream(filename);
      final ObjectOutputStream oos = new ObjectOutputStream(fos);
      final List<T> list = new ArrayList<>();
      list.add((T) element);
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
