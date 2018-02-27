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
 *
 * @param <T> type of data to collect.
 */
public final class CollectTransform<T> implements Transform<T, T> {
  private String filename;
  private FileOutputStream fos;
  private ObjectOutputStream oos;
  private final List<T> list;

  /**
   * Constructor.
   *
   * @param filename file to keep the result in.
   */
  public CollectTransform(final String filename) {
    this.filename = filename;
    this.list = new ArrayList<>();
  }

  @Override
  public void prepare(final Context context, final Pipe<T> p) {
    this.filename = filename + JavaRDD.getResultId();
  }

  @Override
  public void onData(final Object element) {
    // Write result to a temporary file.
    // TODO #740: remove this part, and make it properly transfer with executor.
    list.add((T) element);
  }

  @Override
  public void close() {
    try {
      fos = new FileOutputStream(filename);
      oos = new ObjectOutputStream(fos);
      oos.writeObject(list);
      oos.close();
    } catch (Exception e) {
      throw new RuntimeException("Exception while file closing in CollectTransform " + e);
    }
  }
}
