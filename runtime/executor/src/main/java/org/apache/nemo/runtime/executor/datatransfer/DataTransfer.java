package org.apache.nemo.runtime.executor.datatransfer;


/**
 * Contains common parts involved in {@link InputReader} and {@link OutputWriter}.
 * The two classes are involved in
 * intermediate data transfer between {@link org.apache.nemo.runtime.common.plan.physical.Task}.
 */
public abstract class DataTransfer {
  private final String id;

  public DataTransfer(final String id) {
    this.id = id;
  }

  /**
   * @return ID of the reader/writer.
   */
  public final String getId() {
    return id;
  }
}
