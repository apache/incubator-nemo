package org.apache.nemo.runtime.executor.data.partition;

import java.io.IOException;

/**
 * A collection of data elements.
 * This is a unit of read / write towards {@link org.apache.nemo.runtime.executor.data.block.Block}s.
 * @param <T> the type of the data stored in this {@link Partition}.
 * @param <K> the type of key used for {@link Partition}.
 */
public interface Partition<T, K> {

  /**
   * Writes an element to non-committed partition.
   *
   * @param element element to write.
   * @throws IOException if the partition is already committed.
   */
  void write(Object element) throws IOException;

  /**
   * Commits a partition to prevent further data write.
   * @throws IOException if fail to commit partition.
   */
  void commit() throws IOException;

  /**
   * @return the key value.
   */
  K getKey();

  /**
   * @return whether the data in this {@link Partition} is serialized or not.
   */
  boolean isSerialized();

  /**
   * @return the data in this {@link Partition}.
   * @throws IOException if the partition is not committed yet.
   */
  T getData() throws IOException;
}
