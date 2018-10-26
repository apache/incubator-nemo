package org.apache.nemo.runtime.executor.data.partitioner;

import org.apache.nemo.common.KeyExtractor;

/**
 * An implementation of {@link Partitioner} which hashes output data from a source task
 * according to the key of elements.
 * The data will be hashed by their key, and applied "modulo" operation by the number of destination tasks.
 */
public final class HashPartitioner implements Partitioner<Integer> {
  private final KeyExtractor keyExtractor;
  private final int dstParallelism;

  /**
   * Constructor.
   *
   * @param dstParallelism the number of destination tasks.
   * @param keyExtractor   the key extractor that extracts keys from elements.
   */
  public HashPartitioner(final int dstParallelism,
                         final KeyExtractor keyExtractor) {
    this.keyExtractor = keyExtractor;
    this.dstParallelism = dstParallelism;
  }

  @Override
  public Integer partition(final Object element) {
    return Math.abs(keyExtractor.extractKey(element).hashCode() % dstParallelism);
  }
}
