package org.apache.nemo.runtime.executor.data.metadata;

import java.io.Serializable;

/**
 * This class represents a metadata for a partition.
 * @param <K> the key type of its partitions.
 */
public final class PartitionMetadata<K extends Serializable> {
  private final K key;
  private final int partitionSize;
  private final long offset;

  /**
   * Constructor.
   *
   * @param key           the key of this partition.
   * @param partitionSize the size of this partition.
   * @param offset        the offset of this partition.
   */
  public PartitionMetadata(final K key,
                           final int partitionSize,
                           final long offset) {
    this.key = key;
    this.partitionSize = partitionSize;
    this.offset = offset;
  }

  /**
   * @return the key of this partition.
   */
  public K getKey() {
    return key;
  }

  /**
   * @return the size of this partition.
   */
  public int getPartitionSize() {
    return partitionSize;
  }

  /**
   * @return the offset of this partition.
   */
  public long getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("key: ");
    sb.append(key);
    sb.append("/ partitionSize: ");
    sb.append(partitionSize);
    sb.append("/ offset: ");
    sb.append(offset);
    return sb.toString();
  }
}
