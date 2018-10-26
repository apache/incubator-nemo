package org.apache.nemo.runtime.executor.data.partition;

import org.apache.nemo.runtime.executor.data.DataUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A collection of data elements. The data is stored as an iterable of elements.
 * This is a unit of read / write towards {@link org.apache.nemo.runtime.executor.data.block.Block}s.
 * @param <K> the key type of its partitions.
 */
public final class NonSerializedPartition<K> implements Partition<Iterable, K> {
  private final K key;
  private final List nonSerializedData;
  private final long numSerializedBytes;
  private final long numEncodedBytes;
  private volatile boolean committed;

  /**
   * Creates a non-serialized {@link Partition} without actual data.
   * Data can be written to this partition until it is committed.
   *
   * @param key the key of this partition.
   */
  public NonSerializedPartition(final K key) {
    this.key = key;
    this.nonSerializedData = new ArrayList();
    this.numSerializedBytes = -1;
    this.numEncodedBytes = -1;
    this.committed = false;
  }

  /**
   * Creates a non-serialized {@link Partition} with actual data.
   * Data cannot be written to this partition after the construction.
   *
   * @param key                the key.
   * @param data               the non-serialized data.
   * @param numSerializedBytes the number of bytes in serialized form (which is, for example, encoded and compressed)
   * @param numEncodedBytes    the number of bytes in encoded form (which is ready to be decoded)
   */
  public NonSerializedPartition(final K key,
                                final List data,
                                final long numSerializedBytes,
                                final long numEncodedBytes) {
    this.key = key;
    this.nonSerializedData = data;
    this.numSerializedBytes = numSerializedBytes;
    this.numEncodedBytes = numEncodedBytes;
    this.committed = true;
  }

  /**
   * Writes an element to non-committed partition.
   *
   * @param element element to write.
   * @throws IOException if the partition is already committed.
   */
  @Override
  public void write(final Object element) throws IOException {
    if (committed) {
      throw new IOException("The partition is already committed!");
    } else {
      nonSerializedData.add(element);
    }
  }

  /**
   * Commits a partition to prevent further data write.
   */
  @Override
  public void commit() {
    this.committed = true;
  }

  /**
   * @return the number of bytes in serialized form (which is, for example, encoded and compressed)
   * @throws org.apache.nemo.runtime.executor.data.DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException
   *         when then information is not available
   */
  public long getNumSerializedBytes() throws DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException {
    if (numSerializedBytes == -1) {
      throw new DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException();
    }
    return numSerializedBytes;
  }

  /**
   * @return the number of bytes in encoded form (which is ready to be decoded)
   * @throws org.apache.nemo.runtime.executor.data.DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException
   *         when then information is not available
   */
  public long getNumEncodedBytes() throws DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException {
    if (numEncodedBytes == -1) {
      throw new DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException();
    }
    return numEncodedBytes;
  }

  /**
   * @return the key value.
   */
  @Override
  public K getKey() {
    return key;
  }

  /**
   * @return whether the data in this {@link Partition} is serialized or not.
   */
  @Override
  public boolean isSerialized() {
    return false;
  }

  /**
   * @return the non-serialized data.
   * @throws IOException if the partition is not committed yet.
   */
  @Override
  public Iterable getData() throws IOException {
    if (!committed) {
      throw new IOException("The partition is not committed yet!");
    } else {
      return nonSerializedData;
    }
  }
}
