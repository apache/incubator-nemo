package org.apache.nemo.runtime.executor.data.partitioner;

/**
 * An implementation of {@link Partitioner} which makes an output data
 * from a source task to a single {@link org.apache.nemo.runtime.executor.data.partition.Partition}.
 */
public final class IntactPartitioner implements Partitioner<Integer> {

  @Override
  public Integer partition(final Object element) {
    return 0;
  }
}
