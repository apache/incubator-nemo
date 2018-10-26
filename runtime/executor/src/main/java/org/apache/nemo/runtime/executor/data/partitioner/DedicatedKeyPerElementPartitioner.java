package org.apache.nemo.runtime.executor.data.partitioner;

/**
 * An implementation of {@link Partitioner} which assigns a dedicated key per an output data from a task.
 * WARNING: Because this partitioner assigns a dedicated key per element, it should be used under specific circumstances
 * that the number of output element is not that many. For example, every output element of
 * {@link org.apache.nemo.common.ir.vertex.transform.RelayTransform} inserted by large shuffle optimization is always
 * a partition. In this case, assigning a key for each element can be useful.
 */
@DedicatedKeyPerElement
public final class DedicatedKeyPerElementPartitioner implements Partitioner<Integer> {
  private int key;

  /**
   * Constructor.
   */
  public DedicatedKeyPerElementPartitioner() {
    key = 0;
  }

  @Override
  public Integer partition(final Object element) {
    return key++;
  }
}
