package org.apache.nemo.runtime.executor.data.partitioner;

import java.io.Serializable;

/**
 * This interface represents the way of partitioning output data from a source task.
 * It takes an element and designates key of {@link org.apache.nemo.runtime.executor.data.partition.Partition}
 * to write the element, according to the number of destination tasks, the key of each element, etc.
 * @param <K> the key type of the partition to write.
 */
public interface Partitioner<K extends Serializable> {

  /**
   * Divides the output data from a task into multiple blocks.
   *
   * @param element        the output element from a source task.
   * @return the key of the partition in the block to write the element.
   */
   K partition(Object element);
}
