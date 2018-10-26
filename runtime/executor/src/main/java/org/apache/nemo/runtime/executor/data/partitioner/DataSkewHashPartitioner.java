package org.apache.nemo.runtime.executor.data.partitioner;

import org.apache.nemo.common.KeyExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

/**
 * An implementation of {@link Partitioner} which hashes output data from a source task appropriate to detect data skew.
 * It hashes data finer than {@link HashPartitioner}.
 * The elements will be hashed by their key, and applied "modulo" operation.
 *
 * When we need to split or recombine the output data from a task after it is stored,
 * we multiply the hash range with a multiplier, which is commonly-known by the source and destination tasks,
 * to prevent the extra deserialize - rehash - serialize process.
 * For more information, please check {@link org.apache.nemo.conf.JobConf.HashRangeMultiplier}.
 */
public final class DataSkewHashPartitioner implements Partitioner<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSkewHashPartitioner.class.getName());
  private final KeyExtractor keyExtractor;
  private final BigInteger hashRangeBase;
  private final int hashRange;

  /**
   * Constructor.
   *
   * @param hashRangeMultiplier the hash range multiplier.
   * @param dstParallelism      the number of destination tasks.
   * @param keyExtractor        the key extractor that extracts keys from elements.
   */
  public DataSkewHashPartitioner(final int hashRangeMultiplier,
                                 final int dstParallelism,
                                 final KeyExtractor keyExtractor) {
    this.keyExtractor = keyExtractor;
    // For this hash range, please check the description of HashRangeMultiplier in JobConf.
    // For actual hash range to use, we calculate a prime number right next to the desired hash range.
    this.hashRangeBase = new BigInteger(String.valueOf(dstParallelism * hashRangeMultiplier));
    this.hashRange = hashRangeBase.nextProbablePrime().intValue();
    LOG.info("hashRangeBase {} resulting hashRange {}", hashRangeBase, hashRange);
  }

  /**
   * @see Partitioner#partition(Object).
   */
  @Override
  public Integer partition(final Object element) {
    return Math.abs(keyExtractor.extractKey(element).hashCode() % hashRange);
  }
}
