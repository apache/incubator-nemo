package org.apache.nemo.compiler.frontend.spark.source;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.spark.Partition;
import org.apache.spark.SerializableWritable;
import org.apache.spark.rdd.HadoopPartition;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility methods for spark sources.
 */
final class SparkSourceUtil {
  /**
   * Empty constructor.
   */
  private SparkSourceUtil() {
    // Private constructor.
  }

  /**
   * Gets the source location of a Spark partition.
   *
   * @param partition the partition to get location.
   * @return a list of locations.
   * @throws RuntimeException if failed to get source location.
   */
  static List<String> getPartitionLocation(final Partition partition) {
    try {
      if (partition instanceof HadoopPartition) {
        final Field inputSplitField = partition.getClass().getDeclaredField("inputSplit");
        inputSplitField.setAccessible(true);
        final InputSplit inputSplit = (InputSplit) ((SerializableWritable) inputSplitField.get(partition)).value();

        final String[] splitLocations = inputSplit.getLocations();
        final List<String> parsedLocations = new ArrayList<>();

        for (final String loc : splitLocations) {
          final String canonicalHostName = InetAddress.getByName(loc).getCanonicalHostName();
          parsedLocations.add(canonicalHostName);
        }

        if (parsedLocations.size() == 1 && parsedLocations.get(0).equals("localhost")) {
          return Collections.emptyList();
        } else {
          return parsedLocations;
        }
      } else {
        return Collections.emptyList();
      }
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
