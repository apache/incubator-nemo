package org.apache.nemo.compiler.frontend.spark.source;

import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.compiler.frontend.spark.sql.Dataset;
import org.apache.nemo.compiler.frontend.spark.sql.SparkSession;
import org.apache.spark.*;
import org.apache.spark.rdd.RDD;
import scala.collection.JavaConverters;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.util.*;

/**
 * Bounded source vertex for Spark Dataset.
 * @param <T> type of data to read.
 */
public final class SparkDatasetBoundedSourceVertex<T> extends SourceVertex<T> {
  private List<Readable<T>> readables;

  /**
   * Constructor.
   *
   * @param sparkSession sparkSession to recreate on each executor.
   * @param dataset      Dataset to read data from.
   */
  public SparkDatasetBoundedSourceVertex(final SparkSession sparkSession, final Dataset<T> dataset) {
    super();
    this.readables = new ArrayList<>();
    final RDD rdd = dataset.sparkRDD();
    final Partition[] partitions = rdd.getPartitions();
    for (int i = 0; i < partitions.length; i++) {
      readables.add(new SparkDatasetBoundedSourceReadable(
          partitions[i],
          sparkSession.getDatasetCommandsList(),
          sparkSession.getInitialConf(),
          i));
    }
  }

  /**
   * Copy Constructor for SparkDatasetBoundedSourceVertex.
   *
   * @param that the source object for copying
   */
  public SparkDatasetBoundedSourceVertex(final SparkDatasetBoundedSourceVertex<T> that) {
    super(that);
    this.readables = new ArrayList<>();
    that.readables.forEach(this.readables::add);
  }

  @Override
  public SparkDatasetBoundedSourceVertex getClone() {
    return new SparkDatasetBoundedSourceVertex(this);
  }

  @Override
  public List<Readable<T>> getReadables(final int desiredNumOfSplits) {
    return readables;
  }

  @Override
  public void clearInternalStates() {
    readables = null;
  }

  /**
   * A Readable wrapper for Spark Dataset.
   */
  private final class SparkDatasetBoundedSourceReadable implements Readable<T> {
    private final LinkedHashMap<String, Object[]> commands;
    private final Map<String, String> sessionInitialConf;
    private final int partitionIndex;
    private final List<String> locations;

    /**
     * Constructor.
     *
     * @param partition          the partition to wrap.
     * @param commands           list of commands needed to build the dataset.
     * @param sessionInitialConf spark session's initial configuration.
     * @param partitionIndex     partition for this readable.
     */
    private SparkDatasetBoundedSourceReadable(final Partition partition,
                                              final LinkedHashMap<String, Object[]> commands,
                                              final Map<String, String> sessionInitialConf,
                                              final int partitionIndex) {
      this.commands = commands;
      this.sessionInitialConf = sessionInitialConf;
      this.partitionIndex = partitionIndex;
      this.locations = SparkSourceUtil.getPartitionLocation(partition);
    }

    @Override
    public Iterable<T> read() throws IOException {
      // for setting up the same environment in the executors.
      final SparkSession spark = SparkSession.builder()
          .config(sessionInitialConf)
          .getOrCreate();
      final Dataset<T> dataset;

      try {
        dataset = SparkSession.initializeDataset(spark, commands);
      } catch (final OperationNotSupportedException e) {
        throw new IllegalStateException(e);
      }

      // Spark does lazy evaluation: it doesn't load the full dataset, but only the partition it is asked for.
      final RDD<T> rdd = dataset.sparkRDD();
      return () -> JavaConverters.asJavaIteratorConverter(
          rdd.iterator(rdd.getPartitions()[partitionIndex], TaskContext$.MODULE$.empty())).asJava();
    }

    @Override
    public List<String> getLocations() {
      if (locations.isEmpty()) {
        throw new UnsupportedOperationException();
      } else {
        return locations;
      }
    }
  }
}
