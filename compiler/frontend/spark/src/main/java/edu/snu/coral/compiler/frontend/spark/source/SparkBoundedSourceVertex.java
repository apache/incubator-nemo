package edu.snu.coral.compiler.frontend.spark.source;

import com.google.common.collect.Lists;
import edu.snu.coral.common.ir.Readable;
import edu.snu.coral.common.ir.ReadablesWrapper;
import edu.snu.coral.common.ir.vertex.SourceVertex;
import edu.snu.coral.compiler.frontend.spark.sql.Dataset;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.rdd.RDD;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;

/**
 * Bounded source vertex for Spark.
 * @param <T> type of data to read.
 */
public final class SparkBoundedSourceVertex<T> extends SourceVertex<T> {
  private final ReadablesWrapper<T> readablesWrapper;

  /**
   * Constructor.
   * Note that we have to first create our iterators here and supply them to our readables.
   * @param dataset Dataset to read data from.
   */
  public SparkBoundedSourceVertex(final Dataset<T> dataset) {
    this.readablesWrapper = new SparkBoundedSourceReadablesWrapper(dataset);
  }

  /**
   * Constructor.
   * @param readablesWrapper readables wrapper.
   */
  public SparkBoundedSourceVertex(final ReadablesWrapper<T> readablesWrapper) {
    this.readablesWrapper = readablesWrapper;
  }

  @Override
  public SparkBoundedSourceVertex getClone() {
    final SparkBoundedSourceVertex<T> that = new SparkBoundedSourceVertex<>((this.readablesWrapper));
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public ReadablesWrapper<T> getReadableWrapper(final int desiredNumOfSplits) {
    return readablesWrapper;
  }

  /**
   * A ReadablesWrapper for SparkBoundedSourceVertex.
   */
  private final class SparkBoundedSourceReadablesWrapper implements ReadablesWrapper<T> {
    private final List<Readable<T>> readables;

    /**
     * Constructor.
     * @param dataset dataset to read data from.
     */
    private SparkBoundedSourceReadablesWrapper(final Dataset<T> dataset) {
      this.readables = new ArrayList<>();
      for (final Partition partition: dataset.rdd().getPartitions()) {
        readables.add(new SparkBoundedSourceReadable(partition, dataset.rdd()));
      }
    }

    @Override
    public List<Readable<T>> getReadables() {
      return readables;
    }
  }

  /**
   * A Readable for SparkBoundedSourceReadablesWrapper.
   */
  private final class SparkBoundedSourceReadable implements Readable<T> {
    private final SparkConf conf;
    private final Iterable<T> iterable;

    /**
     * Constructor.
     * @param partition partition for this readable.
     * @param rdd rdd to read data from.
     */
    private SparkBoundedSourceReadable(final Partition partition, final RDD<T> rdd) {
      this.conf = rdd.sparkContext().conf();
      // TODO #756: make this bit distributed.
      this.iterable = Lists.newArrayList(() ->
          JavaConverters.asJavaIteratorConverter(rdd.iterator(partition, TaskContext$.MODULE$.empty())).asJava());
    }

    @Override
    public Iterable<T> read() {
      new SparkContext(conf);
      return iterable;
    }
  }
}
