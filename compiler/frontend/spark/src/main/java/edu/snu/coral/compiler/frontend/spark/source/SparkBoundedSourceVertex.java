package edu.snu.coral.compiler.frontend.spark.source;

import edu.snu.coral.common.ir.Readable;
import edu.snu.coral.common.ir.ReadablesWrapper;
import edu.snu.coral.common.ir.vertex.SourceVertex;
import edu.snu.coral.compiler.frontend.spark.sql.Dataset;
import org.apache.spark.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.plans.logical.CatalystSerde$;
import org.apache.spark.sql.catalyst.plans.logical.DeserializeToObject;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.types.DataType;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Iterator;
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
        readables.add(new SparkBoundedSourceReadable(partition, dataset));
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
    private final Iterable<T> iterable;

    /**
     * Constructor.
     * @param partition partition for this readable.
     * @param dataset dataset to read data from.
     */
    private SparkBoundedSourceReadable(final Partition partition, final Dataset<T> dataset) {
      final TaskContext emptyContext = TaskContext$.MODULE$.empty();

      final DataType objectType = dataset.exprEnc().deserializer().dataType();
      final LogicalPlan logicalPlan = dataset.logicalPlan();
      final DeserializeToObject deserialized =  CatalystSerde$.MODULE$.deserialize(logicalPlan, dataset.exprEnc());
      final SparkPlan plan = dataset.sparkSession().sessionState().executePlan(deserialized).executedPlan();

      final Iterator<InternalRow> rows = JavaConverters.asJavaIteratorConverter(
          ReadData.execute(plan, partition, emptyContext)).asJava();

      final List<T> list = new ArrayList<>();
      rows.forEachRemaining(row -> list.add((T) row.get(0, objectType)));
      this.iterable = list;
    }

    @Override
    public Iterable<T> read() {
      return iterable;
    }
  }
}
