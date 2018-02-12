package edu.snu.coral.compiler.frontend.spark.source;

import edu.snu.coral.common.ir.Readable;
import edu.snu.coral.common.ir.ReadablesWrapper;
import edu.snu.coral.common.ir.vertex.SourceVertex;
import edu.snu.coral.compiler.frontend.spark.sql.BlockManager;
import edu.snu.coral.compiler.frontend.spark.sql.Dataset;
import edu.snu.coral.compiler.frontend.spark.sql.SparkEnv;
import org.apache.spark.*;
import org.apache.spark.SecurityManager;
import org.apache.spark.memory.MemoryManager;
import org.apache.spark.memory.UnifiedMemoryManager$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.rpc.RpcEnvConfig;
import org.apache.spark.rpc.netty.NettyRpcEnvFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.plans.logical.CatalystSerde$;
import org.apache.spark.sql.catalyst.plans.logical.DeserializeToObject;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.storage.BlockManagerMaster;
import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

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
    private final Partition partition;
    private final ExpressionEncoder<T> encoder;

    private final SparkConf conf;

    private final SparkPlan plan;
    private final List<RDD<InternalRow>> inputRDDs;
    private final Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> readFunction;

    /**
     * Constructor.
     * @param partition partition for this readable.
     * @param dataset dataset to read data from.
     */
    private SparkBoundedSourceReadable(final Partition partition, final Dataset<T> dataset) {
      this.partition = partition;
      this.encoder = dataset.exprEnc();

      this.conf = dataset.sparkSession().sparkContext().conf();

      final LogicalPlan logicalPlan = dataset.logicalPlan();
      final DeserializeToObject deserialized =  CatalystSerde$.MODULE$.deserialize(logicalPlan, dataset.exprEnc());
      this.plan = dataset.sparkSession().sessionState().executePlan(deserialized).executedPlan();

      final Tuple2<Seq<RDD<InternalRow>>, Function1<PartitionedFile, scala.collection.Iterator<InternalRow>>> tup =
          ReadData.getInputRDDs(plan);
      this.inputRDDs = JavaConverters.seqAsJavaListConverter(tup._1()).asJava();
      this.readFunction = tup._2;
    }

    @Override
    public Iterable<T> read() {
      final MemoryManager memoryManager = UnifiedMemoryManager$.MODULE$.apply(conf, 1);
      final RpcEnvConfig rpcConf = new RpcEnvConfig(conf, "rpc",
          conf.get("spark.driver.host"),
          conf.get("spark.driver.host"),
          Integer.valueOf(conf.get("spark.driver.port")),
          new SecurityManager(conf, Option.empty()),
          false);
      final RpcEnv rpcEnv = new NettyRpcEnvFactory().create(rpcConf);
      final BlockManagerMaster master = new BlockManagerMaster(null, conf, true);
      final BlockManager blockManager = new BlockManager(conf, master, memoryManager, rpcEnv);

      SparkEnv$.MODULE$.set(new SparkEnv(conf, blockManager));
      final TaskContext emptyContext = TaskContext$.MODULE$.empty();
      final DataType objectType = encoder.deserializer().dataType();

      final Iterator<InternalRow> rows = JavaConverters.asJavaIteratorConverter(
          ReadData.execute(plan, partition, emptyContext,
              JavaConverters.asScalaBufferConverter(inputRDDs).asScala().toSeq(), readFunction)).asJava();

      final List<T> list = new ArrayList<>();
      rows.forEachRemaining(row -> list.add((T) row.get(0, objectType)));
      return list;
    }
  }
}
