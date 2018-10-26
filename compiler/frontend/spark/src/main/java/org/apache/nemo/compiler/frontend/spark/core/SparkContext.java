package org.apache.nemo.compiler.frontend.spark.core;

import org.apache.nemo.compiler.frontend.spark.SparkBroadcastVariables;
import org.apache.nemo.compiler.frontend.spark.core.rdd.JavaRDD;
import org.apache.nemo.compiler.frontend.spark.core.rdd.RDD;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.List;

/**
 * Spark context wrapper for in Nemo.
 */
public final class SparkContext extends org.apache.spark.SparkContext {
  private static final Logger LOG = LoggerFactory.getLogger(SparkContext.class.getName());
  private final org.apache.spark.SparkContext sparkContext;

  /**
   * Constructor.
   */
  public SparkContext() {
    this.sparkContext = org.apache.spark.SparkContext.getOrCreate();
  }

  /**
   * Constructor with configuration.
   *
   * @param sparkConf spark configuration to wrap.
   */
  public SparkContext(final SparkConf sparkConf) {
    super(sparkConf);
    this.sparkContext = org.apache.spark.SparkContext.getOrCreate(sparkConf);
  }

  /**
   * Initiate a JavaRDD with the number of parallelism.
   *
   * @param seq        input data as list.
   * @param numSlices  number of slices (parallelism).
   * @param evidence   type of the initial element.
   * @return the newly initiated JavaRDD.
   */
  @Override
  public <T> RDD<T> parallelize(final Seq<T> seq,
                                final int numSlices,
                                final ClassTag<T> evidence) {
    final List<T> javaList = scala.collection.JavaConversions.seqAsJavaList(seq);
    return JavaRDD.of(this.sparkContext, javaList, numSlices).rdd();
  }

  @Override
  public <T> Broadcast<T> broadcast(final T data,
                                    final ClassTag<T> evidence) {
    final long id = SparkBroadcastVariables.register(data);
    return new SparkBroadcast<>(id, (Class<T>) data.getClass());
  }
}
