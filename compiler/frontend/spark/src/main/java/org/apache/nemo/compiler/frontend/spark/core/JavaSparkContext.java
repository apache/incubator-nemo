package org.apache.nemo.compiler.frontend.spark.core;

import org.apache.nemo.compiler.frontend.spark.core.rdd.JavaRDD;
import org.apache.spark.SparkContext;

import java.util.List;

/**
 * Spark context wrapper for Java.
 */
public final class JavaSparkContext {
  private final SparkContext sparkContext;

  /**
   * Constructor.
   * @param sparkContext spark context to wrap.
   */
  public JavaSparkContext(final SparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  /**
   * Create a String {@link JavaRDD} from a text file path.
   *
   * @param path the path to read.
   * @return the RDD.
   */
  public JavaRDD<String> textFile(final String path) {
    return this.textFile(path, 1);
  }

  /**
   * Create a String {@link JavaRDD} from a text file path with specific minimum parallelism.
   *
   * @param path          the path to read.
   * @param minPartitions the minimum parallelism.
   * @return the RDD.
   */
  public JavaRDD<String> textFile(final String path, final int minPartitions) {
    return JavaRDD.of(sparkContext, minPartitions, path);
  }

  /**
   * Initiate a JavaRDD with the number of parallelism.
   *
   * @param list input data as list.
   * @param <T> type of the initial element.
   * @return the newly initiated JavaRDD.
   */
  public <T> JavaRDD<T> parallelize(final List<T> list) {
    return this.parallelize(list, 1);
  }

  /**
   * Initiate a JavaRDD with the number of parallelism.
   *
   * @param l input data as list.
   * @param slices number of slices (parallelism).
   * @param <T> type of the initial element.
   * @return the newly initiated JavaRDD.
   */
  public <T> JavaRDD<T> parallelize(final List<T> l, final int slices) {
    return JavaRDD.of(this.sparkContext, l, slices);
  }
}
