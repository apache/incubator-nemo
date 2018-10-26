package org.apache.nemo.examples.spark;

import org.apache.nemo.compiler.frontend.spark.core.rdd.JavaRDD;
import org.apache.nemo.compiler.frontend.spark.core.JavaSparkContext;
import org.apache.nemo.compiler.frontend.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Computes an approximation to pi.
 * Usage: JavaSparkPi [partitions]
 */
public final class JavaSparkPi {

  /**
   * Private constructor.
   */
  private JavaSparkPi() {
  }

  /**
   * Main method.
   * @param args arguments.
   * @throws Exception exceptions.
   */
  public static void main(final String[] args) throws Exception {
    SparkSession spark = SparkSession
        .builder()
        .appName("JavaSparkPi")
        .getOrCreate();

    final JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
    int n = 100000 * slices;
    List<Integer> l = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      l.add(i);
    }

    JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

    int count = dataSet.map(integer -> {
      double x = Math.random() * 2 - 1;
      double y = Math.random() * 2 - 1;
      return (x * x + y * y <= 1) ? 1 : 0;
    }).reduce((integer, integer2) -> integer + integer2);

    System.out.println("Pi is roughly " + 4.0 * count / n);

    spark.stop();
  }
}
