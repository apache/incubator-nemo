/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.compiler.frontend.spark.core.rdd;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.util.List;

/**
 * Java RDD for pairs.
 * @param <K> key type.
 * @param <V> value type.
 */
public final class JavaPairRDD<K, V> extends org.apache.spark.api.java.JavaPairRDD<K, V> {

  private final RDD<Tuple2<K, V>> rdd;

  /**
   * Static method to create a JavaPairRDD object from {@link RDD}.
   *
   * @param rddFrom the RDD to parse.
   * @param <K>     type of the key.
   * @param <V>     type of the value.
   * @return the parsed JavaPairRDD object.
   */
  public static <K, V> JavaPairRDD<K, V> fromRDD(final RDD<Tuple2<K, V>> rddFrom) {
    return new JavaPairRDD<>(rddFrom);
  }

  @Override
  public JavaPairRDD<K, V> wrapRDD(final org.apache.spark.rdd.RDD<Tuple2<K, V>> rddFrom) {
    if (!(rddFrom instanceof RDD)) {
      throw new UnsupportedOperationException("Cannot wrap Spark RDD as Nemo RDD!");
    }
    return fromRDD((RDD<Tuple2<K, V>>) rddFrom);
  }

  @Override
  public RDD<Tuple2<K, V>> rdd() {
    return rdd;
  }

  /**
   * Constructor with existing nemo RDD.
   *
   * @param rdd the Nemo rdd to wrap.
   */
  JavaPairRDD(final RDD<Tuple2<K, V>> rdd) {
    super(rdd, ClassTag$.MODULE$.apply(Object.class), ClassTag$.MODULE$.apply(Object.class));

    this.rdd = rdd;
  }

  /**
   * @return the spark context.
   */
  public SparkContext getSparkContext() {
    return rdd.sparkContext();
  }

  /////////////// TRANSFORMATIONS ///////////////

  @Override
  public JavaPairRDD<K, V> reduceByKey(final Function2<V, V, V> func) {
    // Explicit conversion
    final PairRDDFunctions<K, V> pairRdd = RDD.rddToPairRDDFunctions(
        rdd, ClassTag$.MODULE$.apply(Object.class), ClassTag$.MODULE$.apply(Object.class), null);
    final RDD<Tuple2<K, V>> reducedRdd = pairRdd.reduceByKey(func);
    return JavaPairRDD.fromRDD(reducedRdd);
  }

  @Override
  public <R> JavaRDD<R> map(final Function<Tuple2<K, V>, R> f) {
    return rdd.map(f, ClassTag$.MODULE$.apply(Object.class)).toJavaRDD();
  }

  /////////////// ACTIONS ///////////////

  @Override
  public List<Tuple2<K, V>> collect() {
    return rdd.collectAsList();
  }

  //TODO#776: support unimplemented RDD transformation/actions.
}
