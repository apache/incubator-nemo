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

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.reflect.ClassTag$;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

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

  /////////////// UNSUPPORTED METHODS ///////////////
  //TODO#92: Implement the unimplemented transformations/actions & dataset initialization methods for Spark frontend.

  @Override
  public JavaPairRDD<K, V> cache() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> persist(final StorageLevel newLevel) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> unpersist() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> unpersist(final boolean blocking) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> distinct() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> distinct(final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> filter(final Function<Tuple2<K, V>, Boolean> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> coalesce(final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> coalesce(final int numPartitions,
                                    final boolean shuffle) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> repartition(final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> sample(final boolean withReplacement,
                                  final double fraction) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> sample(final boolean withReplacement,
                                  final double fraction,
                                  final long seed) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> sampleByKey(final boolean withReplacement,
                                       final Map<K, Double> fractions,
                                       final long seed) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> sampleByKey(final boolean withReplacement,
                                       final Map<K, Double> fractions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> sampleByKeyExact(final boolean withReplacement,
                                            final Map<K, Double> fractions,
                                            final long seed) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> sampleByKeyExact(final boolean withReplacement,
                                            final Map<K, Double> fractions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> union(final org.apache.spark.api.java.JavaPairRDD<K, V> other) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> intersection(final org.apache.spark.api.java.JavaPairRDD<K, V> other) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public Tuple2<K, V> first() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <C> JavaPairRDD<K, C> combineByKey(final Function<V, C> createCombiner,
                                            final Function2<C, V, C> mergeValue,
                                            final Function2<C, C, C> mergeCombiners,
                                            final Partitioner partitioner,
                                            final boolean mapSideCombine,
                                            final Serializer serializer) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <C> JavaPairRDD<K, C> combineByKey(final Function<V, C> createCombiner,
                                            final Function2<C, V, C> mergeValue,
                                            final Function2<C, C, C> mergeCombiners,
                                            final Partitioner partitioner) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <C> JavaPairRDD<K, C> combineByKey(final Function<V, C> createCombiner,
                                            final Function2<C, V, C> mergeValue,
                                            final Function2<C, C, C> mergeCombiners,
                                            final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> reduceByKey(final Partitioner partitioner,
                                       final Function2<V, V, V> func) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public Map<K, V> reduceByKeyLocally(final Function2<V, V, V> func) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public Map<K, Long> countByKey() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public PartialResult<Map<K, BoundedDouble>> countByKeyApprox(final long timeout) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public PartialResult<Map<K, BoundedDouble>> countByKeyApprox(final long timeout,
                                                               final double confidence) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public double countByKeyApprox$default$2() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> JavaPairRDD<K, U> aggregateByKey(final U zeroValue,
                                              final Partitioner partitioner,
                                              final Function2<U, V, U> seqFunc,
                                              final Function2<U, U, U> combFunc) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> JavaPairRDD<K, U> aggregateByKey(final U zeroValue,
                                              final int numPartitions,
                                              final Function2<U, V, U> seqFunc,
                                              final Function2<U, U, U> combFunc) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> JavaPairRDD<K, U> aggregateByKey(final U zeroValue,
                                              final Function2<U, V, U> seqFunc,
                                              final Function2<U, U, U> combFunc) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> foldByKey(final V zeroValue,
                                     final Partitioner partitioner,
                                     final Function2<V, V, V> func) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> foldByKey(final V zeroValue,
                                     final int numPartitions,
                                     final Function2<V, V, V> func) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> foldByKey(final V zeroValue,
                                     final Function2<V, V, V> func) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> reduceByKey(final Function2<V, V, V> func,
                                       final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, Iterable<V>> groupByKey(final Partitioner partitioner) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, Iterable<V>> groupByKey(final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> subtract(final org.apache.spark.api.java.JavaPairRDD<K, V> other) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> subtract(final org.apache.spark.api.java.JavaPairRDD<K, V> other,
                                    final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> subtract(final org.apache.spark.api.java.JavaPairRDD<K, V> other,
                                    final Partitioner p) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, V> subtractByKey(final org.apache.spark.api.java.JavaPairRDD<K, W> other) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, V> subtractByKey(final org.apache.spark.api.java.JavaPairRDD<K, W> other,
                                             final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, V> subtractByKey(final org.apache.spark.api.java.JavaPairRDD<K, W> other,
                                             final Partitioner p) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> partitionBy(final Partitioner partitioner) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<V, W>> join(final org.apache.spark.api.java.JavaPairRDD<K, W> other,
                                               final Partitioner partitioner) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<V, Optional<W>>>
  leftOuterJoin(final org.apache.spark.api.java.JavaPairRDD<K, W> other,
                final Partitioner partitioner) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<Optional<V>, W>>
  rightOuterJoin(final org.apache.spark.api.java.JavaPairRDD<K, W> other,
                 final Partitioner partitioner) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<Optional<V>, Optional<W>>>
  fullOuterJoin(final org.apache.spark.api.java.JavaPairRDD<K, W> other,
                final Partitioner partitioner) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <C> JavaPairRDD<K, C> combineByKey(final Function<V, C> createCombiner,
                                            final Function2<C, V, C> mergeValue,
                                            final Function2<C, C, C> mergeCombiners) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public org.apache.spark.api.java.JavaPairRDD<K, Iterable<V>> groupByKey() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<V, W>> join(final org.apache.spark.api.java.JavaPairRDD<K, W> other) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<V, W>> join(final org.apache.spark.api.java.JavaPairRDD<K, W> other,
                                               final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<V, Optional<W>>>
  leftOuterJoin(final org.apache.spark.api.java.JavaPairRDD<K, W> other) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<V, Optional<W>>>
  leftOuterJoin(final org.apache.spark.api.java.JavaPairRDD<K, W> other,
                final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<Optional<V>, W>>
  rightOuterJoin(final org.apache.spark.api.java.JavaPairRDD<K, W> other) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<Optional<V>, W>>
  rightOuterJoin(final org.apache.spark.api.java.JavaPairRDD<K, W> other,
                 final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<Optional<V>, Optional<W>>>
  fullOuterJoin(final org.apache.spark.api.java.JavaPairRDD<K, W> other) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<Optional<V>, Optional<W>>>
  fullOuterJoin(final org.apache.spark.api.java.JavaPairRDD<K, W> other,
                final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public Map<K, V> collectAsMap() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> JavaPairRDD<K, U> mapValues(final Function<V, U> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> JavaPairRDD<K, U> flatMapValues(final Function<V, Iterable<U>> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<Iterable<V>, Iterable<W>>>
  cogroup(final org.apache.spark.api.java.JavaPairRDD<K, W> other,
          final Partitioner partitioner) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W1, W2> JavaPairRDD<K, Tuple3<Iterable<V>, Iterable<W1>, Iterable<W2>>>
  cogroup(final org.apache.spark.api.java.JavaPairRDD<K, W1> other1,
          final org.apache.spark.api.java.JavaPairRDD<K, W2> other2,
          final Partitioner partitioner) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W1, W2, W3> JavaPairRDD<K, Tuple4<Iterable<V>, Iterable<W1>, Iterable<W2>, Iterable<W3>>>
  cogroup(final org.apache.spark.api.java.JavaPairRDD<K, W1> other1,
          final org.apache.spark.api.java.JavaPairRDD<K, W2> other2,
          final org.apache.spark.api.java.JavaPairRDD<K, W3> other3,
          final Partitioner partitioner) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<Iterable<V>, Iterable<W>>>
  cogroup(final org.apache.spark.api.java.JavaPairRDD<K, W> other) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W1, W2> JavaPairRDD<K, Tuple3<Iterable<V>, Iterable<W1>, Iterable<W2>>>
  cogroup(final org.apache.spark.api.java.JavaPairRDD<K, W1> other1,
          final org.apache.spark.api.java.JavaPairRDD<K, W2> other2) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W1, W2, W3> JavaPairRDD<K, Tuple4<Iterable<V>, Iterable<W1>, Iterable<W2>, Iterable<W3>>>
  cogroup(final org.apache.spark.api.java.JavaPairRDD<K, W1> other1,
          final org.apache.spark.api.java.JavaPairRDD<K, W2> other2,
          final org.apache.spark.api.java.JavaPairRDD<K, W3> other3) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<Iterable<V>, Iterable<W>>>
  cogroup(final org.apache.spark.api.java.JavaPairRDD<K, W> other,
          final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W1, W2> JavaPairRDD<K, Tuple3<Iterable<V>, Iterable<W1>, Iterable<W2>>>
  cogroup(final org.apache.spark.api.java.JavaPairRDD<K, W1> other1,
          final org.apache.spark.api.java.JavaPairRDD<K, W2> other2,
          final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W1, W2, W3> JavaPairRDD<K, Tuple4<Iterable<V>, Iterable<W1>, Iterable<W2>, Iterable<W3>>>
  cogroup(final org.apache.spark.api.java.JavaPairRDD<K, W1> other1,
          final org.apache.spark.api.java.JavaPairRDD<K, W2> other2,
          final org.apache.spark.api.java.JavaPairRDD<K, W3> other3,
          final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W> JavaPairRDD<K, Tuple2<Iterable<V>, Iterable<W>>>
  groupWith(final org.apache.spark.api.java.JavaPairRDD<K, W> other) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W1, W2> JavaPairRDD<K, Tuple3<Iterable<V>, Iterable<W1>, Iterable<W2>>>
  groupWith(final org.apache.spark.api.java.JavaPairRDD<K, W1> other1,
            final org.apache.spark.api.java.JavaPairRDD<K, W2> other2) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <W1, W2, W3> JavaPairRDD<K, Tuple4<Iterable<V>, Iterable<W1>, Iterable<W2>, Iterable<W3>>>
  groupWith(final org.apache.spark.api.java.JavaPairRDD<K, W1> other1,
            final org.apache.spark.api.java.JavaPairRDD<K, W2> other2,
            final org.apache.spark.api.java.JavaPairRDD<K, W3> other3) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<V> lookup(final K key) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  // Commented out due to an IDE issue
  /*@Override
  public <F extends OutputFormat<?, ?>> void saveAsHadoopFile(final String path,
                                                              final Class<?> keyClass,
                                                              final Class<?> valueClass,
                                                              final Class<F> outputFormatClass,
                                                              final JobConf conf) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <F extends OutputFormat<?, ?>> void saveAsHadoopFile(final String path,
                                                              final Class<?> keyClass,
                                                              final Class<?> valueClass,
                                                              final Class<F> outputFormatClass) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <F extends OutputFormat<?, ?>> void saveAsHadoopFile(final String path,
                                                              final Class<?> keyClass,
                                                              final Class<?> valueClass,
                                                              final Class<F> outputFormatClass,
                                                              final Class<? extends CompressionCodec> codec) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <F extends org.apache.hadoop.mapreduce.OutputFormat<?, ?>> void
  saveAsNewAPIHadoopFile(final String path,
                         final Class<?> keyClass,
                         final Class<?> valueClass,
                         final Class<F> outputFormatClass,
                         final Configuration conf) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public void saveAsNewAPIHadoopDataset(final Configuration conf) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <F extends org.apache.hadoop.mapreduce.OutputFormat<?, ?>> void
  saveAsNewAPIHadoopFile(final String path,
                         final Class<?> keyClass,
                         final Class<?> valueClass,
                         final Class<F> outputFormatClass) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }*/

  @Override
  public void saveAsHadoopDataset(final JobConf conf) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> repartitionAndSortWithinPartitions(final Partitioner partitioner) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> repartitionAndSortWithinPartitions(final Partitioner partitioner,
                                                              final Comparator<K> comp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> sortByKey() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> sortByKey(final boolean ascending) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> sortByKey(final boolean ascending,
                                     final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> sortByKey(final Comparator<K> comp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> sortByKey(final Comparator<K> comp,
                                     final boolean ascending) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> sortByKey(final Comparator<K> comp,
                                     final boolean ascending,
                                     final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<K> keys() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<V> values() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, Long> countApproxDistinctByKey(final double relativeSD,
                                                       final Partitioner partitioner) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, Long> countApproxDistinctByKey(final double relativeSD,
                                                       final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, Long> countApproxDistinctByKey(final double relativeSD) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<K, V> setName(final String name) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }
}
