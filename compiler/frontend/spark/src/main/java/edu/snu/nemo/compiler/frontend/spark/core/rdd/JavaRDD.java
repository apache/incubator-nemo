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

import org.apache.spark.*;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.apache.spark.storage.StorageLevel;
import scala.reflect.ClassTag$;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Java RDD.
 * @param <T> type of the final element.
 */
public final class JavaRDD<T> extends org.apache.spark.api.java.JavaRDD<T> {
  private final RDD<T> rdd;

  /**
   * Static method to create a JavaRDD object from {@link RDD}.
   *
   * @param <T> type of the resulting object.
   * @return the new JavaRDD object.
   */
  public static <T> JavaRDD<T> fromRDD(final RDD<T> rddFrom) {
    return new JavaRDD<>(rddFrom);
  }

  @Override
  public JavaRDD<T> wrapRDD(final org.apache.spark.rdd.RDD<T> rddFrom) {
    if (!(rddFrom instanceof RDD)) {
      throw new UnsupportedOperationException("Cannot wrap Spark RDD as Nemo RDD!");
    }
    return fromRDD((RDD<T>) rddFrom);
  }

  /**
   * Constructor.
   *
   * @param rdd the rdd to wrap.
   */
  private JavaRDD(final RDD<T> rdd) {
    super(rdd, ClassTag$.MODULE$.apply((Class<T>) Object.class));
    this.rdd = rdd;
  }

  /////////////// TRANSFORMATIONS ///////////////

  /**
   * Map transform.
   *
   * @param func function to apply.
   * @param <O>  output type.
   * @return the JavaRDD with the extended DAG.
   */
  @Override
  public <O> JavaRDD<O> map(final Function<T, O> func) {
    return fromRDD(rdd.map(func));
  }

  /**
   * Flat map transform.
   *
   * @param func function to apply.
   * @param <O>  output type.
   * @return the JavaRDD with the extended DAG.
   */
  @Override
  public <O> JavaRDD<O> flatMap(final FlatMapFunction<T, O> func) {
    return fromRDD(rdd.flatMap(func));
  }

  /////////////// TRANSFORMATION TO PAIR RDD ///////////////

  /**
   * @see org.apache.spark.api.java.JavaRDD#mapToPair(PairFunction).
   */
  @Override
  public <K2, V2> JavaPairRDD<K2, V2> mapToPair(final PairFunction<T, K2, V2> f) {
    return rdd.mapToPair(f);
  }

  /////////////// ACTIONS ///////////////

  private static final AtomicInteger RESULT_ID = new AtomicInteger(0);

  /**
   * This method is to be removed after a result handler is implemented.
   *
   * @return a unique integer.
   */
  public static Integer getResultId() {
    return RESULT_ID.getAndIncrement();
  }

  /**
   * Reduce action.
   *
   * @param func function (binary operator) to apply.
   * @return the result of the reduce action.
   */
  @Override
  public T reduce(final Function2<T, T, T> func) {
    return rdd.reduce(func);
  }

  @Override
  public List<T> collect() {
    return rdd.collectToList();
  }

  @Override
  public void saveAsTextFile(final String path) {
    rdd.saveAsTextFile(path);
  }

  /////////////// UNSUPPORTED TRANSFORMATIONS ///////////////
  //TODO#776: support unimplemented RDD transformation/actions.

  @Override
  public JavaRDD<T> cache() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> coalesce(final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> coalesce(final int numPartitions, final boolean shuffle) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> distinct() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> distinct(final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> filter(final Function<T, Boolean> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<List<T>> glom() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> JavaRDD<U> mapPartitions(final FlatMapFunction<Iterator<T>, U> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> JavaRDD<U> mapPartitions(final FlatMapFunction<Iterator<T>, U> f, final boolean preservesPartitioning) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <R> JavaRDD<R> mapPartitionsWithIndex(final Function2<Integer, Iterator<T>, Iterator<R>> f,
                                               final boolean preservesPartitioning) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> persist(final StorageLevel newLevel) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T>[] randomSplit(final double[] weights) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T>[] randomSplit(final double[] weights, final long seed) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> repartition(final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> sample(final boolean withReplacement, final double fraction) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> sample(final boolean withReplacement, final double fraction, final long seed) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> setName(final String name) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <S> JavaRDD<T> sortBy(final Function<T, S> f, final boolean ascending, final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> unpersist() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> unpersist(final boolean blocking) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  /////////////// UNSUPPORTED TRANSFORMATION TO PAIR RDD ///////////////
  //TODO#776: support unimplemented RDD transformation/actions.

  @Override
  public <K2, V2> JavaPairRDD<K2, V2> flatMapToPair(final PairFlatMapFunction<T, K2, V2> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> JavaPairRDD<U, Iterable<T>> groupBy(final Function<T, U> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> JavaPairRDD<U, Iterable<T>> groupBy(final Function<T, U> f, final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> JavaPairRDD<U, T> keyBy(final Function<T, U> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <K2, V2> JavaPairRDD<K2, V2> mapPartitionsToPair(final PairFlatMapFunction<Iterator<T>, K2, V2> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <K2, V2> JavaPairRDD<K2, V2> mapPartitionsToPair(final PairFlatMapFunction<java.util.Iterator<T>, K2, V2> f,
                                                          final boolean preservesPartitioning) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<T, Long> zipWithIndex() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<T, Long> zipWithUniqueId() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  /////////////// UNSUPPORTED ACTIONS ///////////////
  //TODO#776: support unimplemented RDD transformation/actions.

  @Override
  public <U> U aggregate(final U zeroValue, final Function2<U, T, U> seqOp, final Function2<U, U, U> combOp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public void checkpoint() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }


  @Override
  public JavaFutureAction<List<T>> collectAsync() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T>[] collectPartitions(final int[] partitionIds) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public long count() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public PartialResult<BoundedDouble> countApprox(final long timeout) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public PartialResult<BoundedDouble> countApprox(final long timeout, final double confidence) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public long countApproxDistinct(final double relativeSD) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaFutureAction<Long> countAsync() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public Map<T, Long> countByValue() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public PartialResult<Map<T, BoundedDouble>> countByValueApprox(final long timeout) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public PartialResult<Map<T, BoundedDouble>> countByValueApprox(final long timeout, final double confidence) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public T first() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public T fold(final T zeroValue, final Function2<T, T, T> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public void foreach(final VoidFunction<T> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaFutureAction<Void> foreachAsync(final VoidFunction<T> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public void foreachPartition(final VoidFunction<Iterator<T>> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaFutureAction<Void> foreachPartitionAsync(final VoidFunction<Iterator<T>> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public Optional<String> getCheckpointFile() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public int getNumPartitions() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public StorageLevel getStorageLevel() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public int id() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public boolean isCheckpointed() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public Iterator<T> iterator(final Partition split, final TaskContext taskContext) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public T max(final Comparator<T> comp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public T min(final Comparator<T> comp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public String name() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public org.apache.spark.api.java.Optional<Partitioner> partitioner() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<Partition> partitions() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public void saveAsObjectFile(final String path) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public void saveAsTextFile(final String path,
                             final Class<? extends org.apache.hadoop.io.compress.CompressionCodec> codec) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> take(final int num) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaFutureAction<List<T>> takeAsync(final int num) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> takeOrdered(final int num) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> takeOrdered(final int num, final Comparator<T> comp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> takeSample(final boolean withReplacement, final int num) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> takeSample(final boolean withReplacement, final int num, final long seed) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public String toDebugString() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public Iterator<T> toLocalIterator() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> top(final int num) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> top(final int num, final Comparator<T> comp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> U treeAggregate(final U zeroValue, final Function2<U, T, U> seqOp, final Function2<U, U, U> combOp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> U treeAggregate(final U zeroValue, final Function2<U, T, U> seqOp,
                             final Function2<U, U, U> combOp, final int depth) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public T treeReduce(final Function2<T, T, T> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public T treeReduce(final Function2<T, T, T> f, final int depth) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }
}
