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
package edu.snu.nemo.compiler.frontend.spark.core.java;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.KeyExtractorProperty;
import edu.snu.nemo.common.ir.vertex.*;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.compiler.frontend.spark.SparkKeyExtractor;
import edu.snu.nemo.compiler.frontend.spark.coder.SparkCoder;
import edu.snu.nemo.compiler.frontend.spark.core.RDD;
import edu.snu.nemo.compiler.frontend.spark.source.SparkBoundedSourceVertex;
import edu.snu.nemo.compiler.frontend.spark.sql.Dataset;
import edu.snu.nemo.compiler.frontend.spark.sql.SparkSession;
import edu.snu.nemo.compiler.frontend.spark.transform.*;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.storage.StorageLevel;
import scala.reflect.ClassTag$;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static edu.snu.nemo.compiler.frontend.spark.core.java.SparkFrontendUtils.getEdgeCommunicationPattern;

/**
 * Java RDD.
 * @param <T> type of the final element.
 */
public final class JavaRDD<T> extends org.apache.spark.api.java.JavaRDD<T> {
  private final SparkContext sparkContext;
  private final Stack<LoopVertex> loopVertexStack;
  private final DAG<IRVertex, IREdge> dag;
  private final IRVertex lastVertex;
  private final Serializer serializer;

  /**
   * Static method to create a JavaRDD object from an iterable object.
   * @param sparkContext spark context containing configurations.
   * @param initialData initial data.
   * @param parallelism parallelism information.
   * @param <T> type of the resulting object.
   * @return the new JavaRDD object.
   */
  public static <T> JavaRDD<T> of(final SparkContext sparkContext,
                                  final Iterable<T> initialData, final Integer parallelism) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    final IRVertex initializedSourceVertex = new InitializedSourceVertex<>(initialData);
    initializedSourceVertex.setProperty(ParallelismProperty.of(parallelism));
    builder.addVertex(initializedSourceVertex);

    return new JavaRDD<>(sparkContext, builder.buildWithoutSourceSinkCheck(), initializedSourceVertex);
  }

  /**
   * Static method to create a JavaRDD object from a Dataset.
   * @param sparkSession spark session containing configurations.
   * @param dataset dataset to read initial data from.
   * @param <T> type of the resulting object.
   * @return the new JavaRDD object.
   */
  public static <T> JavaRDD<T> of(final SparkSession sparkSession,
                                  final Dataset<T> dataset) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    final IRVertex sparkBoundedSourceVertex = new SparkBoundedSourceVertex<>(sparkSession, dataset);
    sparkBoundedSourceVertex.setProperty(ParallelismProperty.of(dataset.rdd().getNumPartitions()));
    builder.addVertex(sparkBoundedSourceVertex);

    return new JavaRDD<>(sparkSession.sparkContext(), builder.buildWithoutSourceSinkCheck(), sparkBoundedSourceVertex);
  }

  /**
   * Constructor.
   * @param sparkContext spark context containing configurations.
   * @param dag the current DAG.
   * @param lastVertex last vertex added to the builder.
   */
  JavaRDD(final SparkContext sparkContext, final DAG<IRVertex, IREdge> dag, final IRVertex lastVertex) {
    // TODO #366: resolve while implementing scala RDD.
    super(RDD.of(sparkContext), ClassTag$.MODULE$.apply((Class<T>) Object.class));

    this.loopVertexStack = new Stack<>();
    this.sparkContext = sparkContext;
    this.dag = dag;
    this.lastVertex = lastVertex;
    this.serializer = SparkFrontendUtils.deriveSerializerFrom(sparkContext);
  }

  /**
   * @return the spark context.
   */
  public SparkContext getSparkContext() {
    return sparkContext;
  }

  /////////////// TRANSFORMATIONS ///////////////

  /**
   * Map transform.
   * @param func function to apply.
   * @param <O> output type.
   * @return the JavaRDD with the DAG.
   */
  @Override
  public <O> JavaRDD<O> map(final Function<T, O> func) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex mapVertex = new OperatorVertex(new MapTransform<>(func));
    builder.addVertex(mapVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, mapVertex),
        lastVertex, mapVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new JavaRDD<>(this.sparkContext, builder.buildWithoutSourceSinkCheck(), mapVertex);
  }

  @Override
  public <U> JavaRDD<U> flatMap(final FlatMapFunction<T, U> f) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex flatMapVertex = new OperatorVertex(new FlatMapTransform<>(f));
    builder.addVertex(flatMapVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, flatMapVertex),
        lastVertex, flatMapVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new JavaRDD<>(this.sparkContext, builder.buildWithoutSourceSinkCheck(), flatMapVertex);
  }

  /////////////// TRANSFORMATION TO PAIR RDD ///////////////

  @Override
  public <K2, V2> JavaPairRDD<K2, V2> mapToPair(final PairFunction<T, K2, V2> f)  {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex mapToPairVertex = new OperatorVertex(new MapToPairTransform<>(f));
    builder.addVertex(mapToPairVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, mapToPairVertex),
        lastVertex, mapToPairVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new JavaPairRDD<>(this.sparkContext, builder.buildWithoutSourceSinkCheck(), mapToPairVertex);
  }

  /////////////// ACTIONS ///////////////

  private static final AtomicInteger RESULT_ID = new AtomicInteger(0);

  /**
   * This method is to be removed after a result handler is implemented.
   * @return a unique integer.
   */
  public static Integer getResultId() {
    return RESULT_ID.getAndIncrement();
  }

  /**
   * Reduce action.
   * @param func function (binary operator) to apply.
   * @return the result of the reduce action.
   */
  @Override
  public T reduce(final Function2<T, T, T> func) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex reduceVertex = new OperatorVertex(new ReduceTransform<>(func));
    builder.addVertex(reduceVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, reduceVertex),
        lastVertex, reduceVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return ReduceTransform.reduceIterator(collect().iterator(), func);
  }

  @Override
  public List<T> collect() {
    return SparkFrontendUtils.collect(dag, loopVertexStack, lastVertex, serializer);
  }

  @Override
  public void saveAsTextFile(final String path) {

    // Check if given path is HDFS path.
    final boolean isHDFSPath = path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("file://");
    final Transform textFileTransform = isHDFSPath
        ? new HDFSTextFileTransform(path) : new LocalTextFileTransform(path);

    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex flatMapVertex = new OperatorVertex(textFileTransform);
    builder.addVertex(flatMapVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, flatMapVertex),
        lastVertex, flatMapVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    JobLauncher.launchDAG(builder.build());
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
  public <U> JavaPairRDD<U, Iterable<T>> groupBy(final Function<T, U> f, final int numPartitions)  {
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
                                                          final boolean preservesPartitioning)  {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaPairRDD<T, Long> zipWithIndex()  {
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
  public JavaFutureAction<List<T>> collectAsync()  {
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
  public PartialResult<BoundedDouble> countApprox(final long timeout)  {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public PartialResult<BoundedDouble> countApprox(final long timeout, final double confidence) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public long countApproxDistinct(final double relativeSD)  {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaFutureAction<Long> countAsync()  {
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
  public int getNumPartitions()  {
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
  public boolean isEmpty()  {
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
